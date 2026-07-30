use keel_agentd::wire::VolumeStatus;
use keel_agentd::JailStatus;
use keel_controlplane::wire::{NodeStatus, ServiceReplica, ServiceSummary};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub trait ControlPlaneClient: Send + Sync {
    fn fetch_nodes(&self) -> Result<Vec<NodeStatus>, String>;
    fn fetch_jails(&self, node_id: &str) -> Result<Vec<JailStatus>, String>;
    fn fetch_volumes(&self, node_id: &str) -> Result<Vec<VolumeStatus>, String>;
    fn fetch_services(&self) -> Result<Vec<ServiceSummary>, String>;
    /// `GET /services/{name}` returns just the replica placement list (no
    /// name/vip/port wrapper - those already come from `fetch_services`'s
    /// `ServiceSummary`), confirmed against the real
    /// `keel-controlplane::http::handle_get_service` handler, which yaml
    /// -encodes its `DiscoverService` reply's `replicas: Vec<ServiceReplica>`
    /// directly.
    fn fetch_service(&self, name: &str) -> Result<Vec<ServiceReplica>, String>;
}

/// The real client: an mTLS TCP connection per request, exactly the same
/// request/response shape as `keelctl`'s `send_request_tcp` and
/// `keel-agentd::registration`'s `send_request`.
pub struct TlsControlPlaneClient {
    addr: String,
    client_config: Arc<rustls::ClientConfig>,
}

impl TlsControlPlaneClient {
    pub fn new(addr: String, client_config: Arc<rustls::ClientConfig>) -> Self {
        Self {
            addr,
            client_config,
        }
    }

    fn request(&self, method: &str, path: &str) -> Result<(u16, String), String> {
        let server_name =
            crate::tls::server_name_from_addr(&self.addr).map_err(|e| e.to_string())?;
        let tcp_stream = TcpStream::connect(&self.addr)
            .map_err(|e| format!("failed to connect to {}: {e}", self.addr))?;
        let conn = rustls::ClientConnection::new(Arc::clone(&self.client_config), server_name)
            .map_err(|e| e.to_string())?;
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);

        let request =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .map_err(|e| format!("failed to send request: {e}"))?;
        stream.sock.shutdown(std::net::Shutdown::Write).ok();

        let mut response = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => response.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("failed to read response: {e}")),
            }
        }
        parse_response(&response)
    }

    fn get_yaml<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        let (status, body) = self.request("GET", path)?;
        if !(200..300).contains(&status) {
            return Err(format!("GET {path} returned status {status}: {body}"));
        }
        serde_yaml::from_str(&body).map_err(|e| format!("malformed response from GET {path}: {e}"))
    }
}

fn parse_response(response: &[u8]) -> Result<(u16, String), String> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Response::new(&mut headers);
    let header_len = match parsed
        .parse(response)
        .map_err(|e| format!("malformed response: {e}"))?
    {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => return Err("incomplete response from server".to_string()),
    };
    let status = parsed.code.unwrap_or(0);
    let content_length = parsed
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .and_then(|h| std::str::from_utf8(h.value).ok())
        .and_then(|v| v.trim().parse::<usize>().ok())
        .ok_or_else(|| "response missing Content-Length header".to_string())?;
    let actual = response.len() - header_len;
    if actual != content_length {
        return Err(format!(
            "truncated response: expected {content_length} bytes, got {actual}"
        ));
    }
    Ok((
        status,
        String::from_utf8_lossy(&response[header_len..]).to_string(),
    ))
}

impl ControlPlaneClient for TlsControlPlaneClient {
    fn fetch_nodes(&self) -> Result<Vec<NodeStatus>, String> {
        self.get_yaml("/nodes")
    }

    fn fetch_jails(&self, node_id: &str) -> Result<Vec<JailStatus>, String> {
        self.get_yaml(&format!("/nodes/{node_id}/jails"))
    }

    fn fetch_volumes(&self, node_id: &str) -> Result<Vec<VolumeStatus>, String> {
        self.get_yaml(&format!("/nodes/{node_id}/volumes"))
    }

    fn fetch_services(&self) -> Result<Vec<ServiceSummary>, String> {
        self.get_yaml("/services")
    }

    fn fetch_service(&self, name: &str) -> Result<Vec<ServiceReplica>, String> {
        self.get_yaml(&format!("/services/{name}"))
    }
}

/// In-memory test double: seed data with the `set_*` methods, simulate a
/// failed fetch with the `fail_*` methods. Mirrors `FakeZfsManager`'s
/// `Arc<Mutex<...>>`-backed, freely-`Clone`-able shape.
#[derive(Default, Clone)]
pub struct FakeControlPlaneClient {
    nodes: Arc<Mutex<Vec<NodeStatus>>>,
    jails: Arc<Mutex<HashMap<String, Vec<JailStatus>>>>,
    volumes: Arc<Mutex<HashMap<String, Vec<VolumeStatus>>>>,
    services: Arc<Mutex<Vec<ServiceSummary>>>,
    service_details: Arc<Mutex<HashMap<String, Vec<ServiceReplica>>>>,
    nodes_failing: Arc<Mutex<bool>>,
    failing_jail_nodes: Arc<Mutex<std::collections::HashSet<String>>>,
    failing_volume_nodes: Arc<Mutex<std::collections::HashSet<String>>>,
    services_failing: Arc<Mutex<bool>>,
    failing_services: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl FakeControlPlaneClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_nodes(&self, nodes: Vec<NodeStatus>) {
        *self.nodes.lock().unwrap() = nodes;
    }

    pub fn fail_nodes(&self) {
        *self.nodes_failing.lock().unwrap() = true;
    }

    pub fn set_jails(&self, node_id: &str, jails: Vec<JailStatus>) {
        self.jails
            .lock()
            .unwrap()
            .insert(node_id.to_string(), jails);
    }

    pub fn fail_jails(&self, node_id: &str) {
        self.failing_jail_nodes
            .lock()
            .unwrap()
            .insert(node_id.to_string());
    }

    pub fn set_volumes(&self, node_id: &str, volumes: Vec<VolumeStatus>) {
        self.volumes
            .lock()
            .unwrap()
            .insert(node_id.to_string(), volumes);
    }

    pub fn fail_volumes(&self, node_id: &str) {
        self.failing_volume_nodes
            .lock()
            .unwrap()
            .insert(node_id.to_string());
    }

    pub fn set_services(&self, services: Vec<ServiceSummary>) {
        *self.services.lock().unwrap() = services;
    }

    pub fn fail_services(&self) {
        *self.services_failing.lock().unwrap() = true;
    }

    pub fn set_service(&self, name: &str, replicas: Vec<ServiceReplica>) {
        self.service_details
            .lock()
            .unwrap()
            .insert(name.to_string(), replicas);
    }

    pub fn fail_service(&self, name: &str) {
        self.failing_services
            .lock()
            .unwrap()
            .insert(name.to_string());
    }
}

impl ControlPlaneClient for FakeControlPlaneClient {
    fn fetch_nodes(&self) -> Result<Vec<NodeStatus>, String> {
        if *self.nodes_failing.lock().unwrap() {
            return Err("simulated control-plane unreachable".to_string());
        }
        Ok(self.nodes.lock().unwrap().clone())
    }

    fn fetch_jails(&self, node_id: &str) -> Result<Vec<JailStatus>, String> {
        if self.failing_jail_nodes.lock().unwrap().contains(node_id) {
            return Err(format!("simulated failure fetching jails for '{node_id}'"));
        }
        Ok(self
            .jails
            .lock()
            .unwrap()
            .get(node_id)
            .cloned()
            .unwrap_or_default())
    }

    fn fetch_volumes(&self, node_id: &str) -> Result<Vec<VolumeStatus>, String> {
        if self.failing_volume_nodes.lock().unwrap().contains(node_id) {
            return Err(format!(
                "simulated failure fetching volumes for '{node_id}'"
            ));
        }
        Ok(self
            .volumes
            .lock()
            .unwrap()
            .get(node_id)
            .cloned()
            .unwrap_or_default())
    }

    fn fetch_services(&self) -> Result<Vec<ServiceSummary>, String> {
        if *self.services_failing.lock().unwrap() {
            return Err("simulated failure fetching services".to_string());
        }
        Ok(self.services.lock().unwrap().clone())
    }

    fn fetch_service(&self, name: &str) -> Result<Vec<ServiceReplica>, String> {
        if self.failing_services.lock().unwrap().contains(name) {
            return Err(format!("simulated failure fetching service '{name}'"));
        }
        self.service_details
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| format!("no such service '{name}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_controlplane::wire::NodeState;

    fn sample_node(id: &str) -> keel_controlplane::wire::NodeStatus {
        keel_controlplane::wire::NodeStatus {
            id: id.to_string(),
            addr: "192.168.64.4:7621".to_string(),
            pod_cidr: "10.0.4.0/24".to_string(),
            status: NodeState::Alive,
            last_seen_secs: 1,
            capacity_cpu: 4.0,
            capacity_memory: 8 * 1024 * 1024 * 1024,
            committed_cpu: 1.0,
            committed_memory: 1024 * 1024 * 1024,
            ingresses: vec![],
            cordoned: false,
        }
    }

    #[test]
    fn fake_returns_seeded_nodes() {
        let fake = FakeControlPlaneClient::new();
        fake.set_nodes(vec![sample_node("node-1")]);
        let nodes = fake.fetch_nodes().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "node-1");
    }

    #[test]
    fn fake_fetch_nodes_fails_once_marked_failing() {
        let fake = FakeControlPlaneClient::new();
        fake.set_nodes(vec![sample_node("node-1")]);
        fake.fail_nodes();
        assert!(fake.fetch_nodes().is_err());
    }

    #[test]
    fn fake_fetch_jails_fails_only_for_the_marked_node() {
        let fake = FakeControlPlaneClient::new();
        fake.set_jails("node-1", vec![]);
        fake.set_jails("node-2", vec![]);
        fake.fail_jails("node-1");
        assert!(fake.fetch_jails("node-1").is_err());
        assert!(fake.fetch_jails("node-2").is_ok());
    }

    #[test]
    fn fake_returns_seeded_service_detail() {
        let fake = FakeControlPlaneClient::new();
        let replicas = vec![keel_controlplane::wire::ServiceReplica {
            name: "web-0".to_string(),
            node: "node-1".to_string(),
            address: "10.0.4.5".to_string(),
        }];
        fake.set_service("web", replicas.clone());
        assert_eq!(fake.fetch_service("web").unwrap(), replicas);
    }

    #[test]
    fn fake_fetch_service_on_an_unknown_name_fails() {
        let fake = FakeControlPlaneClient::new();
        assert!(fake.fetch_service("missing").is_err());
    }
}
