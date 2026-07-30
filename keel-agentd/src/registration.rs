use crate::tls;
use keel_controlplane::wire::{NodeState, NodeStatus};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub(crate) fn diff_routes(
    self_id: &str,
    peers: &[NodeStatus],
    installed: &HashMap<String, String>,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut to_add = Vec::new();
    for peer in peers {
        if peer.id == self_id || peer.status != NodeState::Alive {
            continue;
        }
        if installed.get(&peer.id) != Some(&peer.pod_cidr) {
            to_add.push((peer.pod_cidr.clone(), peer.addr.clone()));
        }
    }

    let alive_ids: std::collections::HashSet<&str> = peers
        .iter()
        .filter(|p| p.status == NodeState::Alive && p.id != self_id)
        .map(|p| p.id.as_str())
        .collect();
    let mut to_remove = Vec::new();
    for (id, pod_cidr) in installed {
        if !alive_ids.contains(id.as_str()) {
            to_remove.push(pod_cidr.clone());
        }
    }

    (to_add, to_remove)
}

// each parameter is independently needed by the registration loop; bundling into a struct would be over-engineering for this single call site
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    node_id: String,
    advertise_addr: String,
    replicate_addr: String,
    control_plane_addr: String,
    heartbeat_interval: Duration,
    state_dir: PathBuf,
    capacity_cpu: f64,
    capacity_memory: u64,
    reloading_tls: Arc<tls::ReloadingTls>,
    commands: Sender<crate::worker::Command>,
    pod_cidr_slot: crate::PodCidrSlot,
    service_vips: crate::ServiceVipSlot,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut registered = false;
        let mut installed_routes: HashMap<String, String> = HashMap::new();
        let mut proxied_services: HashMap<String, crate::proxy::ProxiedService> = HashMap::new();
        loop {
            let client_config = reloading_tls.client_config();
            if !registered {
                match register_once(
                    &control_plane_addr,
                    &node_id,
                    &advertise_addr,
                    &replicate_addr,
                    capacity_cpu,
                    capacity_memory,
                    &client_config,
                ) {
                    Ok(pod_cidr) => {
                        pod_cidr_slot.set(pod_cidr);
                        registered = true;
                    }
                    Err(e) => eprintln!("keel-agentd: registration failed: {e}"),
                }
            } else {
                match heartbeat_once(
                    &control_plane_addr,
                    &node_id,
                    &commands,
                    &client_config,
                    &state_dir,
                ) {
                    Ok(entries) => {
                        service_vips.set_all(&entries);
                        crate::proxy::reconcile_services(
                            &entries,
                            &mut proxied_services,
                            &commands,
                        );
                    }
                    Err(e) => {
                        eprintln!("keel-agentd: heartbeat failed: {e}");
                        registered = false;
                    }
                }
            }

            match fetch_nodes(&control_plane_addr, &client_config) {
                Ok(peers) => reconcile_routes(&node_id, &peers, &mut installed_routes, &commands),
                Err(e) => eprintln!(
                    "keel-agentd: failed to fetch peer list for route reconciliation: {e}"
                ),
            }

            thread::sleep(heartbeat_interval);
        }
    })
}

fn register_once(
    control_plane_addr: &str,
    node_id: &str,
    advertise_addr: &str,
    replicate_addr: &str,
    capacity_cpu: f64,
    capacity_memory: u64,
    client_config: &Arc<rustls::ClientConfig>,
) -> Result<ipnet::Ipv4Net, String> {
    let body = format!(
        "id: {node_id}\naddr: {advertise_addr}\nreplicate_addr: {replicate_addr}\ncapacity_cpu: {capacity_cpu}\ncapacity_memory: {capacity_memory}\n"
    );
    let response_body = send_request(
        control_plane_addr,
        "POST",
        "/nodes/register",
        &body,
        client_config,
    )?;
    let response: keel_controlplane::wire::RegisterResponse =
        serde_yaml::from_slice(&response_body)
            .map_err(|e| format!("malformed registration response: {e}"))?;
    response.pod_cidr.parse().map_err(|e| {
        format!(
            "control plane returned invalid pod_cidr '{}': {e}",
            response.pod_cidr
        )
    })
}

fn heartbeat_once(
    control_plane_addr: &str,
    node_id: &str,
    commands: &Sender<crate::worker::Command>,
    client_config: &Arc<rustls::ClientConfig>,
    state_dir: &std::path::Path,
) -> Result<Vec<keel_controlplane::wire::ServiceProxyEntry>, String> {
    let (resources_tx, resources_rx) = std::sync::mpsc::channel();
    commands
        .send(crate::worker::Command::CommittedResources(resources_tx))
        .map_err(|_| "worker is not running".to_string())?;
    let (committed_cpu, committed_memory) = resources_rx
        .recv()
        .map_err(|_| "worker did not respond".to_string())?;

    let (jails_tx, jails_rx) = std::sync::mpsc::channel();
    commands
        .send(crate::worker::Command::Get(None, jails_tx))
        .map_err(|_| "worker is not running".to_string())?;
    let statuses = jails_rx
        .recv()
        .map_err(|_| "worker did not respond".to_string())?;
    let jails: Vec<keel_controlplane::wire::JailHealth> = statuses
        .into_iter()
        .map(|s| keel_controlplane::wire::JailHealth {
            name: s.record.spec.metadata.name,
            running: s.running,
        })
        .collect();

    let ingress_records = crate::ingress_store::load_all(state_dir)
        .map_err(|e| format!("failed to load ingress records: {e}"))?;
    let ingresses: Vec<keel_controlplane::wire::IngressHealth> = ingress_records
        .into_iter()
        .map(|r| keel_controlplane::wire::IngressHealth {
            name: r.spec.metadata.name,
            host: r.spec.spec.host,
            backend_service: r.spec.spec.backend.service,
            backend_port: r.spec.spec.backend.port,
            cert_expires_at_unix: r.cert_expires_at_unix,
        })
        .collect();

    let heartbeat = keel_controlplane::wire::Heartbeat {
        committed_cpu,
        committed_memory,
        jails,
        ingresses,
    };
    let body = serde_yaml::to_string(&heartbeat)
        .map_err(|e| format!("failed to serialize heartbeat: {e}"))?;
    let response_body = send_request(
        control_plane_addr,
        "POST",
        &format!("/nodes/{node_id}/heartbeat"),
        &body,
        client_config,
    )?;
    serde_yaml::from_slice(&response_body).map_err(|e| format!("malformed heartbeat response: {e}"))
}

fn fetch_nodes(
    control_plane_addr: &str,
    client_config: &Arc<rustls::ClientConfig>,
) -> Result<Vec<NodeStatus>, String> {
    let body = send_request(control_plane_addr, "GET", "/nodes", "", client_config)?;
    serde_yaml::from_slice(&body).map_err(|e| format!("malformed /nodes response: {e}"))
}

fn reconcile_routes(
    self_id: &str,
    peers: &[NodeStatus],
    installed_routes: &mut HashMap<String, String>,
    commands: &Sender<crate::worker::Command>,
) {
    let (to_add, to_remove) = diff_routes(self_id, peers, installed_routes);

    for pod_cidr in to_remove {
        let (tx, rx) = std::sync::mpsc::channel();
        if commands
            .send(crate::worker::Command::RemoveRoute(pod_cidr.clone(), tx))
            .is_err()
        {
            return;
        }
        match rx.recv() {
            Ok(Ok(())) => {
                installed_routes.retain(|_, v| v != &pod_cidr);
            }
            Ok(Err(e)) => eprintln!("keel-agentd: failed to remove route for {pod_cidr}: {e}"),
            Err(_) => eprintln!("keel-agentd: reconciler worker did not respond to RemoveRoute"),
        }
    }

    for (pod_cidr, gateway_addr) in to_add {
        // `gateway_addr` comes from a peer's `NodeStatus.addr`, which is a
        // `host:port` TCP bind address (see `tls::server_name_from_addr`,
        // which has to do the same stripping before using it as a TLS
        // `ServerName`), not a bare IP. `route(8)` only accepts a bare IP as
        // a gateway, so it must be stripped here before it reaches
        // `NetManager::add_route`.
        let gateway = gateway_addr
            .rsplit_once(':')
            .map(|(host, _port)| host)
            .unwrap_or(&gateway_addr)
            .to_string();
        let (tx, rx) = std::sync::mpsc::channel();
        if commands
            .send(crate::worker::Command::AddRoute(
                pod_cidr.clone(),
                gateway.clone(),
                tx,
            ))
            .is_err()
        {
            return;
        }
        match rx.recv() {
            Ok(Ok(())) => {
                if let Some(peer) = peers.iter().find(|p| p.pod_cidr == pod_cidr) {
                    installed_routes.insert(peer.id.clone(), pod_cidr);
                }
            }
            Ok(Err(e)) => {
                eprintln!("keel-agentd: failed to add route for {pod_cidr} via {gateway}: {e}")
            }
            Err(_) => eprintln!("keel-agentd: reconciler worker did not respond to AddRoute"),
        }
    }
}

fn send_request(
    addr: &str,
    method: &str,
    path: &str,
    body: &str,
    client_config: &Arc<rustls::ClientConfig>,
) -> Result<Vec<u8>, String> {
    let server_name = tls::server_name_from_addr(addr)?;
    let tcp_stream =
        TcpStream::connect(addr).map_err(|e| format!("failed to connect to {addr}: {e}"))?;
    let conn = rustls::ClientConnection::new(Arc::clone(client_config), server_name)
        .map_err(|e| e.to_string())?;
    let mut stream = rustls::StreamOwned::new(conn, tcp_stream);

    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("failed to send request: {e}"))?;
    stream.sock.shutdown(std::net::Shutdown::Write).ok();

    // Read until the peer closes the connection. rustls surfaces a plain TCP
    // close that lacks a TLS `close_notify` alert as `ErrorKind::UnexpectedEof`
    // rather than `Ok(0)`, matching keel-controlplane's own `forward()`; we
    // rely on that being safe below by explicitly checking the received body
    // length against the response's own Content-Length header, so a
    // connection that drops mid-body (an on-path RST, or a crashing control
    // plane) is caught as a truncated response rather than silently accepted.
    let mut response = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("failed to read response: {e}")),
        }
    }

    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Response::new(&mut headers);
    let header_len = match parsed
        .parse(&response)
        .map_err(|e| format!("malformed response: {e}"))?
    {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => {
            return Err("incomplete response from control plane".to_string())
        }
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
    if (200..300).contains(&status) {
        Ok(response[header_len..].to_vec())
    } else {
        Err(format!("control plane returned status {status}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registration;
    use keel_controlplane::placements::Placements;
    use keel_controlplane::registry::Registry;
    use keel_controlplane::worker;
    use keel_net::NetManager;
    use std::net::TcpListener;
    use std::sync::mpsc;

    fn node_status(id: &str, addr: &str, pod_cidr: &str, status: NodeState) -> NodeStatus {
        NodeStatus {
            id: id.to_string(),
            addr: addr.to_string(),
            pod_cidr: pod_cidr.to_string(),
            status,
            last_seen_secs: 0,
            capacity_cpu: 4.0,
            capacity_memory: 8 * 1024 * 1024 * 1024,
            committed_cpu: 0.0,
            committed_memory: 0,
            ingresses: vec![],
        }
    }

    #[test]
    fn a_new_alive_peer_is_added_and_self_is_never_added() {
        let peers = vec![
            node_status("node-1", "10.0.0.1", "10.0.1.0/24", NodeState::Alive),
            node_status("node-2", "10.0.0.2", "10.0.2.0/24", NodeState::Alive),
        ];
        let (to_add, to_remove) = diff_routes("node-1", &peers, &HashMap::new());
        assert_eq!(
            to_add,
            vec![("10.0.2.0/24".to_string(), "10.0.0.2".to_string())]
        );
        assert!(to_remove.is_empty());
    }

    #[test]
    fn an_already_installed_peer_with_the_same_pod_cidr_is_not_re_added() {
        let peers = vec![node_status(
            "node-2",
            "10.0.0.2",
            "10.0.2.0/24",
            NodeState::Alive,
        )];
        let mut installed = HashMap::new();
        installed.insert("node-2".to_string(), "10.0.2.0/24".to_string());
        let (to_add, to_remove) = diff_routes("node-1", &peers, &installed);
        assert!(to_add.is_empty());
        assert!(to_remove.is_empty());
    }

    #[test]
    fn a_dead_peer_that_was_installed_is_removed() {
        let peers = vec![node_status(
            "node-2",
            "10.0.0.2",
            "10.0.2.0/24",
            NodeState::Dead,
        )];
        let mut installed = HashMap::new();
        installed.insert("node-2".to_string(), "10.0.2.0/24".to_string());
        let (to_add, to_remove) = diff_routes("node-1", &peers, &installed);
        assert!(to_add.is_empty());
        assert_eq!(to_remove, vec!["10.0.2.0/24".to_string()]);
    }

    #[test]
    fn a_peer_missing_entirely_from_the_list_that_was_installed_is_removed() {
        let peers: Vec<NodeStatus> = vec![];
        let mut installed = HashMap::new();
        installed.insert("node-2".to_string(), "10.0.2.0/24".to_string());
        let (to_add, to_remove) = diff_routes("node-1", &peers, &installed);
        assert!(to_add.is_empty());
        assert_eq!(to_remove, vec!["10.0.2.0/24".to_string()]);
    }

    fn fixture(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn fresh_controlplane_state_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "keel-agentd-registration-test-controlplane-{}-{id}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn start_test_control_plane() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (_worker_handle, commands) = worker::spawn(
            Registry::new("10.0.0.0/16".parse().unwrap()),
            Placements::new(),
            keel_controlplane::Services::new("10.0.250.0/24".parse().unwrap()),
            keel_controlplane::addresses::UsedAddresses::new(),
            keel_controlplane::Standbys::new(),
            keel_controlplane::PendingFences::new(),
            fresh_controlplane_state_dir(),
        );
        let reloading_tls = keel_controlplane::tls::ReloadingTls::spawn(
            fixture("fixture-node.crt"),
            fixture("fixture-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap();
        thread::spawn(move || keel_controlplane::http::run(listener, commands, reloading_tls));
        addr
    }

    // Mirrors the `spawn_test_worker` helper already used by proxy.rs's and
    // worker.rs's own test modules (each private to its module, hence a
    // separate copy here): a real worker/reconciler pair backed entirely by
    // fakes, suitable for tests that only need the heartbeat loop to be able
    // to answer `CommittedResources`/`Get` without touching real ZFS/jail/net
    // state.
    fn spawn_test_worker(name: &str) -> mpsc::Sender<crate::worker::Command> {
        let zfs = keel_zfs::FakeZfsManager::new();
        crate::worker::spawn(
            crate::Reconciler::new(
                keel_jail::FakeJailRuntime::new(),
                zfs.clone(),
                keel_net::FakeNetManager::new(),
                keel_jail::FakeMountManager::new(),
                "zroot".to_string(),
                std::env::temp_dir().join(format!("keel-agentd-registration-test-{name}")),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap(),
            zfs,
            "zroot".to_string(),
            None,
        )
        .1
    }

    fn node_client_config() -> std::sync::Arc<rustls::ClientConfig> {
        std::sync::Arc::new(
            crate::tls::load_client_config(
                &fixture("fixture-node.crt"),
                &fixture("fixture-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        )
    }

    fn node_reloading_tls() -> std::sync::Arc<crate::tls::ReloadingTls> {
        crate::tls::ReloadingTls::spawn(
            fixture("fixture-node.crt"),
            fixture("fixture-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap()
    }

    fn wrong_ca_reloading_tls() -> std::sync::Arc<crate::tls::ReloadingTls> {
        crate::tls::ReloadingTls::spawn(
            fixture("wrong-ca-node.crt"),
            fixture("wrong-ca-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap()
    }

    fn get_nodes(control_plane_addr: &str) -> String {
        let server_name = crate::tls::server_name_from_addr(control_plane_addr).unwrap();
        let tcp_stream = TcpStream::connect(control_plane_addr).unwrap();
        let conn = rustls::ClientConnection::new(node_client_config(), server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);
        stream
            .write_all(b"GET /nodes HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        stream.sock.shutdown(std::net::Shutdown::Write).ok();
        let mut response = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => response.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => panic!("failed to read response: {e}"),
            }
        }
        // Same Content-Length cross-check as `send_request`: an unclean close
        // (UnexpectedEof) is only safe to tolerate if the received body
        // actually matches what the response header claims.
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut parsed = httparse::Response::new(&mut headers);
        let header_len = match parsed.parse(&response).unwrap() {
            httparse::Status::Complete(len) => len,
            httparse::Status::Partial => panic!("incomplete response: {response:?}"),
        };
        let content_length = parsed
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("content-length"))
            .and_then(|h| std::str::from_utf8(h.value).ok())
            .and_then(|v| v.trim().parse::<usize>().ok())
            .expect("response missing Content-Length header");
        let actual = response.len() - header_len;
        assert_eq!(
            actual, content_length,
            "truncated response: expected {content_length} bytes, got {actual}"
        );
        String::from_utf8_lossy(&response).to_string()
    }

    /// A fake control plane that accepts the TLS handshake, drains the
    /// request, then sends a response header declaring a `Content-Length`
    /// larger than the body it actually writes before dropping the raw TCP
    /// connection without a clean TLS shutdown (no `close_notify`) --
    /// simulating an on-path RST or a control plane that crashes mid-write.
    fn start_fake_control_plane_with_truncated_body(
        claimed_body: &'static str,
        actual_body: &'static str,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = std::sync::Arc::new(
            crate::tls::load_server_config(
                &fixture("fixture-node.crt"),
                &fixture("fixture-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let Ok(conn) = rustls::ServerConnection::new(std::sync::Arc::clone(&server_config))
                else {
                    continue;
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                let mut buf = [0u8; 4096];
                loop {
                    match tls_stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/yaml\r\nConnection: close\r\n\r\n{actual_body}",
                    claimed_body.len()
                );
                let _ = tls_stream.write_all(header.as_bytes());
                let _ = tls_stream.flush();
                // Drop the raw TCP connection without sending a TLS
                // close_notify alert, so the client sees an unclean close
                // partway through the declared body.
                let _ = tls_stream.sock.shutdown(std::net::Shutdown::Both);
            }
        });
        addr
    }

    #[test]
    fn heartbeat_populates_the_service_vip_slot_from_the_proxy_table() {
        let control_plane_addr = start_test_control_plane();
        let commands = spawn_test_worker("heartbeat_populates_the_service_vip_slot");
        let client_config = node_client_config();
        let service_vips = crate::ServiceVipSlot::new();

        registration::spawn(
            "node-1".to_string(),
            "127.0.0.1:0".to_string(),
            "127.0.0.1:0".to_string(),
            control_plane_addr.clone(),
            Duration::from_millis(50),
            std::env::temp_dir()
                .join("keel-agentd-registration-test-heartbeat_populates_the_service_vip_slot"),
            1.0,
            1_073_741_824,
            node_reloading_tls(),
            commands.clone(),
            crate::PodCidrSlot::new(),
            service_vips.clone(),
        );

        // Apply a Service on the real control plane so the next heartbeat's
        // proxy table is non-empty, then wait a few ticks for it to land.
        let apply_body = "apiVersion: keel/v1\nkind: Service\nmetadata:\n  name: hugo-site\nspec:\n  replicas: 1\n  port: 8080\n  template:\n    image: base/test\n    command: [\"/bin/true\"]\n    network:\n      vnet: true\n      bridge: keel0\n    resources:\n      cpu: \"1\"\n      memory: \"128M\"\n    restartPolicy: Always\n";
        let _ = send_request(
            &control_plane_addr,
            "PUT",
            "/services/hugo-site",
            apply_body,
            &client_config,
        );

        let mut found = None;
        for _ in 0..40 {
            if let Some(vip) = service_vips.get("hugo-site") {
                found = Some(vip);
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        assert!(
            found.is_some(),
            "expected 'hugo-site' to appear in the ServiceVipSlot within the timeout"
        );
    }

    #[test]
    fn registers_and_then_keeps_heartbeating() {
        let control_plane_addr = start_test_control_plane();
        let zfs = keel_zfs::FakeZfsManager::new();
        let state_dir = std::env::temp_dir()
            .join("keel-agentd-registration-test-registers_and_then_keeps_heartbeating");
        let (_worker_handle, commands) = crate::worker::spawn(
            crate::Reconciler::new(
                keel_jail::FakeJailRuntime::new(),
                zfs.clone(),
                keel_net::FakeNetManager::new(),
                keel_jail::FakeMountManager::new(),
                "zroot".to_string(),
                state_dir.clone(),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap(),
            zfs,
            "zroot".to_string(),
            None,
        );
        let _handle = spawn(
            "node-1".to_string(),
            "10.0.0.1".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr.clone(),
            Duration::from_millis(50),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            node_reloading_tls(),
            commands,
            crate::PodCidrSlot::new(),
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(200));
        let body = get_nodes(&control_plane_addr);
        assert!(
            body.contains("node-1"),
            "expected node-1 to have registered, got: {body}"
        );
        assert!(
            body.contains("Alive"),
            "expected node-1 to be Alive, got: {body}"
        );
        assert!(
            body.contains("capacity_cpu: 4"),
            "expected reported capacity in body: {body}"
        );
    }

    #[test]
    fn heartbeats_report_the_reconcilers_committed_resources() {
        let control_plane_addr = start_test_control_plane();
        let zfs = keel_zfs::FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let state_dir = std::env::temp_dir().join(
            "keel-agentd-registration-test-heartbeats_report_the_reconcilers_committed_resources",
        );
        let reconciler = crate::Reconciler::new(
            keel_jail::FakeJailRuntime::new(),
            zfs.clone(),
            keel_net::FakeNetManager::new(),
            keel_jail::FakeMountManager::new(),
            "zroot".to_string(),
            state_dir.clone(),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) =
            crate::worker::spawn(reconciler, zfs, "zroot".to_string(), None);

        let (apply_tx, apply_rx) = mpsc::channel();
        commands
            .send(crate::worker::Command::Apply(
                keel_spec::JailSpec {
                    api_version: "keel/v1".to_string(),
                    kind: "Jail".to_string(),
                    metadata: keel_spec::Metadata {
                        name: "web-1".to_string(),
                    },
                    spec: keel_spec::Spec {
                        image: "base/14.2-web".to_string(),
                        command: vec!["/usr/local/bin/myapp".to_string()],
                        network: keel_spec::NetworkSpec {
                            vnet: true,
                            bridge: "keel0".to_string(),
                            address: "10.0.0.5/24".to_string(),
                        },
                        resources: keel_spec::ResourcesSpec {
                            cpu: "2".to_string(),
                            memory: "512M".to_string(),
                        },
                        restart_policy: keel_spec::RestartPolicy::Always,
                        volumes: vec![],
                        replicate_to: None,
                        generation: 0,
                    },
                },
                apply_tx,
            ))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();

        let control_plane_addr_clone = control_plane_addr.clone();
        let _handle = spawn(
            "node-1".to_string(),
            "10.0.0.1".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr_clone,
            Duration::from_millis(50),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            node_reloading_tls(),
            commands,
            crate::PodCidrSlot::new(),
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(200));
        let body = get_nodes(&control_plane_addr);
        assert!(
            body.contains("committed_cpu: 2"),
            "expected committed_cpu from the applied jail, got: {body}"
        );
        assert!(
            body.contains("committed_memory: 536870912"),
            "expected committed_memory from the applied jail, got: {body}"
        );
    }

    #[test]
    fn heartbeats_report_ingress_health_gathered_from_the_ingress_store() {
        let state_dir = std::env::temp_dir()
            .join("keel-agentd-registration-test-heartbeats_report_ingress_health");
        let _ = std::fs::remove_dir_all(&state_dir);
        let record = crate::IngressRecord {
            spec: keel_spec::IngressSpec {
                api_version: "keel/v1".to_string(),
                kind: "Ingress".to_string(),
                metadata: keel_spec::Metadata {
                    name: "blog".to_string(),
                },
                spec: keel_spec::IngressSpecBody {
                    host: "example.com".to_string(),
                    backend: keel_spec::IngressBackend {
                        service: "hugo-site".to_string(),
                        port: 8080,
                    },
                    tls: keel_spec::IngressTls {
                        email: "admin@example.com".to_string(),
                    },
                },
            },
            cert_expires_at_unix: Some(1_800_000_000),
        };
        crate::ingress_store::save(&state_dir, &record).unwrap();

        let control_plane_addr = start_test_control_plane();
        let zfs = keel_zfs::FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let reconciler = crate::Reconciler::new(
            keel_jail::FakeJailRuntime::new(),
            zfs.clone(),
            keel_net::FakeNetManager::new(),
            keel_jail::FakeMountManager::new(),
            "zroot".to_string(),
            state_dir.clone(),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) =
            crate::worker::spawn(reconciler, zfs, "zroot".to_string(), None);

        let _handle = spawn(
            "node-1".to_string(),
            "10.0.0.1".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr.clone(),
            Duration::from_millis(50),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            node_reloading_tls(),
            commands,
            crate::PodCidrSlot::new(),
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(200));
        let body = get_nodes(&control_plane_addr);
        assert!(
            body.contains("backend_service: hugo-site"),
            "expected reported ingress health, got: {body}"
        );
        assert!(body.contains("host: example.com"), "got: {body}");
        assert!(
            body.contains("cert_expires_at_unix: 1800000000"),
            "got: {body}"
        );
    }

    #[test]
    fn a_heartbeat_aliases_and_proxies_an_applied_service() {
        let control_plane_addr = start_test_control_plane();
        let client_config = node_client_config();

        let service_yaml = "apiVersion: keel/v1\nkind: Service\nmetadata:\n  name: web\nspec:\n  replicas: 1\n  port: 9999\n  template:\n    image: base/14.2-web\n    command: [\"/usr/local/bin/myapp\"]\n    network:\n      vnet: true\n      bridge: keel0\n    resources:\n      cpu: \"1\"\n      memory: \"256M\"\n    restartPolicy: Always\n";
        send_request(
            &control_plane_addr,
            "PUT",
            "/services/web",
            service_yaml,
            &client_config,
        )
        .unwrap();

        let net = keel_net::FakeNetManager::new();
        net.ensure_bridge_exists("keel0").unwrap();
        let zfs = keel_zfs::FakeZfsManager::new();
        let state_dir = std::env::temp_dir().join(
            "keel-agentd-registration-test-a_heartbeat_aliases_and_proxies_an_applied_service",
        );
        let (_worker_handle, commands) = crate::worker::spawn(
            crate::Reconciler::new(
                keel_jail::FakeJailRuntime::new(),
                zfs.clone(),
                net.clone(),
                keel_jail::FakeMountManager::new(),
                "zroot".to_string(),
                state_dir.clone(),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap(),
            zfs,
            "zroot".to_string(),
            None,
        );
        let pod_cidr_slot = crate::PodCidrSlot::new();
        // Port 1 on loopback: guaranteed nothing is listening there, so the
        // control plane's own service-reconciliation forward attempt (it
        // tries to schedule this service's one desired replica onto this
        // node as part of every heartbeat) fails with an immediate
        // connection-refused rather than blocking on `FORWARD_CONNECT_TIMEOUT`
        // (2s, see keel-controlplane's `http.rs`) -- otherwise the very
        // first heartbeat response wouldn't return within this test's sleep
        // window below. The alias/proxy wiring under test here doesn't
        // depend on that scheduling attempt succeeding: the heartbeat
        // response's service-proxy entries come from the service table
        // directly, independent of whether a replica ever got placed.
        let _handle = spawn(
            "node-1".to_string(),
            "127.0.0.1:1".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr.clone(),
            Duration::from_millis(50),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            node_reloading_tls(),
            commands,
            pod_cidr_slot,
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(300));

        // The service's VIP was derived from --service-cidr on the test
        // control plane's own default; look it up rather than hardcoding it,
        // and assert on the alias existing for that address specifically --
        // this test only needs to prove the heartbeat -> proxy wiring works
        // end to end. `bridge_address` isn't a usable signal here: it only
        // reflects a jail's `attach_jail` gateway, a value `add_alias`
        // deliberately never touches (see keel-net's own
        // `a_bridges_gateway_and_its_service_alias_coexist_independently`).
        let svc_body =
            send_request(&control_plane_addr, "GET", "/services", "", &client_config).unwrap();
        let services: Vec<keel_controlplane::wire::ServiceSummary> =
            serde_yaml::from_slice(&svc_body).expect("malformed /services response");
        let vip = &services
            .iter()
            .find(|s| s.name == "web")
            .expect("expected the 'web' service to be listed")
            .vip;
        assert!(
            net.has_alias("keel0", vip),
            "expected keel0 to have the service's VIP ({vip}) aliased"
        );
    }

    #[test]
    fn send_request_to_a_peer_that_closes_mid_body_returns_err_not_a_silent_ok() {
        // The header claims a much larger body than what actually gets
        // written before the connection drops uncleanly (no close_notify).
        let addr = start_fake_control_plane_with_truncated_body(
            "this response claims to be far longer than what is actually sent back\n",
            "truncat",
        );

        let result = send_request(&addr, "GET", "/nodes", "", &node_client_config());

        assert!(
            result.is_err(),
            "expected a truncated response to be treated as a failure, got: {result:?}"
        );
    }

    #[test]
    fn registration_with_a_wrong_ca_certificate_never_registers() {
        let control_plane_addr = start_test_control_plane();
        let zfs = keel_zfs::FakeZfsManager::new();
        let state_dir = std::env::temp_dir().join("keel-agentd-registration-test-registration_with_a_wrong_ca_certificate_never_registers");
        let (_worker_handle, commands) = crate::worker::spawn(
            crate::Reconciler::new(
                keel_jail::FakeJailRuntime::new(),
                zfs.clone(),
                keel_net::FakeNetManager::new(),
                keel_jail::FakeMountManager::new(),
                "zroot".to_string(),
                state_dir.clone(),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap(),
            zfs,
            "zroot".to_string(),
            None,
        );
        let _handle = spawn(
            "node-1".to_string(),
            "10.0.0.1".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr.clone(),
            Duration::from_millis(50),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            wrong_ca_reloading_tls(),
            commands,
            crate::PodCidrSlot::new(),
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(200));
        let body = get_nodes(&control_plane_addr);
        assert!(
            !body.contains("node-1"),
            "expected node-1 to never register with a wrong-CA certificate, got: {body}"
        );
    }

    #[test]
    fn a_successful_registration_stores_the_returned_pod_cidr_in_the_slot() {
        let control_plane_addr = start_test_control_plane();
        let zfs = keel_zfs::FakeZfsManager::new();
        let state_dir = std::env::temp_dir().join("keel-agentd-registration-test-a_successful_registration_stores_the_returned_pod_cidr_in_the_slot");
        let (_worker_handle, commands) = crate::worker::spawn(
            crate::Reconciler::new(
                keel_jail::FakeJailRuntime::new(),
                zfs.clone(),
                keel_net::FakeNetManager::new(),
                keel_jail::FakeMountManager::new(),
                "zroot".to_string(),
                state_dir.clone(),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap(),
            zfs,
            "zroot".to_string(),
            None,
        );
        let pod_cidr_slot = crate::PodCidrSlot::new();
        let _handle = spawn(
            "node-1".to_string(),
            "10.0.0.1".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr,
            Duration::from_millis(50),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            node_reloading_tls(),
            commands,
            pod_cidr_slot.clone(),
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(200));
        assert!(
            pod_cidr_slot.get().is_some(),
            "expected the registration loop to have stored a pod_cidr by now"
        );
    }

    fn start_fake_control_plane_with_revoked_cert() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = std::sync::Arc::new(
            crate::tls::load_server_config(
                &fixture("revoked-node.crt"),
                &fixture("revoked-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let Ok(conn) = rustls::ServerConnection::new(std::sync::Arc::clone(&server_config))
                else {
                    continue;
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                let mut buf = [0u8; 4096];
                let _ = tls_stream.read(&mut buf);
            }
        });
        addr
    }

    #[test]
    fn send_request_to_a_peer_presenting_a_revoked_certificate_fails() {
        let addr = start_fake_control_plane_with_revoked_cert();
        let result = send_request(&addr, "GET", "/nodes", "", &node_client_config());
        assert!(
            result.is_err(),
            "expected a revoked peer certificate to fail the connection, got: {result:?}"
        );
    }

    #[test]
    fn route_reconciliation_adds_a_route_for_a_peer() {
        let control_plane_addr = start_test_control_plane();

        let client_config = node_client_config();
        send_request(
            &control_plane_addr,
            "POST",
            "/nodes/register",
            // A realistic peer `addr` is a `host:port` TCP bind address (as
            // produced by `--advertise-addr` since Milestone 8), not a bare
            // IP -- the gateway installed in the route table must still end
            // up as the bare host.
            "id: node-2\naddr: 10.0.0.2:7621\ncapacity_cpu: 4\ncapacity_memory: 8589934592\n",
            &client_config,
        )
        .unwrap();

        let net = keel_net::FakeNetManager::new();
        let zfs = keel_zfs::FakeZfsManager::new();
        let state_dir = std::env::temp_dir()
            .join("keel-agentd-registration-test-route_reconciliation_adds_a_route_for_a_peer");
        let (_worker_handle, commands) = crate::worker::spawn(
            crate::Reconciler::new(
                keel_jail::FakeJailRuntime::new(),
                zfs.clone(),
                net.clone(),
                keel_jail::FakeMountManager::new(),
                "zroot".to_string(),
                state_dir.clone(),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap(),
            zfs,
            "zroot".to_string(),
            None,
        );
        let pod_cidr_slot = crate::PodCidrSlot::new();
        let _handle = spawn(
            "node-1".to_string(),
            "10.0.0.1:7621".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr,
            Duration::from_millis(50),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            node_reloading_tls(),
            commands,
            pod_cidr_slot,
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(300));

        // derive_pod_cidr("node-2", "10.0.0.0/16") == 10.0.22.0/24 (verified independently; see this plan's Verified Facts table).
        assert_eq!(
            net.has_route("10.0.22.0/24"),
            Some("10.0.0.2".to_string()),
            "expected node-1 to have installed a route to node-2's pod_cidr via its advertised address, with the port stripped from the gateway"
        );
    }

    #[test]
    fn route_reconciliation_withdraws_a_route_once_the_peer_is_reported_dead() {
        // This test intentionally waits out the control plane's real
        // DEAD_THRESHOLD (hardcoded to 15s in
        // `keel_controlplane::registry::Registry`). There is no clock
        // injection available through the public HTTP/worker API used by
        // this integration-style test (the worker command loop calls
        // `Instant::now()` directly), so there's no way to fast-forward the
        // control plane's notion of "how long ago did this peer last
        // heartbeat" from outside the process without changing that
        // contract. A real wait is the only way to exercise the full
        // registration-loop -> reconcile_routes -> worker channel path for
        // route withdrawal (as opposed to `diff_routes`'s own pure-function
        // unit tests above, which only exercise hand-built `HashMap`s).
        let control_plane_addr = start_test_control_plane();

        let client_config = node_client_config();
        send_request(
            &control_plane_addr,
            "POST",
            "/nodes/register",
            "id: node-2\naddr: 10.0.0.2:7621\ncapacity_cpu: 4\ncapacity_memory: 8589934592\n",
            &client_config,
        )
        .unwrap();

        let net = keel_net::FakeNetManager::new();
        let zfs = keel_zfs::FakeZfsManager::new();
        let state_dir =
            std::env::temp_dir().join("keel-agentd-registration-test-route_reconciliation_withdraws_a_route_once_the_peer_is_reported_dead");
        let (_worker_handle, commands) = crate::worker::spawn(
            crate::Reconciler::new(
                keel_jail::FakeJailRuntime::new(),
                zfs.clone(),
                net.clone(),
                keel_jail::FakeMountManager::new(),
                "zroot".to_string(),
                state_dir.clone(),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap(),
            zfs,
            "zroot".to_string(),
            None,
        );
        let pod_cidr_slot = crate::PodCidrSlot::new();
        let _handle = spawn(
            "node-1".to_string(),
            "10.0.0.1:7621".to_string(),
            "10.0.0.9:7622".to_string(),
            control_plane_addr,
            Duration::from_millis(500),
            state_dir,
            4.0,
            8 * 1024 * 1024 * 1024,
            node_reloading_tls(),
            commands,
            pod_cidr_slot,
            crate::ServiceVipSlot::new(),
        );

        thread::sleep(Duration::from_millis(700));
        assert_eq!(
            net.has_route("10.0.22.0/24"),
            Some("10.0.0.2".to_string()),
            "expected node-1 to have installed a route to node-2's pod_cidr before node-2 goes dead"
        );

        // node-2 never heartbeats again after the single registration above,
        // so once DEAD_THRESHOLD elapses the control plane will report it as
        // Dead in `GET /nodes`, and node-1's registration loop should
        // withdraw the route on its next tick.
        thread::sleep(Duration::from_secs(15));

        assert_eq!(
            net.has_route("10.0.22.0/24"),
            None,
            "expected node-1 to have withdrawn the route once node-2 was reported dead"
        );
    }
}
