use crate::reconciler::ReconcileError;
use crate::wire::ErrorBody;
use crate::worker::Command;
use crate::{PodCidrSlot, ServiceVipSlot};
use ipnet::IpNet;
use keel_spec::JailSpec;
use rustls::{ServerConnection, StreamOwned};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread;

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

pub fn run(
    listener: UnixListener,
    commands: Sender<Command>,
    pod_cidr_slot: PodCidrSlot,
    service_vips: ServiceVipSlot,
    replica_targets: crate::ReplicaTargetRegistry,
) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let commands = commands.clone();
        let pod_cidr_slot = pod_cidr_slot.clone();
        let service_vips = service_vips.clone();
        let replica_targets = replica_targets.clone();
        thread::spawn(move || {
            let _ = handle_connection(stream, &commands, &pod_cidr_slot, &service_vips, &replica_targets);
        });
    }
}

pub fn run_tls(
    listener: TcpListener,
    commands: Sender<Command>,
    reloading_tls: Arc<crate::tls::ReloadingTls>,
    pod_cidr_slot: PodCidrSlot,
    service_vips: ServiceVipSlot,
    replica_targets: crate::ReplicaTargetRegistry,
) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let commands = commands.clone();
        let tls_config = reloading_tls.server_config();
        let pod_cidr_slot = pod_cidr_slot.clone();
        let service_vips = service_vips.clone();
        let replica_targets = replica_targets.clone();
        thread::spawn(move || {
            let Ok(conn) = ServerConnection::new(tls_config) else { return };
            let mut tls_stream = TlsStream::new(conn, stream);
            if handle_connection_tls(&mut tls_stream, &commands, &pod_cidr_slot, &service_vips, &replica_targets).is_err() {
                eprintln!("keel-agentd: TLS handshake or request handling failed for a connection");
            }
        });
    }
}

struct ParsedRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

fn handle_connection(
    mut stream: UnixStream,
    commands: &Sender<Command>,
    pod_cidr_slot: &PodCidrSlot,
    service_vips: &ServiceVipSlot,
    replica_targets: &crate::ReplicaTargetRegistry,
) -> io::Result<()> {
    let request = match read_request(&mut stream)? {
        Some(r) => r,
        None => return Ok(()),
    };
    let (status, body) = route(&request, commands, pod_cidr_slot, service_vips, replica_targets);
    write_response(&mut stream, status, &body)
}

fn handle_connection_tls(
    stream: &mut TlsStream,
    commands: &Sender<Command>,
    pod_cidr_slot: &PodCidrSlot,
    service_vips: &ServiceVipSlot,
    replica_targets: &crate::ReplicaTargetRegistry,
) -> io::Result<()> {
    let request = match read_request_tls(stream)? {
        Some(r) => r,
        None => return Ok(()),
    };
    let (status, body) = route(&request, commands, pod_cidr_slot, service_vips, replica_targets);
    write_response_tls(stream, status, &body)
}

fn read_request(stream: &mut UnixStream) -> io::Result<Option<ParsedRequest>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];

    let (method, path, header_len, content_length) = loop {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(header_len)) => {
                let content_length = req
                    .headers
                    .iter()
                    .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let method = req.method.unwrap_or("").to_string();
                let path = req.path.unwrap_or("").to_string();
                break (method, path, header_len, content_length);
            }
            Ok(httparse::Status::Partial) => {
                if buf.len() >= MAX_MESSAGE_BYTES {
                    return Ok(None);
                }
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Ok(None);
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => return Ok(None),
        }
    };

    let total_len = header_len + content_length;
    if total_len > MAX_MESSAGE_BYTES {
        return Ok(None);
    }
    while buf.len() < total_len {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf[header_len..total_len].to_vec();
    Ok(Some(ParsedRequest { method, path, body }))
}

fn read_request_tls(stream: &mut TlsStream) -> io::Result<Option<ParsedRequest>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];

    let (method, path, header_len, content_length) = loop {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(header_len)) => {
                let content_length = req
                    .headers
                    .iter()
                    .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let method = req.method.unwrap_or("").to_string();
                let path = req.path.unwrap_or("").to_string();
                break (method, path, header_len, content_length);
            }
            Ok(httparse::Status::Partial) => {
                if buf.len() >= MAX_MESSAGE_BYTES {
                    return Ok(None);
                }
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Ok(None);
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => return Ok(None),
        }
    };

    let total_len = header_len + content_length;
    if total_len > MAX_MESSAGE_BYTES {
        return Ok(None);
    }
    while buf.len() < total_len {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf[header_len..total_len].to_vec();
    Ok(Some(ParsedRequest { method, path, body }))
}

fn write_response(stream: &mut UnixStream, status: u16, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nContent-Type: application/yaml\r\nConnection: close\r\n\r\n",
        reason_phrase(status),
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn write_response_tls(stream: &mut TlsStream, status: u16, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nContent-Type: application/yaml\r\nConnection: close\r\n\r\n",
        reason_phrase(status),
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

fn route(
    request: &ParsedRequest,
    commands: &Sender<Command>,
    pod_cidr_slot: &PodCidrSlot,
    service_vips: &ServiceVipSlot,
    replica_targets: &crate::ReplicaTargetRegistry,
) -> (u16, Vec<u8>) {
    let segments: Vec<&str> =
        request.path.trim_start_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    match (request.method.as_str(), segments.as_slice()) {
        ("PUT", ["jails", name]) => handle_apply(name, &request.body, commands, pod_cidr_slot),
        ("GET", ["jails"]) => handle_get(None, commands),
        ("GET", ["jails", name]) => handle_get(Some(name.to_string()), commands),
        ("DELETE", ["jails", name]) => handle_delete(name, commands),
        ("PUT", ["jails", name, "replicate-to"]) => handle_set_replicate_to(name, &request.body, commands),
        ("GET", ["volumes"]) => handle_list_volumes(commands),
        ("GET", ["volumes", name]) => handle_get_volume(name, commands),
        ("DELETE", ["volumes", name]) => handle_delete_volume(name, commands),
        ("GET", ["replica-targets", name]) => handle_get_replica_target(name, replica_targets),
        ("PUT", ["ingress", name]) => handle_apply_ingress(name, &request.body, commands, service_vips),
        ("GET", ["ingress"]) => handle_get_ingress(None, commands),
        ("GET", ["ingress", name]) => handle_get_ingress(Some(name.to_string()), commands),
        ("DELETE", ["ingress", name]) => handle_delete_ingress(name, commands),
        _ => error_response(404, format!("no route for {} {}", request.method, request.path)),
    }
}

fn handle_get_replica_target(name: &str, replica_targets: &crate::ReplicaTargetRegistry) -> (u16, Vec<u8>) {
    match replica_targets.get(name) {
        None => error_response(404, format!("no replica target '{name}'")),
        Some(target) if target.last_snapshot.is_none() => {
            error_response(409, format!("replica target '{name}' has not completed a first full replication yet"))
        }
        Some(_) => yaml_response(200, &crate::wire::ReplicaTargetStatus { replica_name: name.to_string(), ready: true }),
    }
}

fn handle_apply(
    path_name: &str,
    body: &[u8],
    commands: &Sender<Command>,
    pod_cidr_slot: &PodCidrSlot,
) -> (u16, Vec<u8>) {
    let spec: JailSpec = match serde_yaml::from_slice(body) {
        Ok(s) => s,
        Err(e) => return error_response(400, format!("invalid YAML: {e}")),
    };
    if spec.metadata.name != path_name {
        return error_response(
            400,
            format!("path name '{path_name}' does not match spec.metadata.name '{}'", spec.metadata.name),
        );
    }
    if let Some(pod_cidr) = pod_cidr_slot.get() {
        // A malformed address is left to the existing `validate_address` check
        // inside `Command::Apply` below, rather than duplicated here.
        if let Ok(address) = spec.spec.network.address.parse::<IpNet>() {
            if !IpNet::V4(pod_cidr).contains(&address.addr()) {
                return error_response(
                    400,
                    format!(
                        "network.address '{}' is outside this node's assigned subnet {pod_cidr}",
                        spec.spec.network.address
                    ),
                );
            }
        }
    }

    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::Apply(spec, reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_apply_ingress(
    path_name: &str,
    body: &[u8],
    commands: &Sender<Command>,
    service_vips: &ServiceVipSlot,
) -> (u16, Vec<u8>) {
    let spec: keel_spec::IngressSpec = match serde_yaml::from_slice(body) {
        Ok(s) => s,
        Err(e) => return error_response(400, format!("invalid YAML: {e}")),
    };
    if spec.metadata.name != path_name {
        return error_response(
            400,
            format!("path name '{path_name}' does not match spec.metadata.name '{}'", spec.metadata.name),
        );
    }
    if !service_vips.names().contains(&spec.spec.backend.service) {
        return error_response(
            400,
            format!("backend.service '{}' does not name a currently-known Service", spec.spec.backend.service),
        );
    }

    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::ApplyIngress(spec, reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_get_ingress(name: Option<String>, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::GetIngress(name.clone(), reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    let statuses = match reply_rx.recv() {
        Ok(s) => s,
        Err(_) => return error_response(500, "reconciler worker did not respond".to_string()),
    };
    match name {
        Some(n) => match statuses.into_iter().next() {
            Some(status) => yaml_response(200, &status),
            None => error_response(404, format!("ingress '{n}' not found")),
        },
        None => yaml_response(200, &statuses),
    }
}

fn handle_delete_ingress(name: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::DeleteIngress(name.to_string(), reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_set_replicate_to(name: &str, body: &[u8], commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let replicate_to: Option<String> = if body.is_empty() {
        None
    } else {
        match serde_yaml::from_slice::<crate::wire::ReplicateToBody>(body) {
            Ok(b) => Some(b.replicate_to),
            Err(e) => return error_response(400, format!("invalid YAML: {e}")),
        }
    };
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::SetReplicateTo(name.to_string(), replicate_to, reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_get(name: Option<String>, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::Get(name.clone(), reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    let statuses = match reply_rx.recv() {
        Ok(s) => s,
        Err(_) => return error_response(500, "reconciler worker did not respond".to_string()),
    };
    match name {
        Some(n) => match statuses.into_iter().next() {
            Some(status) => yaml_response(200, &status),
            None => error_response(404, format!("jail '{n}' not found")),
        },
        None => yaml_response(200, &statuses),
    }
}

fn handle_delete(name: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::Delete(name.to_string(), reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_list_volumes(commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::ListVolumes(reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(volumes)) => yaml_response(200, &volumes),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_get_volume(name: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::GetVolume(name.to_string(), reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => yaml_response(200, &crate::wire::VolumeStatus { name: name.to_string() }),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_delete_volume(name: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::DeleteVolume(name.to_string(), reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn status_for_error(error: &ReconcileError) -> u16 {
    match error {
        ReconcileError::InvalidSpec(keel_spec::SpecError::ImmutableField(_)) => 409,
        ReconcileError::InvalidSpec(_) => 400,
        ReconcileError::NotFound(_) => 404,
        ReconcileError::Zfs(keel_zfs::ZfsError::NotFound(_)) => 404,
        ReconcileError::Zfs(keel_zfs::ZfsError::Busy(_)) => 409,
        _ => 500,
    }
}

fn error_response(status: u16, message: String) -> (u16, Vec<u8>) {
    let body = serde_yaml::to_string(&ErrorBody { error: message })
        .expect("ErrorBody serialization should not fail");
    (status, body.into_bytes())
}

fn yaml_response<T: serde::Serialize>(status: u16, value: &T) -> (u16, Vec<u8>) {
    let body = serde_yaml::to_string(value).expect("wire type serialization should not fail");
    (status, body.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconciler::Reconciler;
    use crate::worker;
    use keel_jail::{FakeJailRuntime, FakeMountManager};
    use keel_net::FakeNetManager;
    use keel_zfs::FakeZfsManager;
    use std::path::PathBuf;
    use std::time::Duration;

    fn sample_spec_yaml(name: &str) -> String {
        format!(
            "apiVersion: keel/v1\nkind: Jail\nmetadata:\n  name: {name}\nspec:\n  image: base/14.2-web\n  command: [\"/usr/local/bin/myapp\"]\n  network:\n    vnet: true\n    bridge: keel0\n    address: 10.0.0.5/24\n  resources:\n    cpu: \"2\"\n    memory: 512M\n  restartPolicy: Always\n"
        )
    }

    fn short_unique_socket_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("ka-{}-{}.sock", std::process::id(), id))
    }

    fn start_test_server(name: &str) -> PathBuf {
        let state_dir = std::env::temp_dir().join(format!("keel-agentd-http-test-state-{name}"));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let replica_targets = crate::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            state_dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string());

        let socket_path = short_unique_socket_path();
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path).unwrap();
        thread::spawn(move || run(listener, commands, PodCidrSlot::new(), crate::ServiceVipSlot::new(), replica_targets));
        socket_path
    }

    fn start_test_server_with_pod_cidr(name: &str, pod_cidr: Option<&str>) -> (PathBuf, PodCidrSlot) {
        let state_dir = std::env::temp_dir().join(format!("keel-agentd-http-test-state-{name}"));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let replica_targets = crate::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
        let reconciler = Reconciler::new(FakeJailRuntime::new(), zfs.clone(), FakeNetManager::new(), FakeMountManager::new(), "zroot".to_string(), state_dir, Box::new(keel_ingress::FakeAcmeClient::new()), Box::new(keel_ingress::FakeDnsProvider::new()), Box::new(crate::nginx::FakeNginxController::new()), crate::ServiceVipSlot::new()).unwrap();
        let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string());

        let pod_cidr_slot = PodCidrSlot::new();
        if let Some(cidr) = pod_cidr {
            pod_cidr_slot.set(cidr.parse().unwrap());
        }

        let socket_path = short_unique_socket_path();
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path).unwrap();
        let slot_clone = pod_cidr_slot.clone();
        thread::spawn(move || run(listener, commands, slot_clone, crate::ServiceVipSlot::new(), replica_targets));
        (socket_path, pod_cidr_slot)
    }

    fn start_test_server_with_replica_targets(name: &str, targets: crate::ReplicaTargetRegistry) -> PathBuf {
        let state_dir = std::env::temp_dir().join(format!("keel-agentd-http-test-state-{name}"));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let reconciler = Reconciler::new(FakeJailRuntime::new(), zfs.clone(), FakeNetManager::new(), FakeMountManager::new(), "zroot".to_string(), state_dir, Box::new(keel_ingress::FakeAcmeClient::new()), Box::new(keel_ingress::FakeDnsProvider::new()), Box::new(crate::nginx::FakeNginxController::new()), crate::ServiceVipSlot::new()).unwrap();
        let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string());

        let socket_path = short_unique_socket_path();
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path).unwrap();
        thread::spawn(move || run(listener, commands, PodCidrSlot::new(), crate::ServiceVipSlot::new(), targets));
        socket_path
    }

    fn start_test_server_with_service_vips(name: &str, entries: &[(&str, &str, u16)]) -> (PathBuf, crate::ServiceVipSlot) {
        let state_dir = std::env::temp_dir().join(format!("keel-agentd-http-test-state-{name}"));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let replica_targets = crate::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            state_dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string());

        let service_vips = crate::ServiceVipSlot::new();
        let proxy_entries: Vec<_> = entries
            .iter()
            .map(|(n, vip, port)| keel_controlplane::wire::ServiceProxyEntry {
                name: n.to_string(),
                vip: vip.to_string(),
                port: *port,
                replicas: vec![],
            })
            .collect();
        service_vips.set_all(&proxy_entries);

        let socket_path = short_unique_socket_path();
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path).unwrap();
        let thread_service_vips = service_vips.clone();
        thread::spawn(move || run(listener, commands, PodCidrSlot::new(), thread_service_vips, replica_targets));
        (socket_path, service_vips)
    }

    fn send_request(socket_path: &PathBuf, method: &str, path: &str, body: &str) -> (u16, String) {
        let mut stream = UnixStream::connect(socket_path).unwrap();
        let request =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        stream.write_all(request.as_bytes()).unwrap();
        // Under heavy scheduling contention (observed intermittently during
        // Milestone 5 VM verification, running the full workspace suite
        // concurrently), the server can finish reading the request,
        // respond, and close its end before we get here — the socket is
        // then already fully disconnected, a harmless race since our goal
        // (signal EOF so the server stops expecting more body) is already
        // moot if the server's done reading.
        if let Err(e) = stream.shutdown(std::net::Shutdown::Write) {
            assert_eq!(e.kind(), std::io::ErrorKind::NotConnected, "unexpected shutdown error: {e}");
        }

        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();

        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut parsed = httparse::Response::new(&mut headers);
        let header_len = match parsed.parse(&response).unwrap() {
            httparse::Status::Complete(len) => len,
            httparse::Status::Partial => panic!("incomplete response: {response:?}"),
        };
        let status = parsed.code.unwrap();
        let body = String::from_utf8(response[header_len..].to_vec()).unwrap();
        (status, body)
    }

    #[test]
    fn put_valid_spec_returns_200_and_provisions_the_jail() {
        let socket_path = start_test_server("put_valid_spec_returns_200_and_provisions_the_jail");
        let (status, _) = send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        assert_eq!(status, 200);

        let (status, body) = send_request(&socket_path, "GET", "/jails/web-1", "");
        assert_eq!(status, 200);
        assert!(body.contains("running: true"), "expected running: true in body: {body}");
    }

    #[test]
    fn put_with_mismatched_path_and_body_name_returns_400() {
        let socket_path = start_test_server("put_with_mismatched_path_and_body_name_returns_400");
        let (status, body) = send_request(&socket_path, "PUT", "/jails/other-name", &sample_spec_yaml("web-1"));
        assert_eq!(status, 400);
        assert!(body.contains("does not match"));
    }

    #[test]
    fn put_changing_an_immutable_field_returns_409() {
        let socket_path = start_test_server("put_changing_an_immutable_field_returns_409");
        send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));

        let changed_yaml = sample_spec_yaml("web-1").replace("base/14.2-web", "base/different-image");
        let (status, _) = send_request(&socket_path, "PUT", "/jails/web-1", &changed_yaml);
        assert_eq!(status, 409);
    }

    #[test]
    fn get_on_unknown_name_returns_404() {
        let socket_path = start_test_server("get_on_unknown_name_returns_404");
        let (status, _) = send_request(&socket_path, "GET", "/jails/missing", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn delete_on_unknown_name_returns_404() {
        let socket_path = start_test_server("delete_on_unknown_name_returns_404");
        let (status, _) = send_request(&socket_path, "DELETE", "/jails/missing", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn delete_removes_a_provisioned_jail() {
        let socket_path = start_test_server("delete_removes_a_provisioned_jail");
        send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        let (status, _) = send_request(&socket_path, "DELETE", "/jails/web-1", "");
        assert_eq!(status, 200);

        let (status, _) = send_request(&socket_path, "GET", "/jails/web-1", "");
        assert_eq!(status, 404, "deleted jail should no longer be found");
    }

    fn sample_spec_yaml_with_volume(name: &str, volume_name: &str) -> String {
        format!(
            "apiVersion: keel/v1\nkind: Jail\nmetadata:\n  name: {name}\nspec:\n  image: base/14.2-web\n  command: [\"/usr/local/bin/myapp\"]\n  network:\n    vnet: true\n    bridge: keel0\n    address: 10.0.0.5/24\n  resources:\n    cpu: \"2\"\n    memory: 512M\n  restartPolicy: Always\n  volumes:\n    - name: {volume_name}\n      mountPath: /data\n      size: 1G\n"
        )
    }

    #[test]
    fn get_volume_on_a_provisioned_volume_returns_200() {
        let socket_path = start_test_server("get_volume_on_a_provisioned_volume_returns_200");
        send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml_with_volume("web-1", "web-data"));

        let (status, body) = send_request(&socket_path, "GET", "/volumes/web-data", "");
        assert_eq!(status, 200);
        assert!(body.contains("web-data"));
    }

    #[test]
    fn get_volumes_lists_every_provisioned_volume() {
        let socket_path = start_test_server("get_volumes_lists_every_provisioned_volume");
        send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml_with_volume("web-1", "web-data"));

        let (status, body) = send_request(&socket_path, "GET", "/volumes", "");
        assert_eq!(status, 200);
        assert!(body.contains("web-data"), "got: {body}");
    }

    #[test]
    fn get_volumes_on_a_pool_with_no_volumes_returns_an_empty_list() {
        let socket_path = start_test_server("get_volumes_on_a_pool_with_no_volumes_returns_an_empty_list");
        let (status, body) = send_request(&socket_path, "GET", "/volumes", "");
        assert_eq!(status, 200);
        assert_eq!(body.trim(), "[]");
    }

    #[test]
    fn get_volume_on_an_unknown_name_returns_404() {
        let socket_path = start_test_server("get_volume_on_an_unknown_name_returns_404");
        let (status, _) = send_request(&socket_path, "GET", "/volumes/missing", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn delete_volume_on_an_unknown_name_returns_404() {
        let socket_path = start_test_server("delete_volume_on_an_unknown_name_returns_404");
        let (status, _) = send_request(&socket_path, "DELETE", "/volumes/missing", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn delete_volume_survives_the_owning_jails_deletion_then_succeeds() {
        let socket_path = start_test_server("delete_volume_survives_the_owning_jails_deletion_then_succeeds");
        send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml_with_volume("web-1", "web-data"));
        send_request(&socket_path, "DELETE", "/jails/web-1", "");

        let (status, _) = send_request(&socket_path, "GET", "/volumes/web-data", "");
        assert_eq!(status, 200, "the volume dataset must survive the jail's deletion");

        let (status, _) = send_request(&socket_path, "DELETE", "/volumes/web-data", "");
        assert_eq!(status, 200);

        let (status, _) = send_request(&socket_path, "GET", "/volumes/web-data", "");
        assert_eq!(status, 404, "the volume should be gone for good now");
    }

    #[test]
    fn put_replicate_to_retargets_an_existing_jails_replication_address() {
        let socket_path = start_test_server("put_replicate_to_retargets_an_existing_jails_replication_address");
        send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));

        let (status, _) = send_request(&socket_path, "PUT", "/jails/web-1/replicate-to", "replicate_to: 10.0.0.9:7622\n");
        assert_eq!(status, 200);

        let (status, body) = send_request(&socket_path, "GET", "/jails/web-1", "");
        assert_eq!(status, 200);
        assert!(body.contains("10.0.0.9:7622"), "got: {body}");
    }

    #[test]
    fn put_replicate_to_on_an_unknown_name_returns_404() {
        let socket_path = start_test_server("put_replicate_to_on_an_unknown_name_returns_404");
        let (status, _) = send_request(&socket_path, "PUT", "/jails/missing/replicate-to", "replicate_to: 10.0.0.9:7622\n");
        assert_eq!(status, 404);
    }

    #[test]
    fn get_jails_lists_all_applied_jails() {
        let socket_path = start_test_server("get_jails_lists_all_applied_jails");
        send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        send_request(&socket_path, "PUT", "/jails/web-2", &sample_spec_yaml("web-2"));

        let (status, body) = send_request(&socket_path, "GET", "/jails", "");
        assert_eq!(status, 200);
        assert!(body.contains("web-1"));
        assert!(body.contains("web-2"));
    }

    #[test]
    fn oversized_content_length_closes_the_connection_without_reading_the_body() {
        let socket_path =
            start_test_server("oversized_content_length_closes_the_connection_without_reading_the_body");
        let mut stream = UnixStream::connect(&socket_path).unwrap();
        let request = format!(
            "PUT /jails/web-1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            MAX_MESSAGE_BYTES + 1
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.shutdown(std::net::Shutdown::Write).ok();

        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        assert!(
            response.is_empty(),
            "server should close the connection without responding to an oversized request, got: {response:?}"
        );
    }

    #[test]
    fn put_with_address_inside_the_stored_pod_cidr_is_accepted() {
        let (socket_path, _slot) = start_test_server_with_pod_cidr("put_with_address_inside_the_stored_pod_cidr_is_accepted", Some("10.0.4.0/24"));
        let yaml = sample_spec_yaml("web-1").replace("10.0.0.5/24", "10.0.4.5/24");
        let (status, _) = send_request(&socket_path, "PUT", "/jails/web-1", &yaml);
        assert_eq!(status, 200);
    }

    #[test]
    fn put_with_address_outside_the_stored_pod_cidr_is_rejected_before_any_side_effect() {
        let (socket_path, _slot) = start_test_server_with_pod_cidr("put_with_address_outside_the_stored_pod_cidr_is_rejected", Some("10.0.4.0/24"));
        let (status, body) = send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        assert_eq!(status, 400);
        assert!(body.contains("10.0.0.5/24"), "expected the given address in the error, got: {body}");
        assert!(body.contains("10.0.4.0/24"), "expected the node's actual block in the error, got: {body}");

        let (status, _) = send_request(&socket_path, "GET", "/jails/web-1", "");
        assert_eq!(status, 404, "the rejected apply must never have reached the reconciler");
    }

    #[test]
    fn put_with_no_stored_pod_cidr_skips_the_subnet_check() {
        let (socket_path, _slot) = start_test_server_with_pod_cidr("put_with_no_stored_pod_cidr_skips_the_subnet_check", None);
        let (status, _) = send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        assert_eq!(status, 200, "single-node/never-registered mode must skip the subnet check entirely");
    }

    #[test]
    fn put_ingress_then_get_it_back() {
        let (socket, service_vips) =
            start_test_server_with_service_vips("put_ingress_then_get_it_back", &[("hugo-site", "10.0.0.9", 8080)]);
        let body = "apiVersion: keel/v1\nkind: Ingress\nmetadata:\n  name: blog\nspec:\n  host: example.com\n  backend:\n    service: hugo-site\n    port: 8080\n  tls:\n    email: admin@example.com\n";
        let (status, _) = send_request(&socket, "PUT", "/ingress/blog", body);
        assert_eq!(status, 200);
        let (status, response_body) = send_request(&socket, "GET", "/ingress/blog", "");
        assert_eq!(status, 200);
        assert!(response_body.contains("host: example.com"), "got: {response_body}");
        let _ = service_vips;
    }

    #[test]
    fn put_ingress_rejects_an_unknown_backend_service() {
        let (socket, _service_vips) =
            start_test_server_with_service_vips("put_ingress_rejects_an_unknown_backend_service", &[]);
        let body = "apiVersion: keel/v1\nkind: Ingress\nmetadata:\n  name: blog\nspec:\n  host: example.com\n  backend:\n    service: does-not-exist\n    port: 8080\n  tls:\n    email: admin@example.com\n";
        let (status, response_body) = send_request(&socket, "PUT", "/ingress/blog", body);
        assert_eq!(status, 400);
        assert!(response_body.contains("does-not-exist"), "got: {response_body}");
    }

    #[test]
    fn delete_ingress_then_get_is_404() {
        let (socket, _service_vips) =
            start_test_server_with_service_vips("delete_ingress_then_get_is_404", &[("hugo-site", "10.0.0.9", 8080)]);
        let body = "apiVersion: keel/v1\nkind: Ingress\nmetadata:\n  name: blog\nspec:\n  host: example.com\n  backend:\n    service: hugo-site\n    port: 8080\n  tls:\n    email: admin@example.com\n";
        let _ = send_request(&socket, "PUT", "/ingress/blog", body);
        let (status, _) = send_request(&socket, "DELETE", "/ingress/blog", "");
        assert_eq!(status, 200);
        let (status, _) = send_request(&socket, "GET", "/ingress/blog", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn get_ingress_lists_all_applied_ingresses() {
        let (socket, _service_vips) = start_test_server_with_service_vips(
            "get_ingress_lists_all_applied_ingresses",
            &[("hugo-site", "10.0.0.9", 8080), ("umami", "10.0.0.10", 3000)],
        );
        let blog_body = "apiVersion: keel/v1\nkind: Ingress\nmetadata:\n  name: blog\nspec:\n  host: example.com\n  backend:\n    service: hugo-site\n    port: 8080\n  tls:\n    email: admin@example.com\n";
        let stats_body = "apiVersion: keel/v1\nkind: Ingress\nmetadata:\n  name: stats\nspec:\n  host: stats.example.com\n  backend:\n    service: umami\n    port: 3000\n  tls:\n    email: admin@example.com\n";
        send_request(&socket, "PUT", "/ingress/blog", blog_body);
        send_request(&socket, "PUT", "/ingress/stats", stats_body);

        let (status, body) = send_request(&socket, "GET", "/ingress", "");
        assert_eq!(status, 200);
        assert!(body.contains("example.com"), "got: {body}");
        assert!(body.contains("stats.example.com"), "got: {body}");
    }

    #[test]
    fn get_replica_target_on_an_unknown_name_returns_404() {
        let dir = std::env::temp_dir().join("keel-agentd-http-test-replica-targets-unknown");
        let _ = std::fs::remove_dir_all(&dir);
        let targets = crate::ReplicaTargetRegistry::load(dir).unwrap();
        let socket_path = start_test_server_with_replica_targets("get_replica_target_on_an_unknown_name_returns_404", targets);
        let (status, _) = send_request(&socket_path, "GET", "/replica-targets/missing", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn get_replica_target_before_a_first_snapshot_returns_409() {
        let dir = std::env::temp_dir().join("keel-agentd-http-test-replica-targets-not-ready");
        let _ = std::fs::remove_dir_all(&dir);
        let targets = crate::ReplicaTargetRegistry::load(dir).unwrap();
        targets.ensure_for_test("db-0", "zroot/keel/volumes/db-0-data", "10.0.0.4:7621");
        let socket_path = start_test_server_with_replica_targets("get_replica_target_before_a_first_snapshot_returns_409", targets);
        let (status, _) = send_request(&socket_path, "GET", "/replica-targets/db-0", "");
        assert_eq!(status, 409);
    }

    #[test]
    fn get_replica_target_after_a_first_snapshot_returns_200_and_ready_true() {
        let dir = std::env::temp_dir().join("keel-agentd-http-test-replica-targets-ready");
        let _ = std::fs::remove_dir_all(&dir);
        let targets = crate::ReplicaTargetRegistry::load(dir).unwrap();
        targets.ensure_for_test("db-0", "zroot/keel/volumes/db-0-data", "10.0.0.4:7621");
        targets.record_snapshot_for_test("db-0", "keel-repl-1");
        let socket_path = start_test_server_with_replica_targets("get_replica_target_after_a_first_snapshot_returns_200_and_ready_true", targets);
        let (status, body) = send_request(&socket_path, "GET", "/replica-targets/db-0", "");
        assert_eq!(status, 200);
        assert!(body.contains("ready: true"), "got: {body}");
    }

    use crate::tls;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn start_tcp_test_server(name: &str) -> String {
        let state_dir = std::env::temp_dir().join(format!("keel-agentd-http-tcp-test-state-{name}"));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let replica_targets = crate::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            state_dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let reloading_tls = tls::ReloadingTls::spawn(
            fixture("fixture-node.crt"),
            fixture("fixture-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap();
        thread::spawn(move || run_tls(listener, commands, reloading_tls, PodCidrSlot::new(), crate::ServiceVipSlot::new(), replica_targets));
        addr
    }

    fn client_tls_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            tls::load_client_config(&fixture("fixture-client.crt"), &fixture("fixture-client.key"), &fixture("ca.crt"), &fixture("crl.pem"))
                .unwrap(),
        )
    }

    fn send_request_tcp(addr: &str, method: &str, path: &str, body: &str) -> (u16, String) {
        let server_name = tls::server_name_from_addr(addr).unwrap();
        let tcp_stream = TcpStream::connect(addr).unwrap();
        let conn = rustls::ClientConnection::new(client_tls_config(), server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);
        let request =
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        stream.write_all(request.as_bytes()).unwrap();
        stream.sock.shutdown(std::net::Shutdown::Write).ok();
        let mut response = Vec::new();
        // rustls surfaces a plain TCP close that lacks a TLS `close_notify`
        // alert as `ErrorKind::UnexpectedEof` rather than `Ok(0)`; any bytes
        // already read (the full response, in the success path this helper
        // is used for) are still appended to `response` before the error is
        // returned, so it's safe to ignore this specific error here.
        let _ = stream.read_to_end(&mut response);
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut parsed = httparse::Response::new(&mut headers);
        let header_len = match parsed.parse(&response).unwrap() {
            httparse::Status::Complete(len) => len,
            httparse::Status::Partial => panic!("incomplete response: {response:?}"),
        };
        let status = parsed.code.unwrap();
        let body = String::from_utf8(response[header_len..].to_vec()).unwrap();
        (status, body)
    }

    #[test]
    fn put_valid_spec_over_tcp_returns_200_and_provisions_the_jail() {
        let addr = start_tcp_test_server("put_valid_spec_over_tcp_returns_200_and_provisions_the_jail");
        let (status, _) = send_request_tcp(&addr, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        assert_eq!(status, 200);

        let (status, body) = send_request_tcp(&addr, "GET", "/jails/web-1", "");
        assert_eq!(status, 200);
        assert!(body.contains("running: true"), "expected running: true in body: {body}");
    }

    #[test]
    fn get_jails_over_tcp_lists_all_applied_jails() {
        let addr = start_tcp_test_server("get_jails_over_tcp_lists_all_applied_jails");
        send_request_tcp(&addr, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        send_request_tcp(&addr, "PUT", "/jails/web-2", &sample_spec_yaml("web-2"));

        let (status, body) = send_request_tcp(&addr, "GET", "/jails", "");
        assert_eq!(status, 200);
        assert!(body.contains("web-1"));
        assert!(body.contains("web-2"));
    }

    #[test]
    fn delete_over_tcp_removes_a_provisioned_jail() {
        let addr = start_tcp_test_server("delete_over_tcp_removes_a_provisioned_jail");
        send_request_tcp(&addr, "PUT", "/jails/web-1", &sample_spec_yaml("web-1"));
        let (status, _) = send_request_tcp(&addr, "DELETE", "/jails/web-1", "");
        assert_eq!(status, 200);

        let (status, _) = send_request_tcp(&addr, "GET", "/jails/web-1", "");
        assert_eq!(status, 404, "deleted jail should no longer be found");
    }

    #[test]
    fn a_client_with_no_certificate_cannot_complete_the_tcp_handshake() {
        let addr = start_tcp_test_server("a_client_with_no_certificate_cannot_complete_the_tcp_handshake");
        let roots = {
            let mut roots = rustls::RootCertStore::empty();
            let cert = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(fixture("ca.crt")).unwrap()))
                .next()
                .unwrap()
                .unwrap();
            roots.add(cert).unwrap();
            roots
        };
        let bare_config = Arc::new(rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth());
        let server_name = tls::server_name_from_addr(&addr).unwrap();
        let tcp_stream = TcpStream::connect(&addr).unwrap();
        let conn = rustls::ClientConnection::new(bare_config, server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);
        // Under TLS 1.3, a client can finish its own side of the handshake
        // (and thus have write_all succeed) before it has read the server's
        // rejection alert, since the client doesn't wait for the server's
        // acknowledgement before considering itself done. So the failure
        // must be observed across the full write+read round trip, not the
        // write alone.
        let write_result = stream.write_all(b"GET /jails HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        let mut response = Vec::new();
        let read_result = stream.read_to_end(&mut response);
        assert!(
            write_result.is_err() || read_result.is_err(),
            "expected the handshake to fail with no client certificate presented"
        );
    }

    #[test]
    fn a_client_with_a_wrong_ca_certificate_cannot_complete_the_tcp_handshake() {
        let addr = start_tcp_test_server("a_client_with_a_wrong_ca_certificate_cannot_complete_the_tcp_handshake");
        let wrong_config = Arc::new(
            tls::load_client_config(&fixture("wrong-ca-node.crt"), &fixture("wrong-ca-node.key"), &fixture("ca.crt"), &fixture("crl.pem"))
                .unwrap(),
        );
        let server_name = tls::server_name_from_addr(&addr).unwrap();
        let tcp_stream = TcpStream::connect(&addr).unwrap();
        let conn = rustls::ClientConnection::new(wrong_config, server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);
        // See the comment in the no-certificate test above: the failure must
        // be observed across the full write+read round trip, not the write
        // alone, since a TLS 1.3 client can finish its own handshake side
        // before reading the server's rejection.
        let write_result = stream.write_all(b"GET /jails HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        let mut response = Vec::new();
        let read_result = stream.read_to_end(&mut response);
        assert!(
            write_result.is_err() || read_result.is_err(),
            "expected the handshake to fail for a wrong-CA client certificate"
        );
    }

    #[test]
    fn a_client_with_a_revoked_certificate_cannot_complete_the_tcp_handshake() {
        let addr = start_tcp_test_server("a_client_with_a_revoked_certificate_cannot_complete_the_tcp_handshake");
        let revoked_config = Arc::new(
            tls::load_client_config(
                &fixture("revoked-node.crt"),
                &fixture("revoked-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        let server_name = tls::server_name_from_addr(&addr).unwrap();
        let tcp_stream = TcpStream::connect(&addr).unwrap();
        let conn = rustls::ClientConnection::new(revoked_config, server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);
        let write_result = stream.write_all(b"GET /jails HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        let mut response = Vec::new();
        let read_result = stream.read_to_end(&mut response);
        assert!(
            write_result.is_err() || read_result.is_err(),
            "expected the handshake to fail for a revoked client certificate"
        );
    }

    #[test]
    fn reloading_tls_server_config_picks_up_a_replaced_certificate_without_restart() {
        let cert_dir = std::env::temp_dir().join(format!("keel-agentd-reload-test-{}", std::process::id()));
        std::fs::create_dir_all(&cert_dir).unwrap();
        let cert_path = cert_dir.join("node.crt");
        let key_path = cert_dir.join("node.key");
        std::fs::copy(fixture("fixture-node.crt"), &cert_path).unwrap();
        std::fs::copy(fixture("fixture-node.key"), &key_path).unwrap();

        let reloading = tls::ReloadingTls::spawn(
            cert_path.clone(),
            key_path.clone(),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_millis(50),
        )
        .unwrap();

        let state_dir = std::env::temp_dir()
            .join(format!("keel-agentd-reload-test-state-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let replica_targets = crate::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
        let reconciler =
            Reconciler::new(FakeJailRuntime::new(), zfs.clone(), FakeNetManager::new(), FakeMountManager::new(), "zroot".to_string(), state_dir, Box::new(keel_ingress::FakeAcmeClient::new()), Box::new(keel_ingress::FakeDnsProvider::new()), Box::new(crate::nginx::FakeNginxController::new()), crate::ServiceVipSlot::new()).unwrap();
        let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || run_tls(listener, commands, reloading, PodCidrSlot::new(), crate::ServiceVipSlot::new(), replica_targets));

        let (status, _) = send_request_tcp(&addr, "GET", "/jails", "");
        assert_eq!(status, 200, "expected the initial fixture-node certificate to be served");

        std::fs::copy(fixture("wrong-ca-node.crt"), &cert_path).unwrap();
        std::fs::copy(fixture("wrong-ca-node.key"), &key_path).unwrap();
        thread::sleep(Duration::from_millis(200));

        let result = std::panic::catch_unwind(|| send_request_tcp(&addr, "GET", "/jails", ""));
        assert!(
            result.is_err() || result.unwrap().0 != 200,
            "expected the server's replaced certificate to be rejected by the client after reload"
        );
    }

    #[test]
    fn reloading_tls_keeps_serving_the_last_good_config_if_the_replacement_is_malformed() {
        let cert_dir = std::env::temp_dir().join(format!("keel-agentd-reload-bad-test-{}", std::process::id()));
        std::fs::create_dir_all(&cert_dir).unwrap();
        let cert_path = cert_dir.join("node.crt");
        let key_path = cert_dir.join("node.key");
        std::fs::copy(fixture("fixture-node.crt"), &cert_path).unwrap();
        std::fs::copy(fixture("fixture-node.key"), &key_path).unwrap();

        let reloading = tls::ReloadingTls::spawn(
            cert_path.clone(),
            key_path.clone(),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_millis(50),
        )
        .unwrap();

        let state_dir = std::env::temp_dir()
            .join(format!("keel-agentd-reload-bad-test-state-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let replica_targets = crate::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
        let reconciler =
            Reconciler::new(FakeJailRuntime::new(), zfs.clone(), FakeNetManager::new(), FakeMountManager::new(), "zroot".to_string(), state_dir, Box::new(keel_ingress::FakeAcmeClient::new()), Box::new(keel_ingress::FakeDnsProvider::new()), Box::new(crate::nginx::FakeNginxController::new()), crate::ServiceVipSlot::new()).unwrap();
        let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || run_tls(listener, commands, reloading, PodCidrSlot::new(), crate::ServiceVipSlot::new(), replica_targets));

        std::fs::write(&cert_path, "not a certificate").unwrap();
        thread::sleep(Duration::from_millis(200));

        let (status, _) = send_request_tcp(&addr, "GET", "/jails", "");
        assert_eq!(status, 200, "expected the last-known-good certificate to keep being served after a malformed reload");
    }
}
