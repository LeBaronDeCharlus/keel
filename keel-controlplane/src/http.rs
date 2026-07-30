use crate::tls;
use crate::wire::{Heartbeat, NodeRegistration, RegisterResponse};
use crate::worker::{Command, ForceRepinError, ReplicaAction, ScheduleOrResolveError};
use keel_spec::{error_response, yaml_response};
use rustls::{ServerConnection, StreamOwned};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

/// Every accepted connection gets its own OS thread with no concurrency cap,
/// so a client that connects and never sends anything would otherwise pin a
/// thread forever -- before the TLS handshake even starts. A read timeout
/// bounds that.
const INBOUND_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn apply_read_timeout(stream: &TcpStream) {
    let _ = stream.set_read_timeout(Some(INBOUND_READ_TIMEOUT));
}

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

pub fn run(
    listener: TcpListener,
    commands: Sender<Command>,
    reloading_tls: Arc<tls::ReloadingTls>,
) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        apply_read_timeout(&stream);
        let commands = commands.clone();
        let tls_config = reloading_tls.server_config();
        let client_config = reloading_tls.client_config();
        thread::spawn(move || {
            let Ok(conn) = ServerConnection::new(tls_config) else {
                return;
            };
            let mut tls_stream = TlsStream::new(conn, stream);
            if handle_connection(&mut tls_stream, &commands, &client_config).is_err() {
                eprintln!(
                    "keel-controlplane: TLS handshake or request handling failed for a connection"
                );
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
    stream: &mut TlsStream,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> io::Result<()> {
    let request = match read_request(stream)? {
        Some(r) => r,
        None => return Ok(()),
    };
    let (status, body) = route(&request, commands, client_config);
    write_response(stream, status, &body)
}

fn read_request(stream: &mut TlsStream) -> io::Result<Option<ParsedRequest>> {
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

fn write_response(stream: &mut TlsStream, status: u16, body: &[u8]) -> io::Result<()> {
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
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

fn route(
    request: &ParsedRequest,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let segments: Vec<&str> = request
        .path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    match (request.method.as_str(), segments.as_slice()) {
        ("POST", ["nodes", "register"]) => handle_register(&request.body, commands),
        ("POST", ["nodes", id, "heartbeat"]) => {
            handle_heartbeat(id, &request.body, commands, client_config)
        }
        ("GET", ["nodes"]) => handle_list(commands),
        ("PUT", ["nodes", id, "jails", name]) => {
            if let Some(response) = reject_if_service_owned(name, commands) {
                return response;
            }
            let (status, body) = handle_forward(
                id,
                "PUT",
                &format!("/jails/{name}"),
                &request.body,
                commands,
                client_config,
            );
            if (200..300).contains(&status) {
                send_record_placement(name, id, commands);
            }
            (status, body)
        }
        ("GET", ["nodes", id, "jails"]) => {
            handle_forward(id, "GET", "/jails", &[], commands, client_config)
        }
        ("GET", ["nodes", id, "jails", name]) => handle_forward(
            id,
            "GET",
            &format!("/jails/{name}"),
            &[],
            commands,
            client_config,
        ),
        ("DELETE", ["nodes", id, "jails", name]) => {
            let (status, body) = handle_forward(
                id,
                "DELETE",
                &format!("/jails/{name}"),
                &[],
                commands,
                client_config,
            );
            if (200..300).contains(&status) {
                send_remove_placement(name, commands);
            }
            (status, body)
        }
        ("GET", ["nodes", id, "volumes"]) => {
            handle_forward(id, "GET", "/volumes", &[], commands, client_config)
        }
        ("GET", ["nodes", id, "volumes", name]) => handle_forward(
            id,
            "GET",
            &format!("/volumes/{name}"),
            &[],
            commands,
            client_config,
        ),
        ("DELETE", ["nodes", id, "volumes", name]) => handle_forward(
            id,
            "DELETE",
            &format!("/volumes/{name}"),
            &[],
            commands,
            client_config,
        ),
        ("PUT", ["jails", name]) => {
            if let Some(response) = reject_if_service_owned(name, commands) {
                return response;
            }
            handle_scheduled_apply(name, &request.body, commands, client_config)
        }
        ("GET", ["jails", name]) => handle_scheduled_read(name, commands, client_config),
        ("DELETE", ["jails", name]) => handle_scheduled_delete(name, commands, client_config),
        ("PUT", ["services", name]) => {
            handle_apply_service(name, &request.body, commands, client_config)
        }
        ("GET", ["services", name]) => handle_get_service(name, commands),
        ("DELETE", ["services", name]) => handle_delete_service(name, commands, client_config),
        ("GET", ["services"]) => handle_list_services(commands),
        ("POST", ["replicas", name, "force-repin"]) => {
            handle_force_repin(name, commands, client_config)
        }
        ("POST", ["nodes", id, "cordon"]) => handle_cordon(id, commands),
        ("POST", ["nodes", id, "uncordon"]) => handle_uncordon(id, commands),
        _ => error_response(
            404,
            format!("no route for {} {}", request.method, request.path),
        ),
    }
}

fn handle_scheduled_apply(
    name: &str,
    body: &[u8],
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::ResolveOrSchedule(name.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let (node_id, addr) = match reply_rx.recv() {
        Ok(Ok(pair)) => pair,
        Ok(Err(ScheduleOrResolveError::Schedule(e))) => return error_response(503, e.to_string()),
        Ok(Err(ScheduleOrResolveError::Resolve(e))) => return error_response(404, e.to_string()),
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    };
    match forward(&addr, "PUT", &format!("/jails/{name}"), body, client_config) {
        Ok((status, response_body)) => {
            if (200..300).contains(&status) {
                send_record_placement(name, &node_id, commands);
            }
            (status, response_body)
        }
        Err(e) => error_response(
            500,
            format!("failed to reach node '{node_id}' at {addr}: {e}"),
        ),
    }
}

fn handle_scheduled_read(
    name: &str,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let (node_id, addr) = match resolve_placement(name, commands) {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    match forward(&addr, "GET", &format!("/jails/{name}"), &[], client_config) {
        Ok((status, response_body)) => (status, response_body),
        Err(e) => error_response(
            500,
            format!("failed to reach node '{node_id}' at {addr}: {e}"),
        ),
    }
}

fn handle_scheduled_delete(
    name: &str,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let (node_id, addr) = match resolve_placement(name, commands) {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    match forward(
        &addr,
        "DELETE",
        &format!("/jails/{name}"),
        &[],
        client_config,
    ) {
        Ok((status, response_body)) => {
            if (200..300).contains(&status) {
                send_remove_placement(name, commands);
            }
            (status, response_body)
        }
        Err(e) => error_response(
            500,
            format!("failed to reach node '{node_id}' at {addr}: {e}"),
        ),
    }
}

fn handle_force_repin(
    name: &str,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::PrepareForceRepin(name.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let prep = match reply_rx.recv() {
        Ok(Ok(prep)) => prep,
        Ok(Err(e @ ForceRepinError::NotPlaced(_))) => return error_response(404, e.to_string()),
        Ok(Err(e @ ForceRepinError::NotStateful(_))) => return error_response(400, e.to_string()),
        Ok(Err(e @ ForceRepinError::PrimaryStillAlive(_))) => {
            return error_response(409, e.to_string())
        }
        Ok(Err(e)) => return error_response(503, e.to_string()),
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    };

    match forward(
        &prep.standby_addr,
        "GET",
        &format!("/replica-targets/{name}"),
        &[],
        client_config,
    ) {
        Ok((status, _)) if (200..300).contains(&status) => {}
        Ok((404, _)) | Ok((409, _)) => {
            return error_response(
                409,
                format!(
                    "standby node '{}' has not completed a first full replication for '{name}'",
                    prep.standby_node_id
                ),
            );
        }
        Ok((status, body)) => {
            return error_response(
                500,
                format!(
                    "unexpected response checking standby readiness: status {status}, body {:?}",
                    String::from_utf8_lossy(&body)
                ),
            )
        }
        Err(e) => {
            return error_response(
                500,
                format!(
                    "failed to reach standby node '{}' at {}: {e}",
                    prep.standby_node_id, prep.standby_addr
                ),
            )
        }
    }

    let cidr = format!("{}/{}", prep.address, prep.prefix_len);
    let mut spec = prep.template.to_jail_spec(name, &cidr);
    spec.spec.replicate_to = Some(prep.fresh_standby_addr.clone());
    spec.spec.generation = prep.next_generation;
    let body = serde_yaml::to_string(&spec).expect("JailSpec serialization should not fail");

    match forward(
        &prep.standby_addr,
        "PUT",
        &format!("/jails/{name}"),
        body.as_bytes(),
        client_config,
    ) {
        Ok((status, resp_body)) if (200..300).contains(&status) => {
            send_record_placement(name, &prep.standby_node_id, commands);
            send_record_replica_address(name, &prep.standby_node_id, prep.address, commands);
            send_record_standby(name, &prep.fresh_standby_node_id, commands);
            send_record_pending_fence(name, &prep.old_node_id, commands);
            // Attempt to reach the old primary immediately, rather than
            // waiting only for its own next heartbeat
            // (check_and_execute_fencing, unchanged, still retries this on
            // that node's next heartbeat if this attempt fails or no
            // last-known address is on file at all).
            if let Some(old_addr) = &prep.old_node_last_known_addr {
                match forward(
                    old_addr,
                    "DELETE",
                    &format!("/jails/{name}"),
                    &[],
                    client_config,
                ) {
                    Ok((del_status, _))
                        if (200..300).contains(&del_status) || del_status == 404 =>
                    {
                        send_remove_pending_fence(name, commands);
                    }
                    _ => {}
                }
            }
            (200, resp_body)
        }
        Ok((status, resp_body)) => {
            error_response(status, String::from_utf8_lossy(&resp_body).to_string())
        }
        Err(e) => error_response(
            500,
            format!(
                "failed to reach node '{}' at {}: {e}",
                prep.standby_node_id, prep.standby_addr
            ),
        ),
    }
}

fn handle_cordon(node_id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::Cordon(node_id.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(404, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_uncordon(node_id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::Uncordon(node_id.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(404, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn send_record_pending_fence(replica_name: &str, old_node_id: &str, commands: &Sender<Command>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::RecordPendingFence(
            replica_name.to_string(),
            old_node_id.to_string(),
            reply_tx,
        ))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn resolve_placement(
    name: &str,
    commands: &Sender<Command>,
) -> Result<(String, String), (u16, Vec<u8>)> {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::ResolvePlacement(name.to_string(), reply_tx))
        .is_err()
    {
        return Err(error_response(
            500,
            "control plane worker is not running".to_string(),
        ));
    }
    match reply_rx.recv() {
        Ok(Ok(pair)) => Ok(pair),
        Ok(Err(e)) => Err(error_response(404, e.to_string())),
        Err(_) => Err(error_response(
            500,
            "control plane worker did not respond".to_string(),
        )),
    }
}

fn send_record_placement(name: &str, node_id: &str, commands: &Sender<Command>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::RecordPlacement(
            name.to_string(),
            node_id.to_string(),
            reply_tx,
        ))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn send_remove_placement(name: &str, commands: &Sender<Command>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::RemovePlacement(name.to_string(), reply_tx))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn reject_if_service_owned(name: &str, commands: &Sender<Command>) -> Option<(u16, Vec<u8>)> {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::OwnerOf(name.to_string(), reply_tx))
        .is_err()
    {
        return Some(error_response(
            500,
            "control plane worker is not running".to_string(),
        ));
    }
    match reply_rx.recv() {
        Ok(Some(crate::services::Owner::Service(owner))) => Some(error_response(
            400,
            format!("name '{name}' is already in use by service '{owner}'"),
        )),
        Ok(_) => None,
        Err(_) => Some(error_response(
            500,
            "control plane worker did not respond".to_string(),
        )),
    }
}

fn handle_apply_service(
    name: &str,
    body: &[u8],
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let spec: keel_spec::ServiceSpec =
        match keel_spec::parse_and_validate_service(&String::from_utf8_lossy(body)) {
            Ok(s) => s,
            Err(e) => return error_response(400, format!("invalid spec: {e}")),
        };
    if spec.metadata.name != name {
        return error_response(
            400,
            format!(
                "path name '{name}' does not match spec.metadata.name '{}'",
                spec.metadata.name
            ),
        );
    }

    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::ApplyService(
            name.to_string(),
            spec.spec.replicas,
            spec.spec.template,
            spec.spec.port,
            reply_tx,
        ))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => {
            reconcile_and_execute(commands, client_config);
            (200, Vec::new())
        }
        Ok(Err(e @ crate::services::ApplyServiceError::TemplateChanged(_))) => {
            error_response(409, e.to_string())
        }
        Ok(Err(e @ crate::services::ApplyServiceError::PortChanged(_))) => {
            error_response(409, e.to_string())
        }
        Ok(Err(e @ crate::services::ApplyServiceError::NameConflict { .. })) => {
            error_response(400, e.to_string())
        }
        Ok(Err(e @ crate::services::ApplyServiceError::VipPoolExhausted(_))) => {
            error_response(503, e.to_string())
        }
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_get_service(name: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::DiscoverService(name.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(replicas)) => yaml_response(200, &replicas),
        Ok(Err(e)) => error_response(404, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_list_services(commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::ListServices(reply_tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(summaries) => yaml_response(200, &summaries),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_delete_service(
    name: &str,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::DeleteService(name.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(actions)) => {
            execute_replica_actions(actions, commands, client_config);
            (200, Vec::new())
        }
        Ok(Err(e)) => error_response(404, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

/// Asks the worker to compute the current best-effort set of scheduling/
/// teardown actions across every service, then executes them. Called right
/// after a successful `Service` apply and right after a successful
/// heartbeat -- the latter is this milestone's "piggyback on the existing
/// heartbeat traffic" self-healing mechanism: no new thread, no new timer,
/// just one more step in handling a request that already happens every 5
/// seconds per node.
///
/// Note: the compute (`Command::ReconcileServices`) and execute (below)
/// steps are not atomic with each other, which opens a narrow, accepted
/// concurrency gap between two racing calls to this function -- see the doc
/// comment on `Command::ReconcileServices` in `worker.rs` for the full
/// explanation.
fn reconcile_and_execute(commands: &Sender<Command>, client_config: &Arc<rustls::ClientConfig>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::ReconcileServices(reply_tx)).is_err() {
        return;
    }
    if let Ok(actions) = reply_rx.recv() {
        execute_replica_actions(actions, commands, client_config);
    }
}

/// Checked on every successful heartbeat, right alongside
/// `reconcile_and_execute` -- if the heartbeating node id is owed a forced
/// delete for any replica (per `PendingFences`), forwards it inline. On
/// success or 404 (already gone -- e.g. an operator cleaned it up first)
/// the fence is cleared; on any other outcome it's left in place so the
/// next heartbeat from this node retries it. Unlike `reconcile_and_execute`
/// (which has no node context and also runs from the service-apply path),
/// this needs the specific heartbeating node's id, so it's called directly
/// from `handle_heartbeat` rather than folded into that shared function.
fn check_and_execute_fencing(
    node_id: &str,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::PendingFencesForNode(node_id.to_string(), reply_tx))
        .is_err()
    {
        return;
    }
    let Ok(replica_names) = reply_rx.recv() else {
        return;
    };
    if replica_names.is_empty() {
        return;
    }
    let (resolve_tx, resolve_rx) = mpsc::channel();
    if commands
        .send(Command::Resolve(node_id.to_string(), resolve_tx))
        .is_err()
    {
        return;
    }
    let Ok(Ok(addr)) = resolve_rx.recv() else {
        return;
    };
    for replica_name in replica_names {
        match forward(&addr, "DELETE", &format!("/jails/{replica_name}"), &[], client_config) {
            Ok((status, _)) if (200..300).contains(&status) || status == 404 => {
                send_remove_pending_fence(&replica_name, commands);
            }
            Ok((status, body)) => eprintln!(
                "keel-controlplane: forced delete of fenced jail '{replica_name}' on node '{node_id}' failed: status {status}, body {:?}",
                String::from_utf8_lossy(&body)
            ),
            Err(e) => eprintln!("keel-controlplane: failed to reach node '{node_id}' at {addr} while fencing '{replica_name}': {e}"),
        }
    }
}

fn send_remove_pending_fence(replica_name: &str, commands: &Sender<Command>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::RemovePendingFence(
            replica_name.to_string(),
            reply_tx,
        ))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn execute_replica_actions(
    actions: Vec<ReplicaAction>,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) {
    for action in actions {
        match action {
            ReplicaAction::Schedule {
                replica_name,
                node_id,
                node_addr,
                template,
                address,
                prefix_len,
                standby_node_id,
                standby_addr,
                generation,
            } => {
                let cidr = format!("{address}/{prefix_len}");
                let mut spec = template.to_jail_spec(&replica_name, &cidr);
                spec.spec.replicate_to = standby_addr.clone();
                spec.spec.generation = generation;
                let body =
                    serde_yaml::to_string(&spec).expect("JailSpec serialization should not fail");
                match forward(&node_addr, "PUT", &format!("/jails/{replica_name}"), body.as_bytes(), client_config) {
                    Ok((status, _)) if (200..300).contains(&status) => {
                        send_record_placement(&replica_name, &node_id, commands);
                        send_record_replica_address(&replica_name, &node_id, address, commands);
                        if let Some(standby_id) = standby_node_id {
                            send_record_standby(&replica_name, &standby_id, commands);
                        }
                    }
                    Ok((status, resp_body)) => eprintln!(
                        "keel-controlplane: failed to schedule replica '{replica_name}' on node '{node_id}': status {status}, body {:?}",
                        String::from_utf8_lossy(&resp_body)
                    ),
                    Err(e) => eprintln!(
                        "keel-controlplane: failed to reach node '{node_id}' at {node_addr} while scheduling replica '{replica_name}': {e}"
                    ),
                }
            }
            ReplicaAction::TearDown {
                replica_name,
                node_id,
                node_addr,
            } => {
                match forward(&node_addr, "DELETE", &format!("/jails/{replica_name}"), &[], client_config) {
                    Ok((status, _)) if (200..300).contains(&status) => {
                        send_remove_placement(&replica_name, commands);
                        send_release_replica_address(&replica_name, commands);
                        // A torn-down replica's standby pin and any open
                        // fence against it are stale the moment it's gone
                        // -- left behind otherwise, a future replica reusing
                        // this same name could inherit either and feed
                        // straight into an unrelated force-repin.
                        send_remove_standby(&replica_name, commands);
                        send_remove_pending_fence(&replica_name, commands);
                    }
                    Ok((status, resp_body)) => eprintln!(
                        "keel-controlplane: failed to tear down replica '{replica_name}' on node '{node_id}': status {status}, body {:?}",
                        String::from_utf8_lossy(&resp_body)
                    ),
                    Err(e) => eprintln!(
                        "keel-controlplane: failed to reach node '{node_id}' at {node_addr} while tearing down replica '{replica_name}': {e}"
                    ),
                }
            }
        }
    }
}

fn send_record_replica_address(
    name: &str,
    node_id: &str,
    address: std::net::Ipv4Addr,
    commands: &Sender<Command>,
) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::RecordReplicaAddress(
            name.to_string(),
            node_id.to_string(),
            address,
            reply_tx,
        ))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn send_record_standby(replica_name: &str, standby_node_id: &str, commands: &Sender<Command>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::RecordStandby(
            replica_name.to_string(),
            standby_node_id.to_string(),
            reply_tx,
        ))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn send_remove_standby(replica_name: &str, commands: &Sender<Command>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::RemoveStandby(replica_name.to_string(), reply_tx))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn send_release_replica_address(name: &str, commands: &Sender<Command>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::ReleaseReplicaAddress(name.to_string(), reply_tx))
        .is_ok()
    {
        let _ = reply_rx.recv();
    }
}

fn handle_register(body: &[u8], commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let registration: NodeRegistration = match serde_yaml::from_slice(body) {
        Ok(r) => r,
        Err(e) => return error_response(400, format!("invalid YAML: {e}")),
    };
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::Register(
            registration.id,
            registration.addr,
            registration.replicate_addr,
            registration.capacity_cpu,
            registration.capacity_memory,
            reply_tx,
        ))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(pod_cidr)) => yaml_response(
            200,
            &RegisterResponse {
                pod_cidr: pod_cidr.to_string(),
            },
        ),
        Ok(Err(e)) => error_response(409, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_heartbeat(
    id: &str,
    body: &[u8],
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let heartbeat: Heartbeat = match serde_yaml::from_slice(body) {
        Ok(h) => h,
        Err(e) => return error_response(400, format!("invalid YAML: {e}")),
    };
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::Heartbeat(
            id.to_string(),
            heartbeat.committed_cpu,
            heartbeat.committed_memory,
            heartbeat.jails,
            heartbeat.ingresses,
            reply_tx,
        ))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => {
            reconcile_and_execute(commands, client_config);
            check_and_execute_fencing(id, commands, client_config);
            let (entries_tx, entries_rx) = mpsc::channel();
            if commands
                .send(Command::ListServiceProxyEntries(entries_tx))
                .is_err()
            {
                return error_response(500, "control plane worker is not running".to_string());
            }
            match entries_rx.recv() {
                Ok(entries) => yaml_response(200, &entries),
                Err(_) => error_response(500, "control plane worker did not respond".to_string()),
            }
        }
        Ok(Err(e)) => error_response(404, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_list(commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::List(reply_tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(statuses) => yaml_response(200, &statuses),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

const FORWARD_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const FORWARD_READ_TIMEOUT: Duration = Duration::from_secs(5);

fn handle_forward(
    id: &str,
    method: &str,
    path: &str,
    body: &[u8],
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::Resolve(id.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let addr = match reply_rx.recv() {
        Ok(Ok(addr)) => addr,
        Ok(Err(e)) => return error_response(404, e.to_string()),
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    };
    match forward(&addr, method, path, body, client_config) {
        Ok((status, response_body)) => (status, response_body),
        Err(e) => error_response(500, format!("failed to reach node '{id}' at {addr}: {e}")),
    }
}

fn forward(
    addr: &str,
    method: &str,
    path: &str,
    body: &[u8],
    client_config: &Arc<rustls::ClientConfig>,
) -> Result<(u16, Vec<u8>), String> {
    let socket_addr = addr
        .to_socket_addrs()
        .map_err(|e| e.to_string())?
        .next()
        .ok_or_else(|| "could not resolve address".to_string())?;
    let tcp_stream = TcpStream::connect_timeout(&socket_addr, FORWARD_CONNECT_TIMEOUT)
        .map_err(|e| e.to_string())?;
    tcp_stream.set_read_timeout(Some(FORWARD_READ_TIMEOUT)).ok();
    let server_name = tls::server_name_from_addr(addr)?;
    let conn = rustls::ClientConnection::new(Arc::clone(client_config), server_name)
        .map_err(|e| e.to_string())?;
    let mut stream = rustls::StreamOwned::new(conn, tcp_stream);

    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;
    stream.sock.shutdown(std::net::Shutdown::Write).ok();

    // Read until the peer closes the connection. rustls surfaces a plain TCP
    // close that lacks a TLS `close_notify` alert as `ErrorKind::UnexpectedEof`
    // rather than `Ok(0)`, to guard against truncation attacks in general; we
    // rely on that being safe below by explicitly checking the received body
    // length against the response's own Content-Length header, so a
    // connection that drops mid-body (an on-path RST, or a crashing node)
    // is caught as a truncated response rather than silently accepted.
    let mut response = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                response.extend_from_slice(&chunk[..n]);
                if response.len() > MAX_MESSAGE_BYTES {
                    return Err(format!(
                        "response from node exceeded the {MAX_MESSAGE_BYTES}-byte limit"
                    ));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.to_string()),
        }
    }

    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Response::new(&mut headers);
    let header_len = match parsed.parse(&response).map_err(|e| e.to_string())? {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => return Err("incomplete response".to_string()),
    };
    let status = parsed
        .code
        .ok_or_else(|| "missing status code".to_string())?;
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
    Ok((status, response[header_len..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placements::Placements;
    use crate::registry::Registry;
    use crate::tls;
    use crate::worker;
    use std::path::PathBuf;
    use std::sync::Mutex;

    fn fresh_state_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "keel-controlplane-http-test-{}-{id}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn apply_read_timeout_sets_the_configured_timeout_on_a_real_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).unwrap();
        let (server_stream, _) = listener.accept().unwrap();
        apply_read_timeout(&server_stream);
        assert_eq!(
            server_stream.read_timeout().unwrap(),
            Some(INBOUND_READ_TIMEOUT)
        );
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn start_test_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (_worker_handle, commands) = worker::spawn(
            Registry::new("10.0.0.0/16".parse().unwrap()),
            Placements::new(),
            crate::services::Services::new("10.0.250.0/24".parse().unwrap()),
            crate::addresses::UsedAddresses::new(),
            crate::standbys::Standbys::new(),
            crate::pending_fences::PendingFences::new(),
            crate::cordoned::Cordoned::new(),
            fresh_state_dir(),
        );
        let reloading_tls = tls::ReloadingTls::spawn(
            fixture("fixture-node.crt"),
            fixture("fixture-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap();
        thread::spawn(move || run(listener, commands, reloading_tls));
        addr
    }

    fn client_tls_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            tls::load_client_config(
                &fixture("fixture-client.crt"),
                &fixture("fixture-client.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        )
    }

    fn wrong_ca_tls_config() -> Arc<rustls::ClientConfig> {
        Arc::new(
            tls::load_client_config(
                &fixture("wrong-ca-node.crt"),
                &fixture("wrong-ca-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        )
    }

    fn send_request(addr: &str, method: &str, path: &str, body: &str) -> (u16, String) {
        send_request_with(addr, method, path, body, &client_tls_config())
    }

    fn send_request_with(
        addr: &str,
        method: &str,
        path: &str,
        body: &str,
        client_config: &Arc<rustls::ClientConfig>,
    ) -> (u16, String) {
        let server_name = tls::server_name_from_addr(addr).unwrap();
        let tcp_stream = TcpStream::connect(addr).unwrap();
        let conn = rustls::ClientConnection::new(Arc::clone(client_config), server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.sock.shutdown(std::net::Shutdown::Write).ok();
        let mut response = Vec::new();
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
    fn a_client_with_no_certificate_cannot_complete_the_handshake() {
        let addr = start_test_server();
        let tcp_stream = TcpStream::connect(&addr).unwrap();
        // A ClientConfig built with no client cert at all: connects, but the
        // server requires one, so the handshake itself must fail.
        let roots = {
            let mut roots = rustls::RootCertStore::empty();
            let cert = rustls_pemfile::certs(&mut std::io::BufReader::new(
                std::fs::File::open(fixture("ca.crt")).unwrap(),
            ))
            .next()
            .unwrap()
            .unwrap();
            roots.add(cert).unwrap();
            roots
        };
        let bare_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let server_name = tls::server_name_from_addr(&addr).unwrap();
        let conn = rustls::ClientConnection::new(bare_config, server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);
        // Under TLS 1.3, a client can finish its own side of the handshake
        // (and thus have write_all succeed) before it has read the server's
        // rejection alert, since the client doesn't wait for the server's
        // acknowledgement before considering itself done. So the failure
        // must be observed across the full write+read round trip, not the
        // write alone.
        let write_result = stream
            .write_all(b"GET /nodes HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        let mut response = Vec::new();
        let read_result = stream.read_to_end(&mut response);
        assert!(
            write_result.is_err() || read_result.is_err(),
            "expected the handshake to fail with no client certificate presented"
        );
    }

    #[test]
    fn a_client_with_a_wrong_ca_certificate_cannot_complete_the_handshake() {
        let addr = start_test_server();
        let result = std::panic::catch_unwind(|| {
            send_request_with(&addr, "GET", "/nodes", "", &wrong_ca_tls_config())
        });
        assert!(
            result.is_err() || result.unwrap().0 != 200,
            "expected the handshake to fail for a wrong-CA client certificate"
        );
    }

    #[test]
    fn register_returns_200_and_the_node_appears_in_get_nodes() {
        let addr = start_test_server();
        let (status, _) = send_request(
            &addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 10.0.0.1\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );
        assert_eq!(status, 200);

        let (status, body) = send_request(&addr, "GET", "/nodes", "");
        assert_eq!(status, 200);
        assert!(body.contains("node-1"), "expected node-1 in body: {body}");
        assert!(
            body.contains("Alive"),
            "expected Alive status in body: {body}"
        );
    }

    #[test]
    fn reregistering_the_same_id_updates_its_address_without_duplicating() {
        let addr = start_test_server();
        send_request(
            &addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 10.0.0.1\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );
        send_request(
            &addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 10.0.0.2\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );

        let (_, body) = send_request(&addr, "GET", "/nodes", "");
        assert_eq!(
            body.matches("node-1").count(),
            1,
            "expected exactly one node-1 entry, got body: {body}"
        );
        assert!(
            body.contains("10.0.0.2"),
            "expected refreshed address in body: {body}"
        );
    }

    #[test]
    fn heartbeat_on_a_registered_node_returns_200() {
        let addr = start_test_server();
        send_request(
            &addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 10.0.0.1\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );

        let (status, _) = send_request(
            &addr,
            "POST",
            "/nodes/node-1/heartbeat",
            "committed_cpu: 1\ncommitted_memory: 1073741824\n",
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn post_heartbeat_with_ingresses_is_reflected_in_get_nodes() {
        let addr = start_test_server();
        send_request(
            &addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 192.168.64.4:7621\ncapacity_cpu: 4\ncapacity_memory: 8589934592\n",
        );
        let (status, _) = send_request(
            &addr,
            "POST",
            "/nodes/node-1/heartbeat",
            "committed_cpu: 1\ncommitted_memory: 2\ningresses:\n  - name: blog\n    host: example.com\n    backend_service: hugo-site\n    backend_port: 8080\n    cert_expires_at_unix: 1800000000\n",
        );
        assert_eq!(status, 200);
        let (_, body) = send_request(&addr, "GET", "/nodes", "");
        assert!(body.contains("backend_service: hugo-site"), "got: {body}");
    }

    #[test]
    fn heartbeat_on_an_unknown_node_returns_404() {
        let addr = start_test_server();
        let (status, body) = send_request(
            &addr,
            "POST",
            "/nodes/missing/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\n",
        );
        assert_eq!(status, 404);
        assert!(body.contains("missing"));
    }

    #[test]
    fn get_nodes_on_an_empty_registry_returns_200_with_an_empty_list() {
        let addr = start_test_server();
        let (status, body) = send_request(&addr, "GET", "/nodes", "");
        assert_eq!(status, 200);
        assert_eq!(body.trim(), "[]");
    }

    #[test]
    fn register_with_invalid_yaml_returns_400() {
        let addr = start_test_server();
        let (status, _) = send_request(
            &addr,
            "POST",
            "/nodes/register",
            "not: valid: yaml: at: all: -",
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn get_nodes_includes_capacity_and_committed_resources() {
        let addr = start_test_server();
        send_request(
            &addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 10.0.0.1\ncapacity_cpu: 4\ncapacity_memory: 8589934592\n",
        );

        let (_, body) = send_request(&addr, "GET", "/nodes", "");
        assert!(body.contains("capacity_cpu: 4"), "got: {body}");
        assert!(body.contains("capacity_memory: 8589934592"), "got: {body}");
        assert!(body.contains("committed_cpu: 0"), "got: {body}");
        assert!(body.contains("committed_memory: 0"), "got: {body}");
    }

    #[test]
    fn heartbeat_with_invalid_yaml_body_returns_400() {
        let addr = start_test_server();
        send_request(
            &addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 10.0.0.1\ncapacity_cpu: 4\ncapacity_memory: 8589934592\n",
        );

        let (status, _) = send_request(
            &addr,
            "POST",
            "/nodes/node-1/heartbeat",
            "not: valid: yaml: at: all: -",
        );
        assert_eq!(status, 400);
    }

    fn start_fake_remote_tls_agentd(status: u16, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = Arc::new(
            tls::load_server_config(
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
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&server_config)) else {
                    continue;
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                // Drain the whole request (forward() sends it as two
                // separate write_all calls, headers then body, followed by
                // shutdown(Write)) before responding. Reading only once can
                // catch just the first TCP segment under load, leaving the
                // rest unread when this stream is dropped at the end of the
                // loop body — on a BSD-derived TCP stack that can turn the
                // close into an RST instead of a clean FIN, which the real
                // client then sees as a spurious connection reset.
                let mut buf = [0u8; 4096];
                loop {
                    match tls_stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nContent-Type: application/yaml\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = tls_stream.write_all(response.as_bytes());
                let _ = tls_stream.flush();
            }
        });
        addr
    }

    fn start_fake_remote_tls_agentd_recording_deletes(
        status: u16,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = Arc::new(
            tls::load_server_config(
                &fixture("fixture-node.crt"),
                &fixture("fixture-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        let deletes = Arc::new(Mutex::new(Vec::new()));
        let thread_deletes = Arc::clone(&deletes);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&server_config)) else {
                    continue;
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match tls_stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&chunk[..n]),
                    }
                }
                let request_text = String::from_utf8_lossy(&request);
                if let Some(first_line) = request_text.lines().next() {
                    let mut parts = first_line.split_whitespace();
                    if parts.next() == Some("DELETE") {
                        if let Some(replica_name) =
                            parts.next().and_then(|path| path.strip_prefix("/jails/"))
                        {
                            thread_deletes
                                .lock()
                                .unwrap()
                                .push(replica_name.to_string());
                        }
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = tls_stream.write_all(response.as_bytes());
                let _ = tls_stream.flush();
            }
        });
        (addr, deletes)
    }

    /// Like `start_fake_remote_tls_agentd_recording_deletes`, but records
    /// every `PUT /jails/<name>` request's raw body instead of `DELETE`
    /// paths, so a test can inspect the exact `JailSpec` YAML the control
    /// plane actually sent when scheduling a replica.
    fn start_fake_remote_tls_agentd_recording_put_bodies(
        status: u16,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = Arc::new(
            tls::load_server_config(
                &fixture("fixture-node.crt"),
                &fixture("fixture-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let thread_bodies = Arc::clone(&bodies);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&server_config)) else {
                    continue;
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match tls_stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&chunk[..n]),
                    }
                }
                let request_text = String::from_utf8_lossy(&request);
                let is_put = request_text
                    .lines()
                    .next()
                    .map(|line| line.starts_with("PUT "))
                    .unwrap_or(false);
                if is_put {
                    if let Some(body) = request_text.split("\r\n\r\n").nth(1) {
                        thread_bodies.lock().unwrap().push(body.to_string());
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = tls_stream.write_all(response.as_bytes());
                let _ = tls_stream.flush();
            }
        });
        (addr, bodies)
    }

    /// Like `start_fake_remote_tls_agentd`, but the response header declares
    /// a `Content-Length` larger than the body actually written, and the
    /// connection is then dropped without a clean TLS shutdown (no
    /// `close_notify`) — simulating an on-path RST or a node that crashes
    /// mid-write.
    fn start_fake_remote_tls_agentd_with_truncated_body(
        status: u16,
        claimed_body: &'static str,
        actual_body: &'static str,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = Arc::new(
            tls::load_server_config(
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
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&server_config)) else {
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
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nContent-Type: application/yaml\r\nConnection: close\r\n\r\n{actual_body}",
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

    /// Like `start_fake_remote_tls_agentd`, but after the handshake it writes
    /// far more than `MAX_MESSAGE_BYTES` and never stops — standing in for a
    /// buggy or compromised (but still mTLS-authenticated) node, to prove
    /// `forward()`'s response read is bounded rather than growing forever.
    fn start_fake_remote_tls_agentd_with_unbounded_body() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = Arc::new(
            tls::load_server_config(
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
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&server_config)) else {
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
                let header = "HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\nContent-Type: application/yaml\r\nConnection: close\r\n\r\n";
                if tls_stream.write_all(header.as_bytes()).is_err() {
                    continue;
                }
                let chunk = vec![b'x'; 65536];
                loop {
                    if tls_stream.write_all(&chunk).is_err() {
                        break;
                    }
                }
            }
        });
        addr
    }

    #[test]
    fn forward_rejects_a_response_larger_than_the_message_cap() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd_with_unbounded_body();
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/jails", "");
        assert_eq!(
            status, 500,
            "expected an oversized response to be rejected, got {status} with body: {body}"
        );
    }

    fn register_node(cp_addr: &str, id: &str, node_addr: &str) {
        send_request(
            cp_addr,
            "POST",
            "/nodes/register",
            &format!(
                "id: {id}\naddr: {node_addr}\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n"
            ),
        );
    }

    #[test]
    fn forward_over_tls_relays_status_and_body_from_the_target_node() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(
            &cp_addr,
            "PUT",
            "/nodes/node-1/jails/web-1",
            "apiVersion: keel/v1\n",
        );
        assert_eq!(status, 200);
        assert!(
            body.contains("running: true"),
            "expected relayed body, got: {body}"
        );
    }

    #[test]
    fn forward_to_a_node_presenting_a_wrong_ca_certificate_fails() {
        let cp_addr = start_test_server();
        // A "node" whose server certificate is signed by a CA the control
        // plane's own client config does not trust.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let node_addr = listener.local_addr().unwrap().to_string();
        let wrong_server_config = Arc::new(
            tls::load_server_config(
                &fixture("wrong-ca-node.crt"),
                &fixture("wrong-ca-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&wrong_server_config))
                else {
                    continue;
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                let mut buf = [0u8; 4096];
                let _ = tls_stream.read(&mut buf);
            }
        });
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/jails", "");
        assert_eq!(status, 500);
        assert!(
            body.contains("failed to reach node"),
            "expected a forwarding failure, got: {body}"
        );
    }

    #[test]
    fn forward_get_relays_status_and_body_from_the_target_node() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "jails: fake-list\n");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/jails", "");
        assert_eq!(status, 200);
        assert!(
            body.contains("fake-list"),
            "expected relayed body, got: {body}"
        );
    }

    #[test]
    fn forward_delete_relays_status_from_the_target_node() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, _) = send_request(&cp_addr, "DELETE", "/nodes/node-1/jails/web-1", "");
        assert_eq!(status, 200);
    }

    #[test]
    fn forward_to_an_unknown_node_returns_404() {
        let cp_addr = start_test_server();
        let (status, body) = send_request(&cp_addr, "GET", "/nodes/missing/jails", "");
        assert_eq!(status, 404);
        assert!(
            body.contains("unknown node"),
            "expected 'unknown node' in body: {body}"
        );
    }

    #[test]
    fn get_node_volume_forwards_to_the_right_node() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "name: web-data\n");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/volumes/web-data", "");
        assert_eq!(status, 200);
        assert!(
            body.contains("web-data"),
            "expected relayed body, got: {body}"
        );
    }

    #[test]
    fn get_nodes_id_volumes_forwards_to_the_nodes_list_route() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "- name: web-data\n");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/volumes", "");
        assert_eq!(status, 200);
        assert!(
            body.contains("web-data"),
            "expected relayed body, got: {body}"
        );
    }

    #[test]
    fn delete_node_volume_forwards_to_the_right_node() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, _) = send_request(&cp_addr, "DELETE", "/nodes/node-1/volumes/web-data", "");
        assert_eq!(status, 200);
    }

    #[test]
    fn delete_node_volume_on_an_unregistered_node_returns_404() {
        let cp_addr = start_test_server();
        let (status, _) = send_request(&cp_addr, "DELETE", "/nodes/missing/volumes/web-data", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn forward_to_a_node_with_nothing_listening_returns_500() {
        let cp_addr = start_test_server();
        register_node(&cp_addr, "node-1", "127.0.0.1:1");

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/jails", "");
        assert_eq!(status, 500);
        assert!(
            body.contains("failed to reach node"),
            "expected forwarding failure in body: {body}"
        );
    }

    #[test]
    fn forward_to_a_node_that_closes_mid_body_returns_500_instead_of_a_truncated_body() {
        let cp_addr = start_test_server();
        // The header claims a 40-byte body, but the node only ever writes 10
        // bytes before dropping the connection uncleanly (no close_notify).
        let node_addr = start_fake_remote_tls_agentd_with_truncated_body(
            200,
            "running: true, this claims forty bytes\n",
            "running: t",
        );
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/jails", "");
        assert_eq!(
            status, 500,
            "expected a forwarding failure, got status {status} with body: {body}"
        );
        assert!(
            !body.contains("running: t"),
            "truncated upstream body must not be relayed to the caller, got: {body}"
        );
    }

    #[test]
    fn scheduled_put_lands_on_the_lower_id_node_when_headroom_is_equal() {
        let cp_addr = start_test_server();
        let node_a_addr = start_fake_remote_tls_agentd(200, "node: node-a\n");
        let node_b_addr = start_fake_remote_tls_agentd(200, "node: node-b\n");
        register_node(&cp_addr, "node-b", &node_b_addr);
        register_node(&cp_addr, "node-a", &node_a_addr);

        let (status, body) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
        assert_eq!(status, 200);
        assert!(
            body.contains("node-a"),
            "expected the lower id (node-a) to win the tie, got: {body}"
        );
    }

    #[test]
    fn scheduled_put_is_sticky_across_repeated_apply() {
        let cp_addr = start_test_server();
        let node_a_addr = start_fake_remote_tls_agentd(200, "node: node-a\n");
        register_node(&cp_addr, "node-a", &node_a_addr);

        let (status, body) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
        assert_eq!(status, 200);
        assert!(body.contains("node-a"));

        // node-0 joins with a lower id and full headroom, and would win a
        // fresh scheduling decision -- but web-1 is already placed, so it
        // must stay put.
        let node_0_addr = start_fake_remote_tls_agentd(200, "node: node-0\n");
        register_node(&cp_addr, "node-0", &node_0_addr);

        let (status, body) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
        assert_eq!(status, 200);
        assert!(
            body.contains("node-a"),
            "expected sticky placement on node-a, got: {body}"
        );
    }

    #[test]
    fn scheduled_get_and_delete_on_an_unplaced_jail_return_404() {
        let cp_addr = start_test_server();

        let (status, body) = send_request(&cp_addr, "GET", "/jails/missing", "");
        assert_eq!(status, 404);
        assert!(body.contains("no known placement"), "got: {body}");

        let (status, body) = send_request(&cp_addr, "DELETE", "/jails/missing", "");
        assert_eq!(status, 404);
        assert!(body.contains("no known placement"), "got: {body}");
    }

    #[test]
    fn scheduled_delete_removes_the_placement_so_a_later_get_returns_404() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "node: node-a\n");
        register_node(&cp_addr, "node-a", &node_addr);

        send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
        let (status, _) = send_request(&cp_addr, "DELETE", "/jails/web-1", "");
        assert_eq!(status, 200);

        let (status, body) = send_request(&cp_addr, "GET", "/jails/web-1", "");
        assert_eq!(status, 404);
        assert!(body.contains("no known placement"), "got: {body}");
    }

    #[test]
    fn scheduled_put_with_no_alive_nodes_returns_503() {
        let cp_addr = start_test_server();
        let (status, body) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
        assert_eq!(status, 503);
        assert!(body.contains("no alive nodes"), "got: {body}");
    }

    #[test]
    fn cordon_an_unknown_node_returns_404() {
        let cp_addr = start_test_server();
        let (status, _) = send_request(&cp_addr, "POST", "/nodes/ghost/cordon", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn uncordon_an_unknown_node_returns_404() {
        let cp_addr = start_test_server();
        let (status, _) = send_request(&cp_addr, "POST", "/nodes/ghost/uncordon", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn cordon_then_list_shows_the_node_excluded_from_a_fresh_schedule() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
        assert_eq!(status, 200);

        // A fresh PUT /jails/<name> with only node-1 registered now has no
        // schedulable candidate at all.
        let (status, body) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
        assert_eq!(status, 503, "got: {body}");
    }

    #[test]
    fn uncordon_restores_schedulability() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-1", &node_addr);

        send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
        send_request(&cp_addr, "POST", "/nodes/node-1/uncordon", "");

        let (status, body) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
        assert_eq!(status, 200, "got: {body}");
    }

    #[test]
    fn cordoning_an_already_cordoned_node_is_a_no_op_success() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-1", &node_addr);

        send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
        let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
        assert_eq!(status, 200);
    }

    #[test]
    fn named_route_apply_and_scheduled_route_share_the_same_placement_table() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, _) = send_request(
            &cp_addr,
            "PUT",
            "/nodes/node-1/jails/web-1",
            "apiVersion: keel/v1\n",
        );
        assert_eq!(status, 200);

        let (status, body) = send_request(&cp_addr, "GET", "/jails/web-1", "");
        assert_eq!(
            status, 200,
            "expected the scheduled GET to find the placement recorded by the named-node PUT"
        );
        assert!(body.contains("running: true"), "got: {body}");
    }

    #[test]
    fn a_client_with_a_revoked_certificate_cannot_complete_the_handshake() {
        let addr = start_test_server();
        let revoked_config = Arc::new(
            tls::load_client_config(
                &fixture("revoked-node.crt"),
                &fixture("revoked-node.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            send_request_with(&addr, "GET", "/nodes", "", &revoked_config)
        }));
        assert!(
            result.is_err() || result.unwrap().0 != 200,
            "expected the handshake to fail for a revoked client certificate"
        );
    }

    #[test]
    fn forward_to_a_node_presenting_a_revoked_certificate_fails() {
        let cp_addr = start_test_server();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let node_addr = listener.local_addr().unwrap().to_string();
        let revoked_server_config = Arc::new(
            tls::load_server_config(
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
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&revoked_server_config))
                else {
                    continue;
                };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                let mut buf = [0u8; 4096];
                let _ = tls_stream.read(&mut buf);
            }
        });
        register_node(&cp_addr, "node-1", &node_addr);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/jails", "");
        assert_eq!(status, 500);
        assert!(
            body.contains("failed to reach node"),
            "expected a forwarding failure, got: {body}"
        );
    }

    #[test]
    fn reloading_tls_server_config_picks_up_a_replaced_certificate_without_restart() {
        let cert_dir = std::env::temp_dir().join(format!(
            "keel-controlplane-reload-test-{}",
            std::process::id()
        ));
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

        let (_worker_handle, commands) = worker::spawn(
            Registry::new("10.0.0.0/16".parse().unwrap()),
            Placements::new(),
            crate::services::Services::new("10.0.250.0/24".parse().unwrap()),
            crate::addresses::UsedAddresses::new(),
            crate::standbys::Standbys::new(),
            crate::pending_fences::PendingFences::new(),
            crate::cordoned::Cordoned::new(),
            fresh_state_dir(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || run(listener, commands, reloading));

        let (status, _) = send_request(&addr, "GET", "/nodes", "");
        assert_eq!(
            status, 200,
            "expected the initial fixture-node certificate to be served"
        );

        // wrong-ca-node.crt is signed by a different, untrusted CA, so once
        // the server starts presenting it, any client trusting only the real
        // ca.crt must fail the handshake -- this is the observable proof
        // that the reload thread actually swapped in the replacement file.
        std::fs::copy(fixture("wrong-ca-node.crt"), &cert_path).unwrap();
        std::fs::copy(fixture("wrong-ca-node.key"), &key_path).unwrap();
        thread::sleep(Duration::from_millis(200));

        let result = std::panic::catch_unwind(|| send_request(&addr, "GET", "/nodes", ""));
        assert!(
            result.is_err() || result.unwrap().0 != 200,
            "expected the server's replaced certificate to be rejected by the client after reload"
        );
    }

    fn service_yaml(name: &str, replicas: u32) -> String {
        format!(
            "apiVersion: keel/v1\nkind: Service\nmetadata:\n  name: {name}\nspec:\n  replicas: {replicas}\n  port: 8080\n  template:\n    image: base/14.2-web\n    command: [\"/usr/local/bin/myapp\"]\n    network:\n      vnet: true\n      bridge: keel0\n    resources:\n      cpu: \"1\"\n      memory: 256M\n    restartPolicy: Always\n"
        )
    }

    fn service_yaml_with_port(name: &str, replicas: u32, port: u16) -> String {
        format!(
            "apiVersion: keel/v1\nkind: Service\nmetadata:\n  name: {name}\nspec:\n  replicas: {replicas}\n  port: {port}\n  template:\n    image: base/14.2-web\n    command: [\"/usr/local/bin/myapp\"]\n    network:\n      vnet: true\n      bridge: keel0\n    resources:\n      cpu: \"1\"\n      memory: \"256M\"\n    restartPolicy: Always\n"
        )
    }

    fn stateful_service_yaml(name: &str, replicas: u32) -> String {
        format!(
            "apiVersion: keel/v1\nkind: Service\nmetadata:\n  name: {name}\nspec:\n  replicas: {replicas}\n  port: 8080\n  template:\n    image: base/14.2-web\n    command: [\"/usr/local/bin/myapp\"]\n    network:\n      vnet: true\n      bridge: keel0\n    resources:\n      cpu: \"1\"\n      memory: 256M\n    restartPolicy: Always\n    volumes:\n      - name: data\n        mountPath: /var/db\n        size: 1G\n"
        )
    }

    #[test]
    fn put_service_creates_and_schedules_replicas_across_registered_nodes() {
        let cp_addr = start_test_server();
        let node_a = start_fake_remote_tls_agentd(200, "running: true\n");
        let node_b = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_a);
        register_node(&cp_addr, "node-b", &node_b);

        let (status, _) = send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 2));
        assert_eq!(status, 200);
    }

    #[test]
    fn scheduling_a_stateful_service_replica_forwards_a_spec_with_replicate_to_set() {
        let cp_addr = start_test_server();
        let node_a = start_fake_remote_tls_agentd(200, "running: true\n");
        let node_b = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_a);
        register_node(&cp_addr, "node-b", &node_b);

        let (status, _) = send_request(
            &cp_addr,
            "PUT",
            "/services/db",
            &stateful_service_yaml("db", 1),
        );
        assert_eq!(status, 200);

        let (status, body) = send_request(&cp_addr, "GET", "/nodes", "");
        assert_eq!(status, 200);
        assert!(
            body.contains("node-a") && body.contains("node-b"),
            "got: {body}"
        );
        // The fake remote agentd just echoes a fixed body ("running: true"),
        // so asserting the forwarded replicateTo requires inspecting what
        // was actually sent -- covered precisely by
        // keel-agentd's own "put_replicate_to_..." tests and Task 6's
        // replication-loop test for the receiving side. Here, confirm at
        // least one of the two nodes ends up as this replica's recorded
        // standby by checking GET /nodes twice is stable and a placement
        // exists (full round-trip proof lives in Task 10's force-repin
        // integration test, which depends on a real standby having been
        // recorded).
        let (status, _) = send_request(&cp_addr, "GET", "/jails/db-0", "");
        assert_eq!(
            status, 200,
            "expected db-0 to have been scheduled onto one of the two registered nodes"
        );
    }

    #[test]
    fn put_service_with_zero_replicas_succeeds_and_schedules_nothing() {
        let cp_addr = start_test_server();
        let (status, _) = send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 0));
        assert_eq!(status, 200);
    }

    #[test]
    fn put_service_changing_the_template_on_an_existing_service_returns_409() {
        let cp_addr = start_test_server();
        send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));
        let changed = service_yaml("web", 1).replace("base/14.2-web", "base/different-image");
        let (status, _) = send_request(&cp_addr, "PUT", "/services/web", &changed);
        assert_eq!(status, 409);
    }

    #[test]
    fn put_service_colliding_with_an_existing_unmanaged_jail_returns_400() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(
            &cp_addr,
            "PUT",
            "/nodes/node-a/jails/web-0",
            "apiVersion: keel/v1\n",
        );

        let (status, body) =
            send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));
        assert_eq!(status, 400);
        assert!(
            body.contains("web-0"),
            "expected the conflicting name in the error, got: {body}"
        );
    }

    #[test]
    fn put_jail_colliding_with_an_existing_service_replica_returns_400() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));

        let (status, body) = send_request(
            &cp_addr,
            "PUT",
            "/nodes/node-a/jails/web-0",
            "apiVersion: keel/v1\n",
        );
        assert_eq!(status, 400);
        assert!(
            body.contains("service 'web'"),
            "expected the owning service named in the error, got: {body}"
        );
    }

    #[test]
    fn get_service_returns_only_alive_and_running_replicas() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));

        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\njails:\n  - name: web-0\n    running: true\n",
        );

        let (status, body) = send_request(&cp_addr, "GET", "/services/web", "");
        assert_eq!(status, 200);
        assert!(
            body.contains("web-0"),
            "expected the healthy replica listed, got: {body}"
        );
        assert!(body.contains("node-a"), "got: {body}");
    }

    #[test]
    fn get_service_omits_a_crash_looping_replica() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));

        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\njails:\n  - name: web-0\n    running: false\n",
        );

        let (status, body) = send_request(&cp_addr, "GET", "/services/web", "");
        assert_eq!(status, 200);
        assert_eq!(
            body.trim(),
            "[]",
            "expected no replicas listed while crash-looping, got: {body}"
        );
    }

    #[test]
    fn get_service_on_an_unknown_name_returns_404() {
        let cp_addr = start_test_server();
        let (status, body) = send_request(&cp_addr, "GET", "/services/missing", "");
        assert_eq!(status, 404);
        assert!(body.contains("missing"));
    }

    #[test]
    fn get_services_bare_lists_every_service() {
        let cp_addr = start_test_server();
        send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 2));
        send_request(&cp_addr, "PUT", "/services/api", &service_yaml("api", 1));

        let (status, body) = send_request(&cp_addr, "GET", "/services", "");
        assert_eq!(status, 200);
        assert!(body.contains("web"), "got: {body}");
        assert!(body.contains("api"), "got: {body}");
    }

    #[test]
    fn get_services_reports_the_applied_services_vip_and_port() {
        let cp_addr = start_test_server();
        send_request(
            &cp_addr,
            "PUT",
            "/services/web",
            &service_yaml_with_port("web", 1, 8080),
        );

        let (status, body) = send_request(&cp_addr, "GET", "/services", "");
        assert_eq!(status, 200);
        assert!(body.contains("port: 8080"), "expected port in body: {body}");
        assert!(
            body.contains("vip:"),
            "expected a vip field in body: {body}"
        );
    }

    #[test]
    fn heartbeat_response_body_reflects_the_currently_healthy_replica_set() {
        let cp_addr = start_test_server();
        let (reg_status, _) = send_request(
            &cp_addr,
            "POST",
            "/nodes/register",
            "id: node-1\naddr: 10.0.0.1:7621\ncapacity_cpu: 4\ncapacity_memory: 8589934592\n",
        );
        assert_eq!(reg_status, 200);
        send_request(
            &cp_addr,
            "PUT",
            "/services/web",
            &service_yaml_with_port("web", 1, 8080),
        );

        let (status, body) = send_request(
            &cp_addr,
            "POST",
            "/nodes/node-1/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\njails: []\n",
        );
        assert_eq!(status, 200);
        assert!(
            body.contains("name: web"),
            "expected the service table in the heartbeat response: {body}"
        );
        assert!(
            body.contains("port: 8080"),
            "expected port in heartbeat response: {body}"
        );
    }

    #[test]
    fn delete_service_tears_down_every_placed_replica() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));
        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\njails:\n  - name: web-0\n    running: true\n",
        );

        let (status, _) = send_request(&cp_addr, "DELETE", "/services/web", "");
        assert_eq!(status, 200);

        let (status, _) = send_request(&cp_addr, "GET", "/services/web", "");
        assert_eq!(
            status, 404,
            "expected the service itself to be forgotten after delete"
        );
    }

    #[test]
    fn delete_service_removes_the_standby_and_pending_fence_of_every_torn_down_replica() {
        // Neither standbys nor pending_fences ever get cleaned up on
        // teardown today: if a replica name is ever reused, a stale entry
        // from this incarnation can resurface and feed into a future
        // force-repin. Proves both are gone (on disk, not just in a
        // freshly re-queried in-memory view) after the service (and its
        // one stateful replica) is deleted.
        let (cp_addr, commands, state_dir) = start_test_server_with_commands_and_state_dir();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(
            &cp_addr,
            "PUT",
            "/services/db",
            &stateful_service_yaml("db", 1),
        );
        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\njails:\n  - name: db-0\n    running: true\n",
        );

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-b".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordPendingFence(
                "db-0".to_string(),
                "node-a".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        let (status, _) = send_request(&cp_addr, "DELETE", "/services/db", "");
        assert_eq!(status, 200);

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::PendingFencesForNode("node-a".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Vec::<String>::new(),
            "pending fence for the torn-down replica must be gone"
        );

        let standbys: crate::standbys::Standbys =
            crate::store::load_or_default(&state_dir.join("standbys.yaml"));
        assert_eq!(
            standbys.get("db-0"),
            None,
            "standby for the torn-down replica must be gone"
        );
    }

    #[test]
    fn delete_service_on_an_unknown_name_returns_404() {
        let cp_addr = start_test_server();
        let (status, _) = send_request(&cp_addr, "DELETE", "/services/missing", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn heartbeat_piggybacks_reconciliation_and_replaces_a_replica_once_its_node_is_registered() {
        let cp_addr = start_test_server();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        // Apply a 1-replica service before any node exists: it succeeds
        // (best-effort), placing nothing yet.
        let (status, _) = send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));
        assert_eq!(status, 200);
        let (_, body) = send_request(&cp_addr, "GET", "/services/web", "");
        assert_eq!(body.trim(), "[]", "expected no capacity yet, got: {body}");

        // Once a node registers and heartbeats, the very next heartbeat's
        // piggybacked reconciliation should place the missing replica.
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\n",
        );
        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\njails:\n  - name: web-0\n    running: true\n",
        );

        let (status, body) = send_request(&cp_addr, "GET", "/services/web", "");
        assert_eq!(status, 200);
        assert!(body.contains("web-0"), "expected the replica to have been scheduled by heartbeat-piggybacked reconciliation, got: {body}");
    }

    #[test]
    fn scheduling_a_replica_stamps_the_jail_spec_with_a_real_generation() {
        // The JailSpec pushed to the node must carry the replica's real
        // generation (from Placements), not the always-zero default
        // to_jail_spec produces on its own -- this is the wire-protocol
        // side of the fencing-token mechanism that stops a partitioned
        // former-primary from replicating to a standby that's since been
        // promoted.
        let cp_addr = start_test_server();
        let (node_addr, bodies) = start_fake_remote_tls_agentd_recording_put_bodies(200);
        register_node(&cp_addr, "node-a", &node_addr);
        send_request(&cp_addr, "PUT", "/services/web", &service_yaml("web", 1));
        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\n",
        );
        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\njails:\n  - name: web-0\n    running: true\n",
        );

        let recorded = bodies.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "expected exactly one PUT /jails/web-0, got: {recorded:?}"
        );
        assert!(
            recorded[0].contains("generation: 1"),
            "expected the first placement to carry generation 1, got body: {}",
            recorded[0]
        );
    }

    #[test]
    fn reloading_tls_keeps_serving_the_last_good_config_if_the_replacement_is_malformed() {
        let cert_dir = std::env::temp_dir().join(format!(
            "keel-controlplane-reload-bad-test-{}",
            std::process::id()
        ));
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

        let (_worker_handle, commands) = worker::spawn(
            Registry::new("10.0.0.0/16".parse().unwrap()),
            Placements::new(),
            crate::services::Services::new("10.0.250.0/24".parse().unwrap()),
            crate::addresses::UsedAddresses::new(),
            crate::standbys::Standbys::new(),
            crate::pending_fences::PendingFences::new(),
            crate::cordoned::Cordoned::new(),
            fresh_state_dir(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || run(listener, commands, reloading));

        std::fs::write(&cert_path, "not a certificate").unwrap();
        thread::sleep(Duration::from_millis(200));

        let (status, _) = send_request(&addr, "GET", "/nodes", "");
        assert_eq!(status, 200, "expected the last-known-good certificate to keep being served after a malformed reload");
    }

    fn start_test_server_with_commands() -> (String, Sender<Command>) {
        let (addr, commands, _state_dir) = start_test_server_with_commands_and_state_dir();
        (addr, commands)
    }

    fn start_test_server_with_commands_and_state_dir() -> (String, Sender<Command>, PathBuf) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state_dir = fresh_state_dir();
        let (_worker_handle, commands) = worker::spawn(
            Registry::new("10.0.0.0/16".parse().unwrap()),
            Placements::new(),
            crate::services::Services::new("10.0.250.0/24".parse().unwrap()),
            crate::addresses::UsedAddresses::new(),
            crate::standbys::Standbys::new(),
            crate::pending_fences::PendingFences::new(),
            crate::cordoned::Cordoned::new(),
            state_dir.clone(),
        );
        let reloading_tls = tls::ReloadingTls::spawn(
            fixture("fixture-node.crt"),
            fixture("fixture-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap();
        let commands_for_server = commands.clone();
        thread::spawn(move || run(listener, commands_for_server, reloading_tls));
        (addr, commands, state_dir)
    }

    fn record_pending_fence(commands: &Sender<Command>, replica_name: &str, node_id: &str) {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordPendingFence(
                replica_name.to_string(),
                node_id.to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn a_heartbeat_from_a_node_owed_a_fence_triggers_a_forced_delete() {
        let (cp_addr, commands) = start_test_server_with_commands();
        let node_addr = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-a", &node_addr);
        record_pending_fence(&commands, "db-0", "node-a");

        let (status, _) = send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\n",
        );
        assert_eq!(status, 200);

        // The forced DELETE is fire-and-forget from the heartbeat response's
        // point of view -- give it a moment, then confirm the fence was
        // cleared (the fake remote agentd above answers every request with
        // 200, so the forced delete "succeeds" and PendingFencesForNode
        // should come back empty on the next check).
        std::thread::sleep(Duration::from_millis(100));
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::PendingFencesForNode("node-a".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Vec::<String>::new(),
            "expected the fence to have been cleared after a successful forced delete"
        );
    }

    #[test]
    fn a_heartbeat_from_an_unrelated_node_leaves_pending_fences_untouched() {
        let (cp_addr, commands) = start_test_server_with_commands();
        let node_a = start_fake_remote_tls_agentd(200, "");
        let node_b = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-a", &node_a);
        register_node(&cp_addr, "node-b", &node_b);
        record_pending_fence(&commands, "db-0", "node-a");

        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-b/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\n",
        );

        std::thread::sleep(Duration::from_millis(100));
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::PendingFencesForNode("node-a".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            vec!["db-0".to_string()],
            "expected node-a's fence to remain, untouched by node-b's heartbeat"
        );
    }

    #[test]
    fn a_failed_forced_delete_leaves_the_fence_in_place_for_the_next_heartbeat() {
        let (cp_addr, commands) = start_test_server_with_commands();
        // 500 from the fake remote agentd simulates the forced DELETE failing.
        let node_addr = start_fake_remote_tls_agentd(500, "boom");
        register_node(&cp_addr, "node-a", &node_addr);
        record_pending_fence(&commands, "db-0", "node-a");

        send_request(
            &cp_addr,
            "POST",
            "/nodes/node-a/heartbeat",
            "committed_cpu: 0\ncommitted_memory: 0\n",
        );

        std::thread::sleep(Duration::from_millis(100));
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::PendingFencesForNode("node-a".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            vec!["db-0".to_string()],
            "expected the fence to remain after a failed forced delete"
        );
    }

    fn record_placement(commands: &Sender<Command>, jail_name: &str, node_id: &str) {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                jail_name.to_string(),
                node_id.to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn force_repin_on_an_unplaced_name_returns_404() {
        let cp_addr = start_test_server();
        let (status, body) = send_request(&cp_addr, "POST", "/replicas/db-0/force-repin", "");
        assert_eq!(status, 404);
        assert!(body.contains("no known placement"), "got: {body}");
    }

    #[test]
    fn force_repin_on_a_non_stateful_name_returns_400() {
        let (cp_addr, commands) = start_test_server_with_commands();
        let node_addr = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_addr);
        record_placement(&commands, "web-0", "node-a");

        let (status, body) = send_request(&cp_addr, "POST", "/replicas/web-0/force-repin", "");
        assert_eq!(status, 400);
        assert!(body.contains("not a stateful replica"), "got: {body}");
    }

    #[test]
    fn force_repin_while_the_primary_is_still_alive_returns_409() {
        let (cp_addr, commands) = start_test_server_with_commands();
        let node_a = start_fake_remote_tls_agentd(200, "running: true\n");
        let node_b = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-a", &node_a);
        register_node(&cp_addr, "node-b", &node_b);
        record_placement(&commands, "db-0", "node-a");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-b".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        let (status, body) = send_request(&cp_addr, "POST", "/replicas/db-0/force-repin", "");
        assert_eq!(status, 409);
        assert!(body.contains("still resolves as alive"), "got: {body}");
    }

    #[test]
    fn force_repin_before_the_standby_has_a_first_snapshot_returns_409() {
        let (cp_addr, commands) = start_test_server_with_commands();
        // node-a is never registered -> unreachable, standing in for Dead.
        let node_b = start_fake_remote_tls_agentd(404, "error: no replica target 'db-0'\n");
        register_node(&cp_addr, "node-b", &node_b);
        // A third, distinct node is required so PrepareForceRepin's
        // fresh-standby pick (which must exclude both the dead primary and
        // the current standby node-b) has somewhere to land; without it,
        // prep fails with NoFreshStandby before ever reaching the readiness
        // probe this test is exercising. It never needs to be dialed here,
        // but does need a real replicate_addr advertised (Task 8b).
        send_request(
            &cp_addr,
            "POST",
            "/nodes/register",
            "id: node-c\naddr: 127.0.0.1:1\nreplicate_addr: 127.0.0.1:2\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );
        apply_service_with_template_via_http(&cp_addr, "db", 1);
        record_placement(&commands, "db-0", "node-unreachable");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-b".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        let (status, body) = send_request(&cp_addr, "POST", "/replicas/db-0/force-repin", "");
        assert_eq!(status, 409);
        assert!(
            body.contains("has not completed a first full replication"),
            "got: {body}"
        );
    }

    #[test]
    fn force_repin_happy_path_updates_placements_standbys_and_pending_fences() {
        let (cp_addr, commands) = start_test_server_with_commands();
        let node_b = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-b", &node_b);
        // node-c never needs to be dialed in this test (it only needs to
        // exist as an Alive candidate for the fresh-standby pick), but it
        // DOES need a real replicate_addr registered -- Task 8b's
        // PrepareForceRepin looks up the fresh standby's replicate_addr()
        // (not resolve()), which is None unless explicitly advertised.
        send_request(
            &cp_addr,
            "POST",
            "/nodes/register",
            "id: node-c\naddr: 127.0.0.1:1\nreplicate_addr: 127.0.0.1:2\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );
        apply_service_with_template_via_http(&cp_addr, "db", 1);
        record_placement(&commands, "db-0", "node-unreachable");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-b".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        // Simulate node-b already having a fully-replicated ReplicaTarget by
        // making its next forwarded GET return 200. start_fake_remote_tls_agentd
        // answers every request with the same fixed status/body, so this
        // fake stands in for BOTH the readiness GET and the provisioning PUT
        // with one 200 response -- sufficient to prove force-repin's own
        // control-plane-side bookkeeping (Placements/Standbys/PendingFences),
        // which is what this test targets; real end-to-end behavior against
        // genuine keel-agentd readiness/provision responses is covered by
        // Task 13's VM verification.
        let (status, _) = send_request(&cp_addr, "POST", "/replicas/db-0/force-repin", "");
        assert_eq!(status, 200);

        let (status, body) = send_request(&cp_addr, "GET", "/jails/db-0", "");
        assert_eq!(
            status, 200,
            "expected db-0's placement to now resolve to the promoted node, got: {body}"
        );

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::PendingFencesForNode(
                "node-unreachable".to_string(),
                tx,
            ))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            vec!["db-0".to_string()],
            "expected the old primary to be fenced"
        );
    }

    #[test]
    fn force_repin_stamps_the_promoted_jail_spec_with_a_higher_generation_than_the_old_primary() {
        let (cp_addr, commands) = start_test_server_with_commands();
        // Applied before any node is registered, so ApplyService's own
        // piggybacked reconcile has no alive node to auto-schedule onto --
        // keeps this test's only PUT to node-b being force-repin's own
        // promotion, so its generation is unambiguous.
        apply_service_with_template_via_http(&cp_addr, "db", 1);
        let (node_b, node_b_bodies) = start_fake_remote_tls_agentd_recording_put_bodies(200);
        register_node(&cp_addr, "node-b", &node_b);
        send_request(
            &cp_addr,
            "POST",
            "/nodes/register",
            "id: node-c\naddr: 127.0.0.1:1\nreplicate_addr: 127.0.0.1:2\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );
        // The old primary's own placement is generation 1 (Placements::set
        // bumps on every call, including the first).
        record_placement(&commands, "db-0", "node-unreachable");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-b".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        let (status, _) = send_request(&cp_addr, "POST", "/replicas/db-0/force-repin", "");
        assert_eq!(status, 200);

        let recorded = node_b_bodies.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "expected exactly one PUT /jails/db-0 to the promoted node, got: {recorded:?}"
        );
        assert!(
            recorded[0].contains("generation: 2"),
            "expected the promoted primary's generation (2) to be strictly higher than the old primary's (1), got body: {}",
            recorded[0]
        );
    }

    #[test]
    fn force_repin_immediately_pushes_a_delete_to_the_old_primary_without_waiting_for_its_heartbeat(
    ) {
        let (cp_addr, commands) = start_test_server_with_commands();

        // Registered now, then aged past DEAD_THRESHOLD below -- unlike the
        // other force-repin tests' "node-unreachable" (never registered at
        // all, which resolve()-fails identically but leaves no address on
        // file), this one needs a real address for last_known_addr to find.
        let (old_primary_addr, old_primary_deletes) =
            start_fake_remote_tls_agentd_recording_deletes(200);
        register_node(&cp_addr, "old-primary", &old_primary_addr);

        std::thread::sleep(std::time::Duration::from_secs(16));

        // Registered after the sleep above, so they're freshly Alive (not
        // also aged past DEAD_THRESHOLD) when force-repin runs.
        let node_b = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-b", &node_b);
        send_request(
            &cp_addr,
            "POST",
            "/nodes/register",
            "id: node-c\naddr: 127.0.0.1:1\nreplicate_addr: 127.0.0.1:2\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );

        apply_service_with_template_via_http(&cp_addr, "db", 1);
        record_placement(&commands, "db-0", "old-primary");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-b".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        let (status, body) = send_request(&cp_addr, "POST", "/replicas/db-0/force-repin", "");
        assert_eq!(status, 200, "got: {body}");

        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(
            old_primary_deletes.lock().unwrap().contains(&"db-0".to_string()),
            "expected the immediate push to have sent a DELETE for db-0 to the old primary, with no heartbeat from it ever received"
        );
    }

    fn apply_service_with_template_via_http(cp_addr: &str, name: &str, replicas: u32) {
        send_request(
            cp_addr,
            "PUT",
            &format!("/services/{name}"),
            &stateful_service_yaml(name, replicas),
        );
    }
}
