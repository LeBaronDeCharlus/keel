use keel_agentd::ErrorBody;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::ExitCode;

mod tls;

const DEFAULT_SOCKET: &str = "/var/run/keel-agentd.sock";

#[derive(Debug, PartialEq)]
enum Target {
    Socket(PathBuf),
    ControlPlane {
        addr: String,
        node: Option<String>,
        tls_ca_file: PathBuf,
        tls_cert_file: PathBuf,
        tls_key_file: PathBuf,
        tls_crl_file: PathBuf,
    },
}

fn main() -> ExitCode {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let socket = extract_socket_flag(&mut args).unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET));
    let control_plane_addr = extract_flag(&mut args, "--control-plane-addr");
    let node = extract_flag(&mut args, "--node");
    let tls_ca_file = extract_flag(&mut args, "--tls-ca-file");
    let tls_cert_file = extract_flag(&mut args, "--tls-cert-file");
    let tls_key_file = extract_flag(&mut args, "--tls-key-file");
    let tls_crl_file = extract_flag(&mut args, "--tls-crl-file");

    let target = match resolve_target(
        socket,
        control_plane_addr,
        node,
        tls_ca_file,
        tls_cert_file,
        tls_key_file,
        tls_crl_file,
    ) {
        Ok(target) => target,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::FAILURE;
        }
    };

    let result = match args.split_first() {
        Some((cmd, rest)) if cmd == "apply" => run_apply(&target, rest),
        Some((cmd, rest)) if cmd == "get" => run_get(&target, rest),
        Some((cmd, rest)) if cmd == "delete" => run_delete(&target, rest),
        Some((cmd, rest)) if cmd == "delete-volume" => run_delete_volume(&target, rest),
        Some((cmd, rest)) if cmd == "force-repin" => run_force_repin(&target, rest),
        _ => {
            eprintln!(
                "usage: keelctl <apply -f FILE|get [name]|delete NAME|delete-volume NAME|force-repin NAME> [--socket PATH|--control-plane-addr ADDR --node ID]"
            );
            return ExitCode::FAILURE;
        }
    };

    match result {
        Ok(output) => {
            print!("{output}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn extract_flag(args: &mut Vec<String>, name: &str) -> Option<String> {
    let index = args.iter().position(|a| a == name)?;
    args.remove(index);
    Some(args.remove(index))
}

fn extract_socket_flag(args: &mut Vec<String>) -> Option<PathBuf> {
    extract_flag(args, "--socket").map(PathBuf::from)
}

fn resolve_target(
    socket: PathBuf,
    control_plane_addr: Option<String>,
    node: Option<String>,
    tls_ca_file: Option<String>,
    tls_cert_file: Option<String>,
    tls_key_file: Option<String>,
    tls_crl_file: Option<String>,
) -> Result<Target, String> {
    match (control_plane_addr, node, tls_ca_file, tls_cert_file, tls_key_file, tls_crl_file) {
        (Some(addr), node, Some(ca), Some(cert), Some(key), Some(crl)) => Ok(Target::ControlPlane {
            addr,
            node,
            tls_ca_file: PathBuf::from(ca),
            tls_cert_file: PathBuf::from(cert),
            tls_key_file: PathBuf::from(key),
            tls_crl_file: PathBuf::from(crl),
        }),
        (Some(_), _, _, _, _, _) => Err(
            "--tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required with --control-plane-addr"
                .to_string(),
        ),
        (None, Some(_), _, _, _, _) => Err("--node requires --control-plane-addr".to_string()),
        (None, None, _, _, _, _) => Ok(Target::Socket(socket)),
    }
}

fn jails_path(target: &Target, suffix: &str) -> String {
    match target {
        Target::Socket(_) => suffix.to_string(),
        Target::ControlPlane { node: Some(node), .. } => format!("/nodes/{node}{suffix}"),
        Target::ControlPlane { node: None, .. } => suffix.to_string(),
    }
}

fn dispatch(target: &Target, method: &str, path: &str, body: &str) -> Result<(u16, String), String> {
    match target {
        Target::Socket(socket) => send_request(socket, method, path, body),
        Target::ControlPlane { addr, tls_ca_file, tls_cert_file, tls_key_file, tls_crl_file, .. } => {
            send_request_tcp(addr, method, path, body, tls_ca_file, tls_cert_file, tls_key_file, tls_crl_file)
        }
    }
}

/// Converts a raw `(status, body)` pair into the old collapsed
/// `Result<String, String>` shape: 2xx becomes `Ok(body)`, anything else
/// becomes `Err` of the parsed `ErrorBody`'s message (or the raw body, if
/// it isn't one).
fn success_body(result: Result<(u16, String), String>) -> Result<String, String> {
    let (status, body) = result?;
    if (200..300).contains(&status) {
        Ok(body)
    } else {
        let error: ErrorBody = serde_yaml::from_str(&body).unwrap_or(ErrorBody { error: body });
        Err(error.error)
    }
}

fn run_apply(target: &Target, args: &[String]) -> Result<String, String> {
    let index = args.iter().position(|a| a == "-f").ok_or("apply requires -f FILE")?;
    let file = args.get(index + 1).ok_or("apply requires -f FILE")?;
    let yaml = std::fs::read_to_string(file).map_err(|e| format!("failed to read {file}: {e}"))?;
    let kind = keel_spec::sniff_kind(&yaml).map_err(|e| format!("invalid spec: {e}"))?;
    if kind == "Service" {
        let spec = keel_spec::parse_and_validate_service(&yaml).map_err(|e| format!("invalid spec: {e}"))?;
        let path = jails_path(target, &format!("/services/{}", spec.metadata.name));
        success_body(dispatch(target, "PUT", &path, &yaml)).map(|_| String::new())
    } else if kind == "Ingress" {
        // Bare `/ingress/{name}`, not `jails_path(target, ...)` -- per this
        // milestone's Non-Goals, `Ingress` is never routed through a
        // control plane, so the `--node`-aware path-prefixing `jails_path`
        // exists for doesn't apply here.
        let spec = keel_spec::parse_and_validate_ingress(&yaml).map_err(|e| format!("invalid spec: {e}"))?;
        let path = format!("/ingress/{}", spec.metadata.name);
        success_body(dispatch(target, "PUT", &path, &yaml)).map(|_| String::new())
    } else {
        let spec = keel_spec::parse_and_validate(&yaml).map_err(|e| format!("invalid spec: {e}"))?;
        let path = jails_path(target, &format!("/jails/{}", spec.metadata.name));
        success_body(dispatch(target, "PUT", &path, &yaml)).map(|_| String::new())
    }
}

fn run_get(target: &Target, args: &[String]) -> Result<String, String> {
    match args.first() {
        Some(name) => get_or_delete_with_service_fallback(target, "GET", name),
        None => success_body(dispatch(target, "GET", &jails_path(target, "/jails"), "")),
    }
}

fn run_delete(target: &Target, args: &[String]) -> Result<String, String> {
    let name = args.first().ok_or("delete requires a jail name")?;
    get_or_delete_with_service_fallback(target, "DELETE", name).map(|_| String::new())
}

fn run_delete_volume(target: &Target, args: &[String]) -> Result<String, String> {
    let name = args.first().ok_or("delete-volume requires a volume name")?;
    let path = jails_path(target, &format!("/volumes/{name}"));
    success_body(dispatch(target, "DELETE", &path, "")).map(|_| String::new())
}

fn run_force_repin(target: &Target, args: &[String]) -> Result<String, String> {
    let name = args.first().ok_or("force-repin requires a replica name")?;
    let path = jails_path(target, &format!("/replicas/{name}/force-repin"));
    success_body(dispatch(target, "POST", &path, "")).map(|_| String::new())
}

/// Tries `/jails/<name>` first; on a `404`, retries against
/// `/services/<name>` (jail names and service names share one flat
/// namespace, so a 404 on one path is a cheap, unambiguous signal to try
/// the other); if that's *also* a `404`, retries once more against a bare
/// `/ingress/<name>` (not `jails_path(target, ...)` -- like `run_apply`'s
/// `Ingress` branch, this last attempt is never control-plane-routed, so
/// it skips the `--node`-aware prefixing). If *all three* return 404, the
/// original jail-path error is surfaced -- it's the most familiar message,
/// and the only one available at all against a plain single-node
/// `keel-agentd` predating `Service`/`Ingress` support, which has neither a
/// `/services` nor an `/ingress` route.
fn get_or_delete_with_service_fallback(target: &Target, method: &str, name: &str) -> Result<String, String> {
    let jail_path = jails_path(target, &format!("/jails/{name}"));
    let (status, body) = dispatch(target, method, &jail_path, "")?;
    if status != 404 {
        return success_body(Ok((status, body)));
    }
    let service_path = jails_path(target, &format!("/services/{name}"));
    let (service_status, service_body) = dispatch(target, method, &service_path, "")?;
    if service_status != 404 {
        return success_body(Ok((service_status, service_body)));
    }
    let ingress_path = format!("/ingress/{name}");
    let (ingress_status, ingress_body) = dispatch(target, method, &ingress_path, "")?;
    if ingress_status == 404 {
        success_body(Ok((status, body)))
    } else {
        success_body(Ok((ingress_status, ingress_body)))
    }
}

fn send_request(socket: &PathBuf, method: &str, path: &str, body: &str) -> Result<(u16, String), String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|e| format!("failed to connect to {}: {e}", socket.display()))?;
    let request =
        format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}", body.len());
    stream.write_all(request.as_bytes()).map_err(|e| format!("failed to send request: {e}"))?;
    stream.shutdown(std::net::Shutdown::Write).ok();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).map_err(|e| format!("failed to read response: {e}"))?;
    parse_response(&response)
}

// each parameter is independently needed to dial and authenticate the outbound TLS connection; bundling into a struct would be over-engineering for this single call site
#[allow(clippy::too_many_arguments)]
fn send_request_tcp(
    addr: &str,
    method: &str,
    path: &str,
    body: &str,
    tls_ca_file: &PathBuf,
    tls_cert_file: &PathBuf,
    tls_key_file: &PathBuf,
    tls_crl_file: &PathBuf,
) -> Result<(u16, String), String> {
    let client_config = std::sync::Arc::new(
        tls::load_client_config(tls_cert_file, tls_key_file, tls_ca_file, tls_crl_file)
            .map_err(|e| format!("failed to load TLS client config: {e}"))?,
    );
    let server_name = tls::server_name_from_addr(addr).map_err(|e| e.to_string())?;
    let tcp_stream = TcpStream::connect(addr).map_err(|e| format!("failed to connect to {addr}: {e}"))?;
    let conn = rustls::ClientConnection::new(client_config, server_name).map_err(|e| e.to_string())?;
    let mut stream = rustls::StreamOwned::new(conn, tcp_stream);

    let request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}", body.len());
    stream.write_all(request.as_bytes()).map_err(|e| format!("failed to send request: {e}"))?;
    stream.sock.shutdown(std::net::Shutdown::Write).ok();

    // Read until the peer closes the connection. rustls surfaces a plain TCP
    // close that lacks a TLS `close_notify` alert as `ErrorKind::UnexpectedEof`
    // rather than `Ok(0)`, to guard against truncation attacks in general; we
    // rely on that being safe below by explicitly checking the received body
    // length against the response's own Content-Length header in
    // `parse_response`, so a connection that drops mid-body (an on-path RST,
    // or a crashing control plane) is caught as a truncated response rather
    // than silently accepted. This mirrors `keel_controlplane::http::forward`
    // and `keel_agentd::registration::send_request`'s handling of the same
    // situation on their own TLS client connections.
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

fn parse_response(response: &[u8]) -> Result<(u16, String), String> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Response::new(&mut headers);
    let header_len = match parsed.parse(response).map_err(|e| format!("malformed response: {e}"))? {
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
        return Err(format!("truncated response: expected {content_length} bytes, got {actual}"));
    }
    let response_body = String::from_utf8_lossy(&response[header_len..]).to_string();
    Ok((status, response_body))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spins up a real (fake-backed) `keel-agentd` on a Unix socket and
    /// returns its path, mirroring `keelctl/tests/cli.rs`'s
    /// `start_test_server` -- but callable in-process, since the tests here
    /// exercise `run_apply`/`Target` directly rather than shelling out to
    /// the compiled `keelctl` binary. `known_services` seeds the agent's
    /// `ServiceVipSlot` (name, vip, port) so an `Ingress`'s `backend.service`
    /// can be made to resolve when a test needs the apply to actually
    /// succeed, rather than merely to be routed correctly.
    fn start_test_agent(name: &str, known_services: &[(&str, &str, u16)]) -> PathBuf {
        let state_dir = std::env::temp_dir().join(format!("keelctl-main-test-state-{name}"));
        let _ = std::fs::remove_dir_all(&state_dir);
        let zfs = keel_zfs::FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let replica_targets = keel_agentd::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
        let reconciler = keel_agentd::Reconciler::new(
            keel_jail::FakeJailRuntime::new(),
            zfs.clone(),
            keel_net::FakeNetManager::new(),
            keel_jail::FakeMountManager::new(),
            "zroot".to_string(),
            state_dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(keel_agentd::nginx::FakeNginxController::new()),
            keel_agentd::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) = keel_agentd::worker::spawn(reconciler, zfs, "zroot".to_string(), None);

        let service_vips = keel_agentd::ServiceVipSlot::new();
        let proxy_entries: Vec<_> = known_services
            .iter()
            .map(|(n, vip, port)| keel_controlplane::wire::ServiceProxyEntry {
                name: n.to_string(),
                vip: vip.to_string(),
                port: *port,
                replicas: vec![],
            })
            .collect();
        service_vips.set_all(&proxy_entries);

        // A short, non-descriptive filename (not the full test name) --
        // macOS/BSD cap Unix socket paths at ~104 bytes (SUN_LEN), the same
        // constraint `keelctl/tests/cli.rs`'s helper works around.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let socket_path = std::env::temp_dir().join(format!("kam-{}-{}.sock", std::process::id(), id));
        let _ = std::fs::remove_file(&socket_path);
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        std::thread::spawn(move || {
            keel_agentd::http::run(listener, commands, keel_agentd::PodCidrSlot::new(), service_vips, replica_targets)
        });
        socket_path
    }

    #[test]
    fn run_apply_routes_an_ingress_spec_to_the_ingress_endpoint_not_the_jails_endpoint() {
        let socket = start_test_agent("run_apply_routes_an_ingress_spec", &[]);
        let target = Target::Socket(socket);
        let path = std::env::temp_dir().join("keelctl-test-ingress-run-apply.yaml");
        std::fs::write(
            &path,
            "apiVersion: keel/v1\nkind: Ingress\nmetadata:\n  name: blog\nspec:\n  host: example.com\n  backend:\n    service: hugo-site\n    port: 8080\n  tls:\n    email: admin@example.com\n",
        )
        .unwrap();

        // No 'hugo-site' Service is known to this agent, so a spec that was
        // genuinely routed to /ingress/blog must fail with the ingress
        // handler's "does not name a currently-known Service" 400 -- NOT
        // with a JailSpec YAML-parse error, which is what falling into the
        // Jail else-branch would produce instead.
        let result = run_apply(&target, &["-f".to_string(), path.to_string_lossy().to_string()]);
        let err = result.expect_err("expected the ingress backend-service validation to reject this spec");
        assert!(err.contains("hugo-site"), "expected the Service-not-found message, got: {err}");
    }

    #[test]
    fn get_and_delete_fall_back_to_the_ingress_route_when_a_name_is_neither_a_jail_nor_a_service() {
        // 'hugo-site' is a known Service here (unlike the routing test
        // above), so the Ingress apply itself succeeds -- this test proves
        // the *other* two verbs (`get`/`delete`) reach the same
        // `/ingress/blog` route that `run_apply` does, via
        // `get_or_delete_with_service_fallback`'s third fallback leg, and
        // that a real end-to-end apply -> get -> delete -> get cycle works.
        let socket = start_test_agent(
            "get_and_delete_fall_back_to_ingress",
            &[("hugo-site", "10.0.0.9", 8080)],
        );
        let target = Target::Socket(socket);
        let path = std::env::temp_dir().join("keelctl-test-ingress-get-delete.yaml");
        std::fs::write(
            &path,
            "apiVersion: keel/v1\nkind: Ingress\nmetadata:\n  name: blog\nspec:\n  host: example.com\n  backend:\n    service: hugo-site\n    port: 8080\n  tls:\n    email: admin@example.com\n",
        )
        .unwrap();
        run_apply(&target, &["-f".to_string(), path.to_string_lossy().to_string()])
            .expect("apply of a valid Ingress spec against a known Service should succeed");

        // 'blog' is neither a jail nor a service name, so this only
        // succeeds if the third fallback leg (bare /ingress/blog) is
        // reached and returns the applied Ingress.
        let get_result = run_get(&target, &["blog".to_string()]).expect("get should fall back to /ingress/blog");
        assert!(get_result.contains("host: example.com"), "got: {get_result}");

        run_delete(&target, &["blog".to_string()]).expect("delete should fall back to /ingress/blog");

        // After delete, all three routes 404; the surfaced error must be
        // the original jail-not-found message (the documented behavior when
        // all fallback legs are exhausted).
        let err_after_delete = run_get(&target, &["blog".to_string()]).unwrap_err();
        assert!(err_after_delete.contains("not found"), "got: {err_after_delete}");
    }

    #[test]
    fn no_control_plane_flags_yields_socket_target() {
        let target = resolve_target(PathBuf::from("/var/run/keel-agentd.sock"), None, None, None, None, None, None).unwrap();
        assert_eq!(target, Target::Socket(PathBuf::from("/var/run/keel-agentd.sock")));
    }

    #[test]
    fn node_without_control_plane_addr_is_an_error() {
        let err = resolve_target(
            PathBuf::from("/var/run/keel-agentd.sock"),
            None,
            Some("node-1".to_string()),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(err, "--node requires --control-plane-addr");
    }

    #[test]
    fn control_plane_addr_without_tls_flags_is_an_error() {
        let err = resolve_target(
            PathBuf::from("/var/run/keel-agentd.sock"),
            Some("10.0.0.1:7620".to_string()),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "--tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required with --control-plane-addr"
        );
    }

    #[test]
    fn control_plane_addr_with_all_tls_flags_builds_a_control_plane_target() {
        let target = resolve_target(
            PathBuf::from("/var/run/keel-agentd.sock"),
            Some("10.0.0.1:7620".to_string()),
            Some("node-1".to_string()),
            Some("/etc/keel/ca.crt".to_string()),
            Some("/etc/keel/alice.crt".to_string()),
            Some("/etc/keel/alice.key".to_string()),
            Some("/etc/keel/crl.pem".to_string()),
        )
        .unwrap();
        assert_eq!(
            target,
            Target::ControlPlane {
                addr: "10.0.0.1:7620".to_string(),
                node: Some("node-1".to_string()),
                tls_ca_file: PathBuf::from("/etc/keel/ca.crt"),
                tls_cert_file: PathBuf::from("/etc/keel/alice.crt"),
                tls_key_file: PathBuf::from("/etc/keel/alice.key"),
                tls_crl_file: PathBuf::from("/etc/keel/crl.pem"),
            }
        );
    }
}
