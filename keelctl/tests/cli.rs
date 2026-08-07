use keel_agentd::{worker, Reconciler};
use keel_jail::{FakeJailRuntime, FakeMountManager};
use keel_net::FakeNetManager;
use keel_zfs::FakeZfsManager;
use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::Command;
use std::thread;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
}

fn fresh_controlplane_state_dir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "keelctl-test-controlplane-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn start_test_server(name: &str) -> PathBuf {
    let state_dir = std::env::temp_dir().join(format!("keelctl-test-state-{name}"));
    let _ = std::fs::remove_dir_all(&state_dir);
    let zfs = FakeZfsManager::new();
    zfs.seed_dataset("zroot/keel/base/14.2-web");
    let replica_targets = keel_agentd::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
    let reconciler = Reconciler::new(
        FakeJailRuntime::new(),
        zfs.clone(),
        FakeNetManager::new(),
        FakeMountManager::new(),
        "zroot".to_string(),
        state_dir,
        Box::new(keel_ingress::FakeAcmeClient::new()),
        Box::new(keel_ingress::FakeDnsProvider::new()),
        Box::new(keel_agentd::nginx::FakeNginxController::new()),
        keel_agentd::ServiceVipSlot::new(),
    )
    .unwrap();
    let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string(), None);

    // A short, non-descriptive filename (not the full test name) — macOS/BSD
    // cap Unix socket paths at ~104 bytes (SUN_LEN), and the default macOS
    // TMPDIR (/var/folders/.../T/) already uses ~50 of them.
    let socket_path = short_unique_socket_path();
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path).unwrap();
    thread::spawn(move || {
        keel_agentd::http::run(
            listener,
            commands,
            keel_agentd::PodCidrSlot::new(),
            keel_agentd::ServiceVipSlot::new(),
            replica_targets,
        )
    });
    socket_path
}

fn short_unique_socket_path() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ka-{}-{}.sock", std::process::id(), id))
}

fn write_spec_file(test_name: &str, jail_name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("keelctl-test-spec-{test_name}.yaml"));
    let yaml = format!(
        "apiVersion: keel/v1\nkind: Jail\nmetadata:\n  name: {jail_name}\nspec:\n  image: base/14.2-web\n  command: [\"/usr/local/bin/myapp\"]\n  network:\n    vnet: true\n    bridge: keel0\n    address: 10.0.0.5/24\n  resources:\n    cpu: \"2\"\n    memory: 512M\n  restartPolicy: Always\n"
    );
    std::fs::write(&path, yaml).unwrap();
    path
}

fn run_keelctl(socket_path: &PathBuf, args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_keelctl"))
        .args(args)
        .arg("--socket")
        .arg(socket_path)
        .output()
        .expect("failed to run keelctl binary");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn start_test_agentd_tcp(name: &str) -> String {
    let state_dir = std::env::temp_dir().join(format!("keelctl-routed-test-state-{name}"));
    let _ = std::fs::remove_dir_all(&state_dir);
    let zfs = FakeZfsManager::new();
    zfs.seed_dataset("zroot/keel/base/14.2-web");
    let replica_targets = keel_agentd::ReplicaTargetRegistry::load(state_dir.clone()).unwrap();
    let reconciler = Reconciler::new(
        FakeJailRuntime::new(),
        zfs.clone(),
        FakeNetManager::new(),
        FakeMountManager::new(),
        "zroot".to_string(),
        state_dir,
        Box::new(keel_ingress::FakeAcmeClient::new()),
        Box::new(keel_ingress::FakeDnsProvider::new()),
        Box::new(keel_agentd::nginx::FakeNginxController::new()),
        keel_agentd::ServiceVipSlot::new(),
    )
    .unwrap();
    let (_worker_handle, commands) = worker::spawn(reconciler, zfs, "zroot".to_string(), None);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let reloading_tls = keel_agentd::tls::ReloadingTls::spawn(
        fixture("fixture-node.crt"),
        fixture("fixture-node.key"),
        fixture("ca.crt"),
        fixture("crl.pem"),
        std::time::Duration::from_secs(3600),
    )
    .unwrap();
    thread::spawn(move || {
        keel_agentd::http::run_tls(
            listener,
            commands,
            reloading_tls,
            keel_agentd::PodCidrSlot::new(),
            keel_agentd::ServiceVipSlot::new(),
            replica_targets,
        )
    });
    addr
}

fn start_test_control_plane_with_node(node_id: &str, node_addr: &str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (_worker_handle, commands) = keel_controlplane::worker::spawn(
        keel_controlplane::Registry::new("10.0.0.0/16".parse().unwrap()),
        keel_controlplane::Placements::new(),
        keel_controlplane::Services::new("10.0.250.0/24".parse().unwrap()),
        keel_controlplane::addresses::UsedAddresses::new(),
        keel_controlplane::Standbys::new(),
        keel_controlplane::PendingFences::new(),
        keel_controlplane::Cordoned::new(),
        fresh_controlplane_state_dir(),
    );

    let (reg_tx, reg_rx) = std::sync::mpsc::channel();
    commands
        .send(keel_controlplane::worker::Command::Register(
            node_id.to_string(),
            node_addr.to_string(),
            None,
            4.0,
            8 * 1024 * 1024 * 1024,
            reg_tx,
        ))
        .unwrap();
    reg_rx.recv().unwrap().unwrap();

    let reloading_tls = keel_controlplane::tls::ReloadingTls::spawn(
        fixture("fixture-node.crt"),
        fixture("fixture-node.key"),
        fixture("ca.crt"),
        fixture("crl.pem"),
        std::time::Duration::from_secs(3600),
    )
    .unwrap();
    thread::spawn(move || keel_controlplane::http::run(listener, commands, reloading_tls));
    addr
}

fn run_keelctl_routed(
    control_plane_addr: &str,
    node: &str,
    args: &[&str],
) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_keelctl"))
        .args(args)
        .arg("--control-plane-addr")
        .arg(control_plane_addr)
        .arg("--node")
        .arg(node)
        .arg("--tls-ca-file")
        .arg(fixture("ca.crt"))
        .arg("--tls-cert-file")
        .arg(fixture("fixture-client.crt"))
        .arg("--tls-key-file")
        .arg(fixture("fixture-client.key"))
        .arg("--tls-crl-file")
        .arg(fixture("crl.pem"))
        .output()
        .expect("failed to run keelctl binary");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// A fake control plane that completes the TLS handshake, drains whatever
/// request keelctl sends, then responds with a header claiming a
/// `Content-Length` larger than the body it actually writes before dropping
/// the raw TCP connection without a clean TLS shutdown (no `close_notify`) --
/// simulating an on-path RST or a control plane that crashes mid-write.
/// Matches the pattern of `keel_controlplane::http`'s own
/// `start_fake_remote_tls_agentd_with_truncated_body` and
/// `keel_agentd::registration`'s `start_fake_control_plane_with_truncated_body`.
fn start_fake_control_plane_with_truncated_body(
    claimed_body: &'static str,
    actual_body: &'static str,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server_config = std::sync::Arc::new(
        keel_controlplane::tls::load_server_config(
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
            // Drop the raw TCP connection without sending a TLS close_notify
            // alert, so keelctl sees an unclean close partway through the
            // declared body.
            let _ = tls_stream.sock.shutdown(std::net::Shutdown::Both);
        }
    });
    addr
}

#[test]
fn get_against_a_control_plane_that_truncates_mid_body_fails_instead_of_printing_a_partial_response(
) {
    // The header claims a much longer body than what actually gets written
    // before the connection drops uncleanly (no close_notify). Before the
    // fix, keelctl's UnexpectedEof tolerance let this print the partial body
    // to the operator as if it were a complete, successful response.
    let control_plane_addr = start_fake_control_plane_with_truncated_body(
        "this response claims to be far longer than what is actually sent back\n",
        "truncat",
    );

    let (ok, stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["get", "web-1"]);

    assert!(!ok, "expected a truncated response to be treated as a failure, got success with stdout: {stdout}");
    assert!(!stdout.contains("truncat"), "truncated upstream body must not be printed as if it were a complete response, got stdout: {stdout}");
    assert!(
        stderr.contains("truncated response"),
        "expected a truncation error in stderr, got: {stderr}"
    );
}

fn run_keelctl_scheduled(control_plane_addr: &str, args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_keelctl"))
        .args(args)
        .arg("--control-plane-addr")
        .arg(control_plane_addr)
        .arg("--tls-ca-file")
        .arg(fixture("ca.crt"))
        .arg("--tls-cert-file")
        .arg(fixture("fixture-client.crt"))
        .arg("--tls-key-file")
        .arg(fixture("fixture-client.key"))
        .arg("--tls-crl-file")
        .arg(fixture("crl.pem"))
        .output()
        .expect("failed to run keelctl binary");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

#[test]
fn apply_get_delete_round_trip_through_the_control_plane() {
    let node_addr = start_test_agentd_tcp("routed_round_trip");
    let control_plane_addr = start_test_control_plane_with_node("node-1", &node_addr);
    let spec_path = write_spec_file("routed_round_trip", "web-1");

    let (ok, _, stderr) = run_keelctl_routed(
        &control_plane_addr,
        "node-1",
        &["apply", "-f", spec_path.to_str().unwrap()],
    );
    assert!(ok, "apply failed: {stderr}");

    let (ok, stdout, stderr) = run_keelctl_routed(&control_plane_addr, "node-1", &["get", "web-1"]);
    assert!(ok, "get failed: {stderr}");
    assert!(
        stdout.contains("running: true"),
        "expected running: true, got: {stdout}"
    );

    let (ok, _, stderr) = run_keelctl_routed(&control_plane_addr, "node-1", &["delete", "web-1"]);
    assert!(ok, "delete failed: {stderr}");
}

#[test]
fn apply_through_the_control_plane_to_an_unknown_node_fails() {
    let control_plane_addr = start_test_control_plane_with_node("node-1", "127.0.0.1:1");
    let spec_path = write_spec_file("routed_unknown_node", "web-1");

    let (ok, _, stderr) = run_keelctl_routed(
        &control_plane_addr,
        "node-missing",
        &["apply", "-f", spec_path.to_str().unwrap()],
    );
    assert!(!ok);
    assert!(
        stderr.contains("unknown node"),
        "expected 'unknown node' in stderr, got: {stderr}"
    );
}

#[test]
fn control_plane_addr_without_node_schedules_through_the_control_plane() {
    let node_addr = start_test_agentd_tcp("scheduled_round_trip");
    let control_plane_addr = start_test_control_plane_with_node("node-1", &node_addr);
    let spec_path = write_spec_file("scheduled_round_trip", "web-1");

    let (ok, _, stderr) = run_keelctl_scheduled(
        &control_plane_addr,
        &["apply", "-f", spec_path.to_str().unwrap()],
    );
    assert!(ok, "apply failed: {stderr}");

    let (ok, stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["get", "web-1"]);
    assert!(ok, "get failed: {stderr}");
    assert!(
        stdout.contains("running: true"),
        "expected running: true, got: {stdout}"
    );

    let (ok, _, stderr) = run_keelctl_scheduled(&control_plane_addr, &["delete", "web-1"]);
    assert!(ok, "delete failed: {stderr}");
}

#[test]
fn node_without_control_plane_addr_is_a_usage_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_keelctl"))
        .args(["get"])
        .arg("--node")
        .arg("node-1")
        .output()
        .expect("failed to run keelctl binary");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--node requires --control-plane-addr"),
        "got: {stderr}"
    );
}

#[test]
fn apply_get_delete_round_trip() {
    let socket_path = start_test_server("apply_get_delete_round_trip");
    let spec_path = write_spec_file("apply_get_delete_round_trip", "web-1");

    let (ok, _, stderr) = run_keelctl(&socket_path, &["apply", "-f", spec_path.to_str().unwrap()]);
    assert!(ok, "apply failed: {stderr}");

    let (ok, stdout, stderr) = run_keelctl(&socket_path, &["get", "web-1"]);
    assert!(ok, "get failed: {stderr}");
    assert!(
        stdout.contains("running: true"),
        "expected running: true, got: {stdout}"
    );

    let (ok, _, stderr) = run_keelctl(&socket_path, &["delete", "web-1"]);
    assert!(ok, "delete failed: {stderr}");

    let (ok, _, stderr) = run_keelctl(&socket_path, &["get", "web-1"]);
    assert!(!ok, "expected get on a deleted jail to fail");
    assert!(
        stderr.contains("not found"),
        "expected 'not found' in stderr, got: {stderr}"
    );
}

fn write_spec_file_with_volume(test_name: &str, jail_name: &str, volume_name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("keelctl-test-spec-{test_name}.yaml"));
    let yaml = format!(
        "apiVersion: keel/v1\nkind: Jail\nmetadata:\n  name: {jail_name}\nspec:\n  image: base/14.2-web\n  command: [\"/usr/local/bin/myapp\"]\n  network:\n    vnet: true\n    bridge: keel0\n    address: 10.0.0.5/24\n  resources:\n    cpu: \"2\"\n    memory: 512M\n  restartPolicy: Always\n  volumes:\n    - name: {volume_name}\n      mountPath: /data\n      size: 1G\n"
    );
    std::fs::write(&path, yaml).unwrap();
    path
}

#[test]
fn delete_volume_survives_the_jails_deletion_then_frees_the_dataset() {
    let socket_path =
        start_test_server("delete_volume_survives_the_jails_deletion_then_frees_the_dataset");
    let spec_path = write_spec_file_with_volume(
        "delete_volume_survives_the_jails_deletion_then_frees_the_dataset",
        "web-1",
        "web-data",
    );

    let (ok, _, stderr) = run_keelctl(&socket_path, &["apply", "-f", spec_path.to_str().unwrap()]);
    assert!(ok, "apply failed: {stderr}");

    let (ok, _, stderr) = run_keelctl(&socket_path, &["delete", "web-1"]);
    assert!(ok, "delete failed: {stderr}");

    let (ok, _, stderr) = run_keelctl(&socket_path, &["delete-volume", "web-data"]);
    assert!(ok, "delete-volume failed: {stderr}");
}

#[test]
fn delete_volume_on_a_never_created_name_fails_with_not_found() {
    let socket_path =
        start_test_server("delete_volume_on_a_never_created_name_fails_with_not_found");
    let (ok, _, stderr) = run_keelctl(&socket_path, &["delete-volume", "missing"]);
    assert!(
        !ok,
        "expected delete-volume on a never-created volume to fail"
    );
    assert!(
        stderr.contains("not found"),
        "expected 'not found' in stderr, got: {stderr}"
    );
}

fn write_service_spec_file(test_name: &str, service_name: &str, replicas: u32) -> PathBuf {
    let path = std::env::temp_dir().join(format!("keelctl-test-service-spec-{test_name}.yaml"));
    let yaml = format!(
        "apiVersion: keel/v1\nkind: Service\nmetadata:\n  name: {service_name}\nspec:\n  replicas: {replicas}\n  port: 8080\n  template:\n    image: base/14.2-web\n    command: [\"/usr/local/bin/myapp\"]\n    network:\n      vnet: true\n      bridge: keel0\n    resources:\n      cpu: \"1\"\n      memory: 256M\n    restartPolicy: Always\n"
    );
    std::fs::write(&path, yaml).unwrap();
    path
}

#[test]
fn apply_routes_a_service_kind_to_the_services_path() {
    let node_addr = start_test_agentd_tcp("service_apply_routing");
    let control_plane_addr = start_test_control_plane_with_node("node-1", &node_addr);
    let spec_path = write_service_spec_file("service_apply_routing", "web", 0);

    let (ok, _, stderr) = run_keelctl_scheduled(
        &control_plane_addr,
        &["apply", "-f", spec_path.to_str().unwrap()],
    );
    assert!(ok, "service apply failed: {stderr}");

    let (ok, body, stderr) = run_keelctl_scheduled(&control_plane_addr, &["get", "web"]);
    assert!(
        ok,
        "expected the jail-path 404 to fall back to the service path: {stderr}"
    );
    assert_eq!(
        body.trim(),
        "[]",
        "a zero-replica service has no discoverable replicas yet"
    );
}

#[test]
fn delete_falls_back_from_jail_to_service_on_404() {
    let control_plane_addr = start_test_control_plane_with_node("node-1", "127.0.0.1:1");
    let spec_path = write_service_spec_file("service_delete_fallback", "web", 0);
    run_keelctl_scheduled(
        &control_plane_addr,
        &["apply", "-f", spec_path.to_str().unwrap()],
    );

    let (ok, _, stderr) = run_keelctl_scheduled(&control_plane_addr, &["delete", "web"]);
    assert!(
        ok,
        "expected delete to fall back to the service path: {stderr}"
    );
}

#[test]
fn get_on_a_name_that_is_neither_a_jail_nor_a_service_still_reports_jail_not_found() {
    let socket_path = start_test_server("neither_jail_nor_service");
    let (ok, _, stderr) = run_keelctl(&socket_path, &["get", "missing"]);
    assert!(!ok);
    assert!(
        stderr.contains("not found"),
        "expected the original jail-not-found error preserved, got: {stderr}"
    );
}

#[test]
fn apply_rejects_a_file_with_an_invalid_spec() {
    let socket_path = start_test_server("apply_rejects_a_file_with_an_invalid_spec");
    let path = std::env::temp_dir().join("keelctl-test-invalid-spec.yaml");
    std::fs::write(&path, "not: valid: yaml: [").unwrap();

    let (ok, _, stderr) = run_keelctl(&socket_path, &["apply", "-f", path.to_str().unwrap()]);
    assert!(!ok);
    assert!(!stderr.is_empty());
}

#[test]
fn get_lists_multiple_applied_jails() {
    let socket_path = start_test_server("get_lists_multiple_applied_jails");
    run_keelctl(
        &socket_path,
        &[
            "apply",
            "-f",
            write_spec_file("get_lists_multiple_applied_jails_1", "web-1")
                .to_str()
                .unwrap(),
        ],
    );
    run_keelctl(
        &socket_path,
        &[
            "apply",
            "-f",
            write_spec_file("get_lists_multiple_applied_jails_2", "web-2")
                .to_str()
                .unwrap(),
        ],
    );

    let (ok, stdout, stderr) = run_keelctl(&socket_path, &["get"]);
    assert!(ok, "get failed: {stderr}");
    assert!(stdout.contains("web-1"));
    assert!(stdout.contains("web-2"));
}

#[test]
fn force_repin_on_an_unplaced_name_prints_the_control_planes_404_message() {
    let control_plane_addr = start_test_control_plane_with_node("node-1", "10.0.0.1:7621");
    let (ok, _stdout, stderr) =
        run_keelctl_scheduled(&control_plane_addr, &["force-repin", "db-0"]);
    assert!(!ok, "expected force-repin on an unplaced name to fail");
    assert!(
        stderr.contains("no known placement"),
        "got stderr: {stderr}"
    );
}

#[test]
fn cordon_an_unknown_node_prints_the_control_planes_404_message() {
    let control_plane_addr = start_test_control_plane_with_node("node-1", "10.0.0.1:7621");
    let (ok, _stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["cordon", "ghost"]);
    assert!(!ok, "expected cordon on an unknown node to fail");
    assert!(stderr.contains("unknown node"), "got stderr: {stderr}");
}

#[test]
fn cordon_then_uncordon_a_known_node_round_trips_successfully() {
    let control_plane_addr = start_test_control_plane_with_node("node-1", "10.0.0.1:7621");
    let (ok, _stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["cordon", "node-1"]);
    assert!(ok, "cordon failed: {stderr}");

    let (ok, _stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["uncordon", "node-1"]);
    assert!(ok, "uncordon failed: {stderr}");
}

#[test]
fn drain_an_unknown_node_prints_the_control_planes_404_message() {
    let control_plane_addr = start_test_control_plane_with_node("node-1", "10.0.0.1:7621");
    let (ok, _stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["drain", "ghost"]);
    assert!(!ok, "expected drain on an unknown node to fail");
    assert!(stderr.contains("unknown node"), "got stderr: {stderr}");
}

#[test]
fn drain_a_known_empty_node_succeeds() {
    let control_plane_addr = start_test_control_plane_with_node("node-1", "10.0.0.1:7621");
    let (ok, _stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["drain", "node-1"]);
    assert!(ok, "drain failed: {stderr}");
}

#[test]
fn backup_create_list_and_restore_round_trip_through_the_control_plane() {
    let node_addr = start_test_agentd_tcp("backup_round_trip");
    let control_plane_addr = start_test_control_plane_with_node("node-1", &node_addr);

    let (ok, create_stdout, stderr) =
        run_keelctl_scheduled(&control_plane_addr, &["backup", "create"]);
    assert!(ok, "backup create failed: {stderr}");
    assert!(
        create_stdout.contains("success: true"),
        "expected a successful backup manifest, got: {create_stdout}"
    );
    let id = create_stdout
        .lines()
        .find_map(|line| line.strip_prefix("id: "))
        .expect("expected an 'id: <value>' line in the backup create output")
        .trim()
        .to_string();

    let (ok, list_stdout, stderr) = run_keelctl_scheduled(&control_plane_addr, &["backup", "list"]);
    assert!(ok, "backup list failed: {stderr}");
    assert!(
        list_stdout.contains(&id),
        "expected the created backup's id to show up in the list, got: {list_stdout}"
    );

    let (ok, restore_stdout, stderr) =
        run_keelctl_scheduled(&control_plane_addr, &["restore", &id, "--yes"]);
    assert!(ok, "restore failed: {stderr}");
    assert!(
        restore_stdout.contains("success: true"),
        "expected a successful restore manifest, got: {restore_stdout}"
    );
    // Neither side hot-reloads restored state (both read their state dir
    // only at process startup), so the manifest alone would leave an
    // operator believing the restore had already taken effect.
    assert!(
        restore_stdout.contains("Restart keel-controlplane"),
        "expected the restart reminder the design doc requires, got: {restore_stdout}"
    );

    // `--yes` before the id must work identically to `--yes` after it: the
    // id lookup has to skip the flag regardless of its position, rather than
    // blindly taking the first token (which would send the literal id
    // "--yes" to the control plane).
    let (ok, restore_stdout, stderr) =
        run_keelctl_scheduled(&control_plane_addr, &["restore", "--yes", &id]);
    assert!(ok, "flag-first restore failed: {stderr}");
    assert!(
        restore_stdout.contains("success: true"),
        "expected a successful restore manifest, got: {restore_stdout}"
    );
}

#[test]
fn restore_without_yes_is_rejected_without_contacting_the_control_plane() {
    // An unroutable address: if this reached dispatch() at all, the test
    // would hang or fail with a connection error instead of the
    // confirmation-required message below.
    let (ok, _stdout, stderr) = run_keelctl_scheduled("127.0.0.1:1", &["restore", "some-id"]);
    assert!(!ok, "expected restore without --yes to fail");
    assert!(
        stderr.contains("--yes"),
        "expected the confirmation-required message, got: {stderr}"
    );
}
