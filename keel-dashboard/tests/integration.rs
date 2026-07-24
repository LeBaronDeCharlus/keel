use base64::{engine::general_purpose::STANDARD, Engine as _};
use keel_controlplane::wire::{NodeState, NodeStatus, ServiceReplica, ServiceSummary};
use keel_dashboard::control_plane_client::FakeControlPlaneClient;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
}

#[derive(Debug)]
struct NoVerify;
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes()
    }
}

fn request(addr: std::net::SocketAddr, path: &str, user: &str, password: &str) -> (u16, String) {
    keel_dashboard::tls::ensure_crypto_provider();
    let client_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    let server_name = rustls::pki_types::ServerName::IpAddress(std::net::Ipv4Addr::new(127, 0, 0, 1).into());
    let tcp = std::net::TcpStream::connect(addr).unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(client_config), server_name).unwrap();
    let mut stream = rustls::StreamOwned::new(conn, tcp);
    let auth = format!("Basic {}", STANDARD.encode(format!("{user}:{password}")));
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {auth}\r\nContent-Length: 0\r\n\r\n");
    stream.write_all(req.as_bytes()).unwrap();
    stream.sock.shutdown(std::net::Shutdown::Write).ok();
    let mut response = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&response).to_string();
    let status: u16 = text.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[test]
fn a_poll_cycle_is_reflected_in_both_the_json_api_and_the_rendered_html() {
    let client = FakeControlPlaneClient::new();
    client.set_nodes(vec![NodeStatus {
        id: "node-1".to_string(),
        addr: "192.168.64.4:7621".to_string(),
        pod_cidr: "10.0.4.0/24".to_string(),
        status: NodeState::Alive,
        last_seen_secs: 1,
        capacity_cpu: 4.0,
        capacity_memory: 8 * 1024 * 1024 * 1024,
        committed_cpu: 1.0,
        committed_memory: 1024 * 1024 * 1024,
        ingresses: vec![],
    }]);
    client.set_jails("node-1", vec![]);
    client.set_volumes("node-1", vec![]);
    client.set_services(vec![ServiceSummary { name: "web".to_string(), desired_replicas: 1, vip: "10.0.250.7".to_string(), port: 8080 }]);
    client.set_service("web", Vec::<ServiceReplica>::new());

    let snapshot = keel_dashboard::poller::spawn(Box::new(client), Duration::from_millis(20));

    let mut attempts = 0;
    loop {
        if !snapshot.read().unwrap().nodes.is_empty() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "the poller never populated the snapshot");
        std::thread::sleep(Duration::from_millis(20));
    }

    let tls_config = Arc::new(
        keel_dashboard::tls::load_browser_server_config(&fixture("fixture-node.crt"), &fixture("fixture-node.key")).unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        keel_dashboard::http::run(listener, tls_config, snapshot, "admin".to_string(), "hunter2".to_string())
    });

    let (unauth_status, _) = request(addr, "/", "admin", "wrongpassword");
    assert_eq!(unauth_status, 401, "wrong credentials must be rejected even after a successful poll");

    let (json_status, json_body) = request(addr, "/api/snapshot", "admin", "hunter2");
    assert_eq!(json_status, 200);
    assert!(json_body.contains("\"node-1\""), "got: {json_body}");
    assert!(json_body.contains("\"web\""), "got: {json_body}");

    let (html_status, html_body) = request(addr, "/", "admin", "hunter2");
    assert_eq!(html_status, 200);
    assert!(html_body.contains("node-1"), "got: {html_body}");
    assert!(html_body.contains("10.0.250.7"), "got: {html_body}");
}
