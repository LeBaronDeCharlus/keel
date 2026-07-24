use crate::snapshot::Snapshot;
use rustls::{ServerConnection, StreamOwned};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

/// Every accepted connection gets its own OS thread with no concurrency cap,
/// so a client that connects and never sends anything would otherwise pin a
/// thread forever -- before the TLS handshake even starts. A read timeout
/// bounds that. This listener is meant to be internet-facing, so it matters
/// more here than almost anywhere else in the codebase.
const INBOUND_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn apply_read_timeout(stream: &TcpStream) {
    let _ = stream.set_read_timeout(Some(INBOUND_READ_TIMEOUT));
}

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

pub fn run(listener: TcpListener, tls_config: Arc<rustls::ServerConfig>, snapshot: Arc<RwLock<Snapshot>>) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        apply_read_timeout(&stream);
        let tls_config = Arc::clone(&tls_config);
        let snapshot = Arc::clone(&snapshot);
        thread::spawn(move || {
            let Ok(conn) = ServerConnection::new(tls_config) else { return };
            let mut tls_stream = TlsStream::new(conn, stream);
            let _ = handle_connection(&mut tls_stream, &snapshot);
        });
    }
}

struct ParsedRequest {
    method: String,
    path: String,
}

fn handle_connection(stream: &mut TlsStream, snapshot: &Arc<RwLock<Snapshot>>) -> io::Result<()> {
    let request = match read_request(stream)? {
        Some(r) => r,
        None => return Ok(()),
    };
    let (status, content_type, body) = route(&request, snapshot);
    write_response(stream, status, content_type, &body)
}

fn read_request(stream: &mut TlsStream) -> io::Result<Option<ParsedRequest>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(_)) => {
                let method = req.method.unwrap_or("").to_string();
                let path = req.path.unwrap_or("").to_string();
                return Ok(Some(ParsedRequest { method, path }));
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
    }
}

fn write_response(stream: &mut TlsStream, status: u16, content_type: &str, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n",
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
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

fn route(request: &ParsedRequest, snapshot: &Arc<RwLock<Snapshot>>) -> (u16, &'static str, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => {
            let snapshot = snapshot.read().unwrap();
            let now_unix =
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
            (200, "text/html", crate::html::render(&snapshot, now_unix).into_bytes())
        }
        ("GET", "/api/snapshot") => {
            let snapshot = snapshot.read().unwrap();
            json_response(&*snapshot)
        }
        _ => (404, "text/plain", b"not found".to_vec()),
    }
}

/// Serializes `value` to a JSON response, without panicking (and thus
/// without poisoning the caller's `RwLock` read guard) if serialization
/// ever fails. `serde_json` returns `Err` for some inputs (e.g. a map with
/// a non-finite float key) even though `Snapshot` itself can't currently
/// produce one - handling the `Result` here is cheap insurance against
/// that changing, or against a future field type that can fail, rather
/// than relying on serialization staying infallible forever.
fn json_response<T: serde::Serialize>(value: &T) -> (u16, &'static str, Vec<u8>) {
    match serde_json::to_vec(value) {
        Ok(body) => (200, "application/json", body),
        Err(_) => (500, "text/plain", b"failed to serialize snapshot".to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::Snapshot;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    #[test]
    fn apply_read_timeout_sets_the_configured_timeout_on_a_real_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).unwrap();
        let (server_stream, _) = listener.accept().unwrap();
        apply_read_timeout(&server_stream);
        assert_eq!(server_stream.read_timeout().unwrap(), Some(INBOUND_READ_TIMEOUT));
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn start_test_server(snapshot: Snapshot) -> std::net::SocketAddr {
        let tls_config = Arc::new(
            crate::tls::load_browser_server_config(&fixture("fixture-node.crt"), &fixture("fixture-node.key")).unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let snapshot = Arc::new(RwLock::new(snapshot));
        std::thread::spawn(move || run(listener, tls_config, snapshot));
        addr
    }

    fn request(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
        crate::tls::ensure_crypto_provider();
        let roots = rustls::RootCertStore::empty();
        let verifier = std::sync::Arc::new(NoVerify);
        let client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        let _ = roots;
        let server_name = rustls::pki_types::ServerName::IpAddress(std::net::Ipv4Addr::new(127, 0, 0, 1).into());
        let tcp = std::net::TcpStream::connect(addr).unwrap();
        let conn = rustls::ClientConnection::new(Arc::new(client_config), server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp);
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
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

    #[test]
    fn a_request_gets_the_dashboard_html() {
        let addr = start_test_server(Snapshot::default());
        let (status, body) = request(addr, "/");
        assert_eq!(status, 200);
        assert!(body.contains("keel dashboard"), "got: {body}");
    }

    #[test]
    fn api_snapshot_returns_json() {
        let addr = start_test_server(Snapshot::default());
        let (status, body) = request(addr, "/api/snapshot");
        assert_eq!(status, 200);
        assert!(body.contains("\"nodes\""), "got: {body}");
    }

    #[test]
    fn an_unknown_path_is_404() {
        let addr = start_test_server(Snapshot::default());
        let (status, _) = request(addr, "/nope");
        assert_eq!(status, 404);
    }

    #[test]
    fn a_snapshot_with_a_non_finite_float_does_not_panic_or_poison_the_lock() {
        // `serde_json` (as vendored here, 1.0.151) serializes non-finite
        // f64 struct fields as JSON `null` rather than returning `Err` -
        // verified against `serde_json::ser::Serializer::serialize_f64`,
        // which only errors on non-finite floats used as *map keys*, not
        // as ordinary field values. So this specific input doesn't
        // actually exercise the 500 path below; it's still worth a test
        // to confirm NaN data flowing in from the network is served
        // without panicking and without poisoning the shared `RwLock`
        // (see `json_response_returns_a_clean_500_on_a_genuine_serialization_error`
        // for the actual error-path coverage).
        use crate::snapshot::NodeSnapshot;
        use keel_controlplane::wire::{NodeState, NodeStatus};

        let node_status = NodeStatus {
            id: "node-1".to_string(),
            addr: "192.168.64.4:7621".to_string(),
            pod_cidr: "10.0.4.0/24".to_string(),
            status: NodeState::Alive,
            last_seen_secs: 1,
            capacity_cpu: f64::NAN,
            capacity_memory: 8 * 1024 * 1024 * 1024,
            committed_cpu: 1.0,
            committed_memory: 1024 * 1024 * 1024,
            ingresses: vec![],
        };
        let node = NodeSnapshot { status: node_status, jails: vec![], volumes: vec![], data_stale: false };
        let snapshot =
            Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };

        let addr = start_test_server(snapshot);
        let (status, body) = request(addr, "/api/snapshot");
        assert_eq!(status, 200, "got body: {body}");
        assert!(body.contains("\"capacity_cpu\":null"), "got: {body}");

        // The lock must not be poisoned: a follow-up request against the
        // same running server (and thus the same shared RwLock) must still
        // succeed.
        let (status2, _) = request(addr, "/");
        assert_eq!(status2, 200);
    }

    #[derive(Debug)]
    struct AlwaysFailsToSerialize;
    impl serde::Serialize for AlwaysFailsToSerialize {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("simulated serialization failure"))
        }
    }

    #[test]
    fn json_response_returns_a_clean_500_on_a_genuine_serialization_error() {
        let (status, content_type, body) = json_response(&AlwaysFailsToSerialize);
        assert_eq!(status, 500);
        assert_eq!(content_type, "text/plain");
        assert_eq!(body, b"failed to serialize snapshot");
    }
}
