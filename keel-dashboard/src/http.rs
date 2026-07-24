use crate::snapshot::Snapshot;
use rustls::{ServerConnection, StreamOwned};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

pub fn run(
    listener: TcpListener,
    tls_config: Arc<rustls::ServerConfig>,
    snapshot: Arc<RwLock<Snapshot>>,
    basic_auth_user: String,
    basic_auth_password: String,
) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let tls_config = Arc::clone(&tls_config);
        let snapshot = Arc::clone(&snapshot);
        let basic_auth_user = basic_auth_user.clone();
        let basic_auth_password = basic_auth_password.clone();
        thread::spawn(move || {
            let Ok(conn) = ServerConnection::new(tls_config) else { return };
            let mut tls_stream = TlsStream::new(conn, stream);
            let _ = handle_connection(&mut tls_stream, &snapshot, &basic_auth_user, &basic_auth_password);
        });
    }
}

struct ParsedRequest {
    method: String,
    path: String,
    authorization: Option<String>,
}

fn handle_connection(
    stream: &mut TlsStream,
    snapshot: &Arc<RwLock<Snapshot>>,
    basic_auth_user: &str,
    basic_auth_password: &str,
) -> io::Result<()> {
    let request = match read_request(stream)? {
        Some(r) => r,
        None => return Ok(()),
    };
    if !crate::basic_auth::check(request.authorization.as_deref(), basic_auth_user, basic_auth_password) {
        return write_response(stream, 401, "text/plain", b"unauthorized");
    }
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
                let authorization = req
                    .headers
                    .iter()
                    .find(|h| h.name.eq_ignore_ascii_case("authorization"))
                    .map(|h| String::from_utf8_lossy(h.value).to_string());
                return Ok(Some(ParsedRequest { method, path, authorization }));
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
        401 => "Unauthorized",
        404 => "Not Found",
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
            let body = serde_json::to_vec(&*snapshot).expect("Snapshot serialization should not fail");
            (200, "application/json", body)
        }
        _ => (404, "text/plain", b"not found".to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::Snapshot;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

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
        std::thread::spawn(move || run(listener, tls_config, snapshot, "admin".to_string(), "hunter2".to_string()));
        addr
    }

    fn request(addr: std::net::SocketAddr, path: &str, auth_header: Option<&str>) -> (u16, String) {
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
        let auth = auth_header.map(|h| format!("Authorization: {h}\r\n")).unwrap_or_default();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Length: 0\r\n\r\n");
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
    fn a_request_with_no_auth_header_is_rejected() {
        let addr = start_test_server(Snapshot::default());
        let (status, _) = request(addr, "/", None);
        assert_eq!(status, 401);
    }

    #[test]
    fn a_request_with_wrong_credentials_is_rejected() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:wrongpassword"));
        let (status, _) = request(addr, "/", Some(&header));
        assert_eq!(status, 401);
    }

    #[test]
    fn a_request_with_correct_credentials_gets_the_dashboard_html() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:hunter2"));
        let (status, body) = request(addr, "/", Some(&header));
        assert_eq!(status, 200);
        assert!(body.contains("keel dashboard"), "got: {body}");
    }

    #[test]
    fn api_snapshot_returns_json() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:hunter2"));
        let (status, body) = request(addr, "/api/snapshot", Some(&header));
        assert_eq!(status, 200);
        assert!(body.contains("\"nodes\""), "got: {body}");
    }

    #[test]
    fn an_unknown_path_is_404() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:hunter2"));
        let (status, _) = request(addr, "/nope", Some(&header));
        assert_eq!(status, 404);
    }
}
