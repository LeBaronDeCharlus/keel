use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::{Arc, Once};
use std::thread;
use std::time::Duration;

static CRYPTO_PROVIDER_INIT: Once = Once::new();

pub fn ensure_crypto_provider() {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        // Ignore the error: it only occurs if some other crate (e.g. another
        // `keel-*` crate linked into the same process, as happens in
        // `keelctl`'s integration tests) already installed a default
        // provider first. Either way, a process-wide default is now in
        // place, which is all this function promises.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
    crl_path: &Path,
) -> Result<rustls::ServerConfig, String> {
    ensure_crypto_provider();
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let roots = load_root_store(ca_path)?;
    let crls = load_crls(crl_path)?;
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .with_crls(crls)
        .build()
        .map_err(|e| format!("failed to build client certificate verifier: {e}"))?;
    rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| format!("failed to build TLS server config: {e}"))
}

pub fn load_client_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
    crl_path: &Path,
) -> Result<rustls::ClientConfig, String> {
    ensure_crypto_provider();
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let roots = load_root_store(ca_path)?;
    let crls = load_crls(crl_path)?;
    let server_verifier = WebPkiServerVerifier::builder(Arc::new(roots))
        .with_crls(crls)
        .build()
        .map_err(|e| format!("failed to build server certificate verifier: {e}"))?;
    rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(server_verifier)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("failed to build TLS client config: {e}"))
}

pub struct ReloadingTls {
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: PathBuf,
    crl_path: PathBuf,
    server: RwLock<Arc<rustls::ServerConfig>>,
    client: RwLock<Arc<rustls::ClientConfig>>,
}

impl ReloadingTls {
    pub fn spawn(
        cert_path: PathBuf,
        key_path: PathBuf,
        ca_path: PathBuf,
        crl_path: PathBuf,
        reload_interval: Duration,
    ) -> Result<Arc<Self>, String> {
        let server = load_server_config(&cert_path, &key_path, &ca_path, &crl_path)?;
        let client = load_client_config(&cert_path, &key_path, &ca_path, &crl_path)?;
        let this = Arc::new(Self {
            cert_path,
            key_path,
            ca_path,
            crl_path,
            server: RwLock::new(Arc::new(server)),
            client: RwLock::new(Arc::new(client)),
        });
        let reload_target = Arc::clone(&this);
        thread::spawn(move || loop {
            thread::sleep(reload_interval);
            reload_target.reload_once();
        });
        Ok(this)
    }

    fn reload_once(&self) {
        match load_server_config(
            &self.cert_path,
            &self.key_path,
            &self.ca_path,
            &self.crl_path,
        ) {
            Ok(cfg) => *self.server.write().unwrap() = Arc::new(cfg),
            Err(e) => eprintln!("keel-controlplane: TLS reload failed (server config): {e}"),
        }
        match load_client_config(
            &self.cert_path,
            &self.key_path,
            &self.ca_path,
            &self.crl_path,
        ) {
            Ok(cfg) => *self.client.write().unwrap() = Arc::new(cfg),
            Err(e) => eprintln!("keel-controlplane: TLS reload failed (client config): {e}"),
        }
    }

    pub fn server_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.server.read().unwrap())
    }

    pub fn client_config(&self) -> Arc<rustls::ClientConfig> {
        Arc::clone(&self.client.read().unwrap())
    }
}

pub fn server_name_from_addr(addr: &str) -> Result<ServerName<'static>, String> {
    let host = addr
        .rsplit_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(addr);
    let ip: std::net::IpAddr = host
        .parse()
        .map_err(|e| format!("expected an IP address in '{addr}', got '{host}': {e}"))?;
    Ok(ServerName::IpAddress(ip.into()))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path)
        .map_err(|e| format!("failed to open certificate file {}: {e}", path.display()))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse certificate file {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!(
            "failed to find any PEM-encoded certificates in {}",
            path.display()
        ));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file =
        File::open(path).map_err(|e| format!("failed to open key file {}: {e}", path.display()))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| format!("failed to parse key file {}: {e}", path.display()))?
        .ok_or_else(|| format!("no private key found in {}", path.display()))
}

fn load_root_store(ca_path: &Path) -> Result<RootCertStore, String> {
    let certs = load_certs(ca_path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|e| {
            format!(
                "failed to add CA certificate from {}: {e}",
                ca_path.display()
            )
        })?;
    }
    Ok(roots)
}

fn load_crls(path: &Path) -> Result<Vec<CertificateRevocationListDer<'static>>, String> {
    let file =
        File::open(path).map_err(|e| format!("failed to open CRL file {}: {e}", path.display()))?;
    let crls: Vec<_> = rustls_pemfile::crls(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse CRL file {}: {e}", path.display()))?;
    if crls.is_empty() {
        return Err(format!(
            "failed to find a PEM-encoded CRL in {}",
            path.display()
        ));
    }
    Ok(crls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    #[test]
    fn load_server_config_succeeds_with_valid_fixtures() {
        load_server_config(
            &fixture("fixture-node.crt"),
            &fixture("fixture-node.key"),
            &fixture("ca.crt"),
            &fixture("crl.pem"),
        )
        .expect("expected a valid server config");
    }

    #[test]
    fn load_server_config_fails_on_a_missing_cert_file() {
        let err = load_server_config(
            &fixture("does-not-exist.crt"),
            &fixture("fixture-node.key"),
            &fixture("ca.crt"),
            &fixture("crl.pem"),
        )
        .unwrap_err();
        assert!(err.contains("does-not-exist.crt"), "got: {err}");
    }

    #[test]
    fn load_server_config_fails_on_a_missing_crl_file() {
        let err = load_server_config(
            &fixture("fixture-node.crt"),
            &fixture("fixture-node.key"),
            &fixture("ca.crt"),
            &fixture("does-not-exist-crl.pem"),
        )
        .unwrap_err();
        assert!(err.contains("does-not-exist-crl.pem"), "got: {err}");
    }

    #[test]
    fn load_server_config_fails_on_a_malformed_crl_file() {
        let bad_crl = std::env::temp_dir().join(format!(
            "keel-controlplane-tls-test-bad-crl-{}",
            std::process::id()
        ));
        std::fs::write(&bad_crl, "not a crl").unwrap();
        let err = load_server_config(
            &fixture("fixture-node.crt"),
            &fixture("fixture-node.key"),
            &fixture("ca.crt"),
            &bad_crl,
        )
        .unwrap_err();
        assert!(err.contains("failed to"), "got: {err}");
    }

    #[test]
    fn load_client_config_succeeds_with_valid_fixtures() {
        load_client_config(
            &fixture("fixture-client.crt"),
            &fixture("fixture-client.key"),
            &fixture("ca.crt"),
            &fixture("crl.pem"),
        )
        .expect("expected a valid client config");
    }

    #[test]
    fn load_client_config_fails_on_a_malformed_ca_file() {
        let bad_ca = std::env::temp_dir().join(format!(
            "keel-controlplane-tls-test-bad-ca-{}",
            std::process::id()
        ));
        std::fs::write(&bad_ca, "not a certificate").unwrap();
        let err = load_client_config(
            &fixture("fixture-client.crt"),
            &fixture("fixture-client.key"),
            &bad_ca,
            &fixture("crl.pem"),
        )
        .unwrap_err();
        assert!(err.contains("failed to"), "got: {err}");
    }

    #[test]
    fn server_name_from_addr_parses_the_host_and_drops_the_port() {
        let name = server_name_from_addr("192.168.64.4:7621").unwrap();
        assert_eq!(
            name,
            rustls::pki_types::ServerName::IpAddress(
                std::net::Ipv4Addr::new(192, 168, 64, 4).into()
            )
        );
    }

    #[test]
    fn server_name_from_addr_rejects_a_non_ip_host() {
        assert!(server_name_from_addr("not-an-ip:7620").is_err());
    }
}
