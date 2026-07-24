use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName};
use rustls::RootCertStore;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, Once};

static CRYPTO_PROVIDER_INIT: Once = Once::new();

pub fn ensure_crypto_provider() {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// mTLS client config for talking to `keel-controlplane`, identical in
/// shape and behavior to `keelctl::tls::load_client_config` - this project
/// duplicates TLS-loading code per binary rather than sharing it through a
/// library (see `keelctl/src/tls.rs` vs. `keel-controlplane/src/tls.rs`).
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

/// Server config for the browser-facing listener: a bare cert/key with no
/// client-certificate verification, since browsers never present one.
/// Deliberately distinct from `keel_controlplane::tls::load_server_config`,
/// which hardcodes a `WebPkiClientVerifier` for mTLS between cluster
/// components and would reject every browser connection outright.
pub fn load_browser_server_config(cert_path: &Path, key_path: &Path) -> Result<rustls::ServerConfig, String> {
    ensure_crypto_provider();
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("failed to build TLS server config: {e}"))
}

pub fn server_name_from_addr(addr: &str) -> Result<ServerName<'static>, String> {
    let host = addr.rsplit_once(':').map(|(host, _port)| host).unwrap_or(addr);
    let ip: std::net::IpAddr =
        host.parse().map_err(|e| format!("expected an IP address in '{addr}', got '{host}': {e}"))?;
    Ok(ServerName::IpAddress(ip.into()))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open certificate file {}: {e}", path.display()))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse certificate file {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("failed to find any PEM-encoded certificates in {}", path.display()));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open key file {}: {e}", path.display()))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| format!("failed to parse key file {}: {e}", path.display()))?
        .ok_or_else(|| format!("no private key found in {}", path.display()))
}

fn load_root_store(ca_path: &Path) -> Result<RootCertStore, String> {
    let certs = load_certs(ca_path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|e| format!("failed to add CA certificate from {}: {e}", ca_path.display()))?;
    }
    Ok(roots)
}

fn load_crls(path: &Path) -> Result<Vec<CertificateRevocationListDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open CRL file {}: {e}", path.display()))?;
    let crls: Vec<_> = rustls_pemfile::crls(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse CRL file {}: {e}", path.display()))?;
    if crls.is_empty() {
        return Err(format!("failed to find a PEM-encoded CRL in {}", path.display()));
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
    fn load_client_config_succeeds_with_valid_fixtures() {
        load_client_config(&fixture("fixture-client.crt"), &fixture("fixture-client.key"), &fixture("ca.crt"), &fixture("crl.pem"))
            .expect("expected a valid client config");
    }

    #[test]
    fn load_client_config_fails_on_a_missing_cert_file() {
        let err = load_client_config(&fixture("does-not-exist.crt"), &fixture("fixture-client.key"), &fixture("ca.crt"), &fixture("crl.pem"))
            .unwrap_err();
        assert!(err.contains("does-not-exist.crt"), "got: {err}");
    }

    #[test]
    fn load_browser_server_config_succeeds_with_a_bare_cert_and_key_and_no_ca() {
        load_browser_server_config(&fixture("fixture-node.crt"), &fixture("fixture-node.key"))
            .expect("expected a valid server config with no client-cert verification");
    }

    #[test]
    fn load_browser_server_config_fails_on_a_missing_key_file() {
        let err = load_browser_server_config(&fixture("fixture-node.crt"), &fixture("does-not-exist.key")).unwrap_err();
        assert!(err.contains("does-not-exist.key"), "got: {err}");
    }

    #[test]
    fn server_name_from_addr_parses_the_host_and_drops_the_port() {
        let name = server_name_from_addr("192.168.64.4:7621").unwrap();
        assert_eq!(name, rustls::pki_types::ServerName::IpAddress(std::net::Ipv4Addr::new(192, 168, 64, 4).into()));
    }

    #[test]
    fn server_name_from_addr_rejects_a_non_ip_host() {
        assert!(server_name_from_addr("not-an-ip:7620").is_err());
    }
}
