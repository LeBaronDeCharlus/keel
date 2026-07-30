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
        return Err(format!("no certificates found in {}", path.display()));
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
    fn load_client_config_fails_on_a_missing_key_file() {
        let err = load_client_config(
            &fixture("fixture-client.crt"),
            &fixture("does-not-exist.key"),
            &fixture("ca.crt"),
            &fixture("crl.pem"),
        )
        .unwrap_err();
        assert!(err.contains("does-not-exist.key"), "got: {err}");
    }

    #[test]
    fn load_client_config_fails_on_a_missing_crl_file() {
        let err = load_client_config(
            &fixture("fixture-client.crt"),
            &fixture("fixture-client.key"),
            &fixture("ca.crt"),
            &fixture("does-not-exist-crl.pem"),
        )
        .unwrap_err();
        assert!(err.contains("does-not-exist-crl.pem"), "got: {err}");
    }

    #[test]
    fn load_client_config_fails_on_a_malformed_crl_file() {
        let bad_crl =
            std::env::temp_dir().join(format!("keelctl-tls-test-bad-crl-{}", std::process::id()));
        std::fs::write(&bad_crl, "not a crl").unwrap();
        let err = load_client_config(
            &fixture("fixture-client.crt"),
            &fixture("fixture-client.key"),
            &fixture("ca.crt"),
            &bad_crl,
        )
        .unwrap_err();
        assert!(err.contains("failed to"), "got: {err}");
    }

    #[test]
    fn server_name_from_addr_parses_the_host_and_drops_the_port() {
        let name = server_name_from_addr("10.0.0.1:7620").unwrap();
        assert_eq!(
            name,
            rustls::pki_types::ServerName::IpAddress(std::net::Ipv4Addr::new(10, 0, 0, 1).into())
        );
    }
}
