use crate::dns::DnsProvider;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct Cert {
    pub cert_pem: String,
    pub key_pem: String,
}

impl Cert {
    /// Parses the real `notAfter` out of the issued certificate itself,
    /// as a Unix timestamp -- previously `reconcile_certs` hardcoded a
    /// placeholder 90-day validity instead, which could silently drift
    /// from whatever the CA actually issued.
    pub fn expires_at_unix(&self) -> Result<i64, AcmeError> {
        let (_, pem) = x509_parser::pem::parse_x509_pem(self.cert_pem.as_bytes())
            .map_err(|e| AcmeError::Request(format!("failed to parse certificate PEM: {e}")))?;
        let cert = pem.parse_x509().map_err(|e| AcmeError::Request(format!("failed to parse certificate: {e}")))?;
        Ok(cert.validity().not_after.timestamp())
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum AcmeError {
    #[error("DNS-01 challenge failed: {0}")]
    Dns(#[from] crate::dns::DnsError),
    #[error("ACME request failed: {0}")]
    Request(String),
    /// The ACME server responded with `urn:ietf:params:acme:error:rateLimited`
    /// (RFC 8555 section 6.7) -- distinct from `Request` so a caller (or a
    /// future backoff policy) can tell "the CA is rate-limiting us" apart
    /// from an ordinary transient network error, which previously
    /// collapsed into the exact same generic variant.
    #[error("ACME server rate limit exceeded: {0}")]
    RateLimited(String),
}

pub trait AcmeClient {
    fn request_certificate(&self, domain: &str, contact_email: &str, dns: &dyn DnsProvider) -> Result<Cert, AcmeError>;
}

/// The synthetic clock `FakeAcmeClient` issues certificates against.
/// Matters that this isn't arbitrary: `keel-agentd`'s reconciler tests all
/// simulate "now" starting at this same instant (`1_800_000_000`), and a
/// freshly issued fake certificate must look safely unexpired relative to
/// that "now" -- an earlier synthetic epoch would make every fake cert look
/// already due for renewal the moment it's issued.
const FAKE_ISSUANCE_EPOCH_UNIX: i64 = 1_800_000_000;

#[derive(Default)]
pub struct FakeAcmeClient {
    fail: std::sync::Mutex<bool>,
    // A synthetic, deterministically-advancing clock rather than the real
    // one: two issuances a real CA would space days apart can happen
    // microseconds apart in a fast test, which at 1-second timestamp
    // resolution risks two "different" certificates ending up with the
    // exact same notAfter and breaking any test asserting renewal strictly
    // advances the expiry.
    issuance_count: std::sync::atomic::AtomicI64,
}

impl FakeAcmeClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_fail(&self, fail: bool) {
        *self.fail.lock().unwrap() = fail;
    }
}

impl AcmeClient for FakeAcmeClient {
    fn request_certificate(&self, domain: &str, _contact_email: &str, dns: &dyn DnsProvider) -> Result<Cert, AcmeError> {
        if *self.fail.lock().unwrap() {
            return Err(AcmeError::Request(format!("simulated ACME failure for '{domain}'")));
        }
        let challenge_name = format!("_acme-challenge.{domain}");
        dns.create_txt_record(&challenge_name, "fake-token")?;
        dns.wait_for_propagation(&challenge_name, "fake-token")?;
        dns.delete_txt_record(&challenge_name)?;
        // A real, self-signed certificate (not a placeholder string) so
        // that callers exercising the real `Cert::expires_at_unix` parsing
        // path -- exactly what `reconcile_certs` does -- get a genuine
        // notAfter rather than a parse failure every time. 90 days matches
        // a typical Let's Encrypt issuance; each successive issuance is
        // pinned a full day later than the last on the synthetic clock
        // above, so consecutive fake issuances are always distinguishable.
        let n = self.issuance_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let not_before = time::OffsetDateTime::from_unix_timestamp(FAKE_ISSUANCE_EPOCH_UNIX).unwrap() + time::Duration::days(n);
        let key_pair = rcgen::KeyPair::generate().expect("key generation should not fail");
        let mut params = rcgen::CertificateParams::new(vec![domain.to_string()]).expect("valid SAN should not fail");
        params.not_before = not_before;
        params.not_after = not_before + time::Duration::days(90);
        let cert = params.self_signed(&key_pair).expect("self-signing should not fail");
        Ok(Cert { cert_pem: cert.pem(), key_pem: key_pair.serialize_pem() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dns::FakeDnsProvider;

    /// Generates a real, self-signed certificate valid from `not_before`
    /// (Unix seconds) until `not_after` (Unix seconds) -- used to prove
    /// `expires_at_unix` parses a genuine certificate's real `notAfter`,
    /// not just a placeholder string.
    fn cert_with_expiry(not_before: i64, not_after: i64) -> String {
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["example.com".to_string()]).unwrap();
        params.not_before = time::OffsetDateTime::from_unix_timestamp(not_before).unwrap();
        params.not_after = time::OffsetDateTime::from_unix_timestamp(not_after).unwrap();
        params.self_signed(&key_pair).unwrap().pem()
    }

    #[test]
    fn expires_at_unix_parses_the_real_certificates_not_after() {
        let not_after = 1_900_000_000;
        let cert = Cert { cert_pem: cert_with_expiry(1_800_000_000, not_after), key_pem: String::new() };
        assert_eq!(cert.expires_at_unix().unwrap(), not_after);
    }

    #[test]
    fn expires_at_unix_on_unparseable_pem_returns_an_error() {
        let cert = Cert { cert_pem: "not a real certificate".to_string(), key_pem: String::new() };
        assert!(cert.expires_at_unix().is_err());
    }

    #[test]
    fn request_certificate_succeeds_and_drives_the_dns_challenge() {
        let dns = FakeDnsProvider::new();
        let acme = FakeAcmeClient::new();
        let cert = acme.request_certificate("example.com", "admin@example.com", &dns).unwrap();
        // A real, parseable certificate now, not a placeholder string -- a
        // literal substring match against the domain name isn't meaningful
        // against real base64-encoded DER, so assert on real properties
        // instead: it parses, and its notAfter is 90 days past the fake
        // client's synthetic first-issuance instant.
        let expires_at = cert.expires_at_unix().expect("expected a real, parseable certificate");
        assert_eq!(expires_at, FAKE_ISSUANCE_EPOCH_UNIX + 90 * 24 * 60 * 60);
        // The challenge record must be cleaned up by the time the cert comes back.
        assert!(dns.wait_for_propagation("_acme-challenge.example.com", "fake-token").is_err());
    }

    #[test]
    fn request_certificate_can_be_made_to_fail_for_backoff_tests() {
        let dns = FakeDnsProvider::new();
        let acme = FakeAcmeClient::new();
        acme.set_fail(true);
        assert!(acme.request_certificate("example.com", "admin@example.com", &dns).is_err());
    }

    #[test]
    fn request_certificate_surfaces_a_dns_provider_failure() {
        let dns = FakeDnsProvider::new();
        dns.set_fail_create(true);
        let acme = FakeAcmeClient::new();
        assert!(matches!(
            acme.request_certificate("example.com", "admin@example.com", &dns),
            Err(AcmeError::Dns(_))
        ));
    }
}
