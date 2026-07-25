use crate::acme::{AcmeClient, AcmeError, Cert};
use crate::dns::DnsProvider;
use instant_acme::{
    Account, AccountCredentials, ChallengeType, Identifier, NewAccount, NewOrder, OrderStatus, RetryPolicy,
};
use std::path::PathBuf;
use std::time::Duration;

/// How long `poll_ready`/`poll_certificate` retry before giving up. Longer
/// than `RetryPolicy::new()`'s 5s default: a real DNS-01 challenge involves
/// real DNS propagation (`DnsProvider::wait_for_propagation` has already
/// completed by the time we call `poll_ready`, but Let's Encrypt's own
/// re-check of the TXT record over the public DNS system, plus its own
/// validation queueing, can still take longer than 5s in practice).
const POLL_TIMEOUT: Duration = Duration::from_secs(90);

pub struct InstantAcmeClient {
    directory_url: String,
    account_key_path: PathBuf,
    runtime: tokio::runtime::Runtime,
}

/// Distinguishes an ACME server's `urn:ietf:params:acme:error:rateLimited`
/// response (RFC 8555 section 6.7) from every other kind of ACME failure,
/// which previously all collapsed into the same generic `AcmeError::Request`
/// regardless of whether the CA was rate-limiting us or the network just
/// hiccuped.
fn convert_acme_error(e: instant_acme::Error) -> AcmeError {
    if let instant_acme::Error::Api(problem) = &e {
        if problem.r#type.as_deref() == Some("urn:ietf:params:acme:error:rateLimited") {
            return AcmeError::RateLimited(e.to_string());
        }
    }
    AcmeError::Request(e.to_string())
}

impl InstantAcmeClient {
    pub fn new(directory_url: String, account_key_path: PathBuf) -> Result<Self, AcmeError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| AcmeError::Request(e.to_string()))?;
        Ok(Self { directory_url, account_key_path, runtime })
    }
}

impl AcmeClient for InstantAcmeClient {
    fn request_certificate(&self, domain: &str, contact_email: &str, dns: &dyn DnsProvider) -> Result<Cert, AcmeError> {
        self.runtime.block_on(self.request_certificate_async(domain, contact_email, dns))
    }
}

impl InstantAcmeClient {
    /// Loads the persisted ACME account from `self.account_key_path` if it
    /// exists, or creates a new one and persists it (temp-file-then-rename,
    /// matching every other piece of state this project persists) if not -
    /// so a `keel-agentd` restart reuses the same account rather than
    /// registering a fresh one with the ACME server on every run.
    async fn load_or_create_account(&self, contact_email: &str) -> Result<Account, AcmeError> {
        if let Ok(existing) = std::fs::read_to_string(&self.account_key_path) {
            let credentials: AccountCredentials =
                serde_json::from_str(&existing).map_err(|e| AcmeError::Request(format!("malformed account credentials: {e}")))?;
            let account = Account::builder()
                .map_err(convert_acme_error)?
                .from_credentials(credentials)
                .await
                .map_err(convert_acme_error)?;
            return Ok(account);
        }

        let contact = format!("mailto:{contact_email}");
        let new_account = NewAccount { contact: &[&contact], terms_of_service_agreed: true, only_return_existing: false };
        let (account, credentials) = Account::builder()
            .map_err(convert_acme_error)?
            .create(&new_account, self.directory_url.clone(), None)
            .await
            .map_err(convert_acme_error)?;

        let serialized =
            serde_json::to_string(&credentials).map_err(|e| AcmeError::Request(format!("failed to serialize account credentials: {e}")))?;
        persist_account_credentials(&self.account_key_path, &serialized)?;

        Ok(account)
    }

    async fn request_certificate_async(&self, domain: &str, contact_email: &str, dns: &dyn DnsProvider) -> Result<Cert, AcmeError> {
        let account = self.load_or_create_account(contact_email).await?;

        let identifier = Identifier::Dns(domain.to_string());
        let mut order = account.new_order(&NewOrder::new(&[identifier])).await.map_err(convert_acme_error)?;

        let challenge_name = format!("_acme-challenge.{domain}");
        let mut dns_values = Vec::new();

        // Every `?` inside this block used to be a `?` directly inside
        // `request_certificate_async` itself, returning before the cleanup
        // below was ever reached: a DNS-provider error or propagation
        // timeout on, say, the second of two authorizations left the
        // first's real TXT record permanently orphaned on the zone. Capturing
        // the whole authorization phase's result here first, the same
        // early-return-capture idiom `keel-controlplane`'s `worker.rs` uses
        // for its own command handlers, means an early failure still falls
        // through to the unconditional cleanup afterward.
        let authorization_result: Result<(), AcmeError> = async {
            let mut authorizations = order.authorizations();
            while let Some(result) = authorizations.next().await {
                let mut authorization = result.map_err(convert_acme_error)?;
                let mut challenge = authorization
                    .challenge(ChallengeType::Dns01)
                    .ok_or_else(|| AcmeError::Request(format!("no DNS-01 challenge offered for '{domain}'")))?;
                let dns_value = challenge.key_authorization().dns_value();
                dns.create_txt_record(&challenge_name, &dns_value)?;
                dns_values.push(dns_value.clone());
                dns.wait_for_propagation(&challenge_name, &dns_value)?;
                challenge.set_ready().await.map_err(convert_acme_error)?;
            }
            Ok(())
        }
        .await;

        let issuance_result = match authorization_result {
            Ok(()) => self.finalize_and_download(&mut order).await,
            Err(e) => Err(e),
        };

        // Clean up the TXT record this order created, regardless of
        // whether issuance succeeded - a failed order must not leave a
        // stale challenge record sitting on the zone forever.
        if !dns_values.is_empty() {
            let _ = dns.delete_txt_record(&challenge_name);
        }

        issuance_result
    }

    async fn finalize_and_download(&self, order: &mut instant_acme::Order) -> Result<Cert, AcmeError> {
        let retry_policy = RetryPolicy::new().timeout(POLL_TIMEOUT);
        let status = order.poll_ready(&retry_policy).await.map_err(convert_acme_error)?;
        if status != OrderStatus::Ready {
            return Err(AcmeError::Request(format!("order did not become ready, status: {status:?}")));
        }

        let key_pem = order.finalize().await.map_err(convert_acme_error)?;
        let cert_pem = order.poll_certificate(&retry_policy).await.map_err(convert_acme_error)?;

        Ok(Cert { cert_pem, key_pem })
    }
}

/// Persists the ACME account credentials (an account private key plus its
/// server-issued `kid`) to `account_key_path` via temp-file-then-rename,
/// owner-only permissions applied before the rename so the credentials are
/// never briefly world-readable under their final name.
fn persist_account_credentials(account_key_path: &std::path::Path, serialized: &str) -> Result<(), AcmeError> {
    use std::os::unix::fs::PermissionsExt;

    if let Some(parent) = account_key_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| AcmeError::Request(e.to_string()))?;
    }
    let tmp_path = account_key_path.with_extension("tmp");
    std::fs::write(&tmp_path, serialized).map_err(|e| AcmeError::Request(e.to_string()))?;
    std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600)).map_err(|e| AcmeError::Request(e.to_string()))?;
    std::fs::rename(&tmp_path, account_key_path).map_err(|e| AcmeError::Request(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn test_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-ingress-acme-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("acme-account.json")
    }

    #[test]
    fn persist_account_credentials_writes_owner_only_permissions() {
        let path = test_path("owner-only-perms");

        persist_account_credentials(&path, r#"{"kid":"https://example.com/acct/1"}"#).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "persisted ACME account credentials must not be readable by group/other");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), r#"{"kid":"https://example.com/acct/1"}"#);
    }

    #[test]
    fn persist_account_credentials_creates_missing_parent_directories() {
        let path = test_path("missing-parent").parent().unwrap().join("nested").join("acme-account.json");

        persist_account_credentials(&path, "{}").unwrap();

        assert!(path.is_file());
    }

    #[test]
    fn convert_acme_error_recognizes_a_rate_limited_problem() {
        let problem = instant_acme::Problem {
            r#type: Some("urn:ietf:params:acme:error:rateLimited".to_string()),
            detail: Some("too many certificates already issued".to_string()),
            status: Some(429),
            subproblems: vec![],
        };
        assert!(matches!(convert_acme_error(problem.into()), AcmeError::RateLimited(_)));
    }

    #[test]
    fn convert_acme_error_treats_every_other_problem_type_as_a_generic_request_failure() {
        let problem = instant_acme::Problem {
            r#type: Some("urn:ietf:params:acme:error:malformed".to_string()),
            detail: Some("invalid identifier".to_string()),
            status: Some(400),
            subproblems: vec![],
        };
        assert!(matches!(convert_acme_error(problem.into()), AcmeError::Request(_)));
    }
}
