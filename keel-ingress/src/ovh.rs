use crate::dns::{DnsError, DnsProvider};
use sha1::{Digest, Sha1};

pub struct OvhDnsProvider {
    app_key: String,
    app_secret: String,
    consumer_key: String,
    zone: String,
    endpoint: String,
}

impl OvhDnsProvider {
    pub fn new(app_key: String, app_secret: String, consumer_key: String, zone: String) -> Self {
        Self {
            app_key,
            app_secret,
            consumer_key,
            zone,
            endpoint: "https://eu.api.ovh.com/1.0".to_string(),
        }
    }

    fn sign(&self, method: &str, url: &str, body: &str, timestamp: u64) -> String {
        let to_hash = format!(
            "{}+{}+{}+{}+{}+{}",
            self.app_secret, self.consumer_key, method, url, body, timestamp
        );
        let mut hasher = Sha1::new();
        hasher.update(to_hash.as_bytes());
        format!("$1${:x}", hasher.finalize())
    }

    /// Sends a signed request to the OVH API and returns the raw response body as a string.
    ///
    /// `ureq` 3.3.0 has no `agent.request(method, url)` builder that accepts a dynamic HTTP
    /// method: `Agent` only exposes fixed-verb helpers (`get`, `post`, `put`, `delete`, ...), each
    /// of which pins the request into a `WithBody`/`WithoutBody` typestate at compile time. Since
    /// this method needs to support GET, POST and DELETE from a runtime `&str`, we instead build a
    /// plain `http::Request` (the `http` crate, re-exported as `ureq::http`) and hand it to
    /// `Agent::run`, which accepts any method. The response is read via `Response::into_body()`
    /// (from the `http` crate) followed by `Body::read_to_string()` (`ureq`'s own reader, which
    /// takes `&mut self` rather than consuming, but that's fine on the temporary).
    fn request(&self, method: &str, path: &str, body: &str) -> Result<String, DnsError> {
        let url = format!("{}{}", self.endpoint, path);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| DnsError::Request(e.to_string()))?
            .as_secs();
        let signature = self.sign(method, &url, body, timestamp);
        let method = ureq::http::Method::from_bytes(method.as_bytes())
            .map_err(|e| DnsError::Request(e.to_string()))?;
        let request = ureq::http::Request::builder()
            .method(method)
            .uri(&url)
            .header("X-Ovh-Application", &self.app_key)
            .header("X-Ovh-Consumer", &self.consumer_key)
            .header("X-Ovh-Timestamp", timestamp.to_string())
            .header("X-Ovh-Signature", &signature)
            // Note: this always sends body (even if empty) and Content-Type: application/json
            // for all request types, as ureq 3.3.0 doesn't expose a generic .request(method, url)
            // builder. If real OVH API calls (GET/DELETE with no body) fail with ~400, check
            // here first: strict servers may reject empty body + Content-Type: application/json.
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .map_err(|e| DnsError::Request(e.to_string()))?;
        let agent = ureq::Agent::new_with_defaults();
        let mut response = agent
            .run(request)
            .map_err(|e| DnsError::Request(e.to_string()))?;
        response
            .body_mut()
            .read_to_string()
            .map_err(|e| DnsError::Request(e.to_string()))
    }
}

impl DnsProvider for OvhDnsProvider {
    fn create_txt_record(&self, name: &str, value: &str) -> Result<(), DnsError> {
        let sub_domain = name
            .trim_end_matches(&format!(".{}", self.zone))
            .trim_end_matches(&self.zone);
        let body = serde_json::json!({ "fieldType": "TXT", "subDomain": sub_domain, "target": value, "ttl": 60 }).to_string();
        self.request("POST", &format!("/domain/zone/{}/record", self.zone), &body)?;
        self.request("POST", &format!("/domain/zone/{}/refresh", self.zone), "")?;
        Ok(())
    }

    fn delete_txt_record(&self, name: &str) -> Result<(), DnsError> {
        let sub_domain = name
            .trim_end_matches(&format!(".{}", self.zone))
            .trim_end_matches(&self.zone);
        let list_body = self.request(
            "GET",
            &format!(
                "/domain/zone/{}/record?fieldType=TXT&subDomain={sub_domain}",
                self.zone
            ),
            "",
        )?;
        let ids: Vec<u64> =
            serde_json::from_str(&list_body).map_err(|e| DnsError::Request(e.to_string()))?;
        for id in ids {
            self.request(
                "DELETE",
                &format!("/domain/zone/{}/record/{id}", self.zone),
                "",
            )?;
        }
        self.request("POST", &format!("/domain/zone/{}/refresh", self.zone), "")?;
        Ok(())
    }

    fn wait_for_propagation(&self, name: &str, value: &str) -> Result<(), DnsError> {
        const MAX_ATTEMPTS: u32 = 30;
        const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(10);
        // Querying the node's own configured resolver risks a caching (or
        // negative-caching) intermediary reporting this brand-new record
        // absent long after it's actually live on the zone, which then
        // trips the timeout below and triggers the orphaned-TXT-record bug
        // this same DNS-01 flow already has to guard against elsewhere.
        // Querying the zone's own authoritative nameservers directly (via
        // `host`'s trailing `[server]` argument) sidesteps that layer
        // entirely.
        let authoritative = self.authoritative_nameservers()?;
        for _ in 0..MAX_ATTEMPTS {
            for ns in &authoritative {
                let output = std::process::Command::new("host")
                    .args(["-t", "TXT", name, ns])
                    .output()
                    .map_err(|e| DnsError::Request(e.to_string()))?;
                let stdout = String::from_utf8_lossy(&output.stdout);
                if stdout.contains(value) {
                    return Ok(());
                }
            }
            std::thread::sleep(RETRY_DELAY);
        }
        Err(DnsError::PropagationTimeout(name.to_string()))
    }
}

impl OvhDnsProvider {
    /// Resolves the zone's own authoritative nameservers via `host -t NS`,
    /// once per `wait_for_propagation` call (NS records themselves change
    /// far less often than the TXT challenge record being propagated, so
    /// there's no need to re-resolve them on every retry).
    fn authoritative_nameservers(&self) -> Result<Vec<String>, DnsError> {
        let output = std::process::Command::new("host")
            .args(["-t", "NS", &self.zone])
            .output()
            .map_err(|e| DnsError::Request(e.to_string()))?;
        let nameservers = parse_ns_records(&String::from_utf8_lossy(&output.stdout));
        if nameservers.is_empty() {
            return Err(DnsError::Request(format!(
                "could not resolve any authoritative nameserver for zone '{}'",
                self.zone
            )));
        }
        Ok(nameservers)
    }
}

/// Parses `host -t NS`'s stdout (lines like `example.com name server has NS
/// record ns13.ovh.net.`) into bare nameserver hostnames with the trailing
/// root dot stripped, so they're usable directly as `host`'s own `[server]`
/// argument.
fn parse_ns_records(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| line.split_once("NS record ").map(|(_, ns)| ns))
        .map(|ns| ns.trim().trim_end_matches('.').to_string())
        .filter(|ns| !ns.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_produces_the_dollar_one_dollar_prefixed_sha1_hex_digest() {
        let provider = OvhDnsProvider::new(
            "app-key".to_string(),
            "app-secret".to_string(),
            "consumer-key".to_string(),
            "example.com".to_string(),
        );
        let signature = provider.sign(
            "POST",
            "https://eu.api.ovh.com/1.0/domain/zone/example.com/record",
            "{}",
            1_800_000_000,
        );
        assert!(signature.starts_with("$1$"));
        assert_eq!(signature.len(), 3 + 40); // "$1$" + 40 hex chars of a SHA1 digest
    }

    #[test]
    fn sign_is_deterministic_for_the_same_inputs() {
        let provider = OvhDnsProvider::new(
            "app-key".to_string(),
            "app-secret".to_string(),
            "consumer-key".to_string(),
            "example.com".to_string(),
        );
        let a = provider.sign(
            "POST",
            "https://eu.api.ovh.com/1.0/domain/zone/example.com/record",
            "{}",
            1_800_000_000,
        );
        let b = provider.sign(
            "POST",
            "https://eu.api.ovh.com/1.0/domain/zone/example.com/record",
            "{}",
            1_800_000_000,
        );
        assert_eq!(a, b);
    }

    #[test]
    fn sign_changes_when_the_timestamp_changes() {
        let provider = OvhDnsProvider::new(
            "app-key".to_string(),
            "app-secret".to_string(),
            "consumer-key".to_string(),
            "example.com".to_string(),
        );
        let a = provider.sign(
            "POST",
            "https://eu.api.ovh.com/1.0/domain/zone/example.com/record",
            "{}",
            1_800_000_000,
        );
        let b = provider.sign(
            "POST",
            "https://eu.api.ovh.com/1.0/domain/zone/example.com/record",
            "{}",
            1_800_000_001,
        );
        assert_ne!(a, b);
    }

    #[test]
    fn parse_ns_records_extracts_hostnames_and_strips_the_trailing_root_dot() {
        let output = "example.com name server has NS record ns13.ovh.net.\nexample.com name server has NS record dns13.ovh.net.\n";
        assert_eq!(
            parse_ns_records(output),
            vec!["ns13.ovh.net".to_string(), "dns13.ovh.net".to_string()]
        );
    }

    #[test]
    fn parse_ns_records_ignores_unrelated_lines() {
        let output = "Using domain server:\nAddress: 127.0.0.1#53\n\nexample.com name server has NS record ns13.ovh.net.\n";
        assert_eq!(parse_ns_records(output), vec!["ns13.ovh.net".to_string()]);
    }

    #[test]
    fn parse_ns_records_on_empty_output_is_empty() {
        assert_eq!(parse_ns_records(""), Vec::<String>::new());
    }
}
