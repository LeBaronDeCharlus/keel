use keel_spec::IngressSpec;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngressRecord {
    pub spec: IngressSpec,
    pub cert_expires_at_unix: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_spec::{IngressBackend, IngressSpecBody, IngressTls, Metadata};

    fn sample_spec(name: &str) -> IngressSpec {
        IngressSpec {
            api_version: "keel/v1".to_string(),
            kind: "Ingress".to_string(),
            metadata: Metadata {
                name: name.to_string(),
            },
            spec: IngressSpecBody {
                host: "example.com".to_string(),
                backend: IngressBackend {
                    service: "hugo-site".to_string(),
                    port: 8080,
                },
                tls: IngressTls {
                    email: "admin@example.com".to_string(),
                },
            },
        }
    }

    #[test]
    fn ingress_record_round_trips_through_yaml() {
        let record = IngressRecord {
            spec: sample_spec("blog"),
            cert_expires_at_unix: Some(1_800_000_000),
        };
        let yaml = serde_yaml::to_string(&record).unwrap();
        let parsed: IngressRecord = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed, record);
    }

    #[test]
    fn a_fresh_record_has_no_certificate_expiry() {
        let record = IngressRecord {
            spec: sample_spec("blog"),
            cert_expires_at_unix: None,
        };
        assert_eq!(record.cert_expires_at_unix, None);
    }
}
