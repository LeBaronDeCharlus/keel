use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum DnsError {
    #[error("TXT record '{0}' not found")]
    NotFound(String),
    #[error("DNS provider request failed: {0}")]
    Request(String),
    #[error("timed out waiting for '{0}' to propagate")]
    PropagationTimeout(String),
}

pub trait DnsProvider {
    fn create_txt_record(&self, name: &str, value: &str) -> Result<(), DnsError>;
    fn delete_txt_record(&self, name: &str) -> Result<(), DnsError>;
    fn wait_for_propagation(&self, name: &str, value: &str) -> Result<(), DnsError>;
}

#[derive(Default)]
pub struct FakeDnsProvider {
    records: Mutex<HashMap<String, String>>,
    fail_create: Mutex<bool>,
}

impl FakeDnsProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_fail_create(&self, fail: bool) {
        *self.fail_create.lock().unwrap() = fail;
    }
}

impl DnsProvider for FakeDnsProvider {
    fn create_txt_record(&self, name: &str, value: &str) -> Result<(), DnsError> {
        if *self.fail_create.lock().unwrap() {
            return Err(DnsError::Request(format!(
                "simulated failure creating '{name}'"
            )));
        }
        self.records
            .lock()
            .unwrap()
            .insert(name.to_string(), value.to_string());
        Ok(())
    }

    fn delete_txt_record(&self, name: &str) -> Result<(), DnsError> {
        self.records.lock().unwrap().remove(name);
        Ok(())
    }

    fn wait_for_propagation(&self, name: &str, value: &str) -> Result<(), DnsError> {
        match self.records.lock().unwrap().get(name) {
            Some(v) if v == value => Ok(()),
            Some(_) | None => Err(DnsError::NotFound(name.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_wait_for_propagation_succeeds() {
        let dns = FakeDnsProvider::new();
        dns.create_txt_record("_acme-challenge.example.com", "token-value")
            .unwrap();
        assert!(dns
            .wait_for_propagation("_acme-challenge.example.com", "token-value")
            .is_ok());
    }

    #[test]
    fn wait_for_propagation_fails_before_create() {
        let dns = FakeDnsProvider::new();
        assert_eq!(
            dns.wait_for_propagation("_acme-challenge.example.com", "token-value"),
            Err(DnsError::NotFound(
                "_acme-challenge.example.com".to_string()
            ))
        );
    }

    #[test]
    fn delete_then_wait_for_propagation_fails() {
        let dns = FakeDnsProvider::new();
        dns.create_txt_record("_acme-challenge.example.com", "token-value")
            .unwrap();
        dns.delete_txt_record("_acme-challenge.example.com")
            .unwrap();
        assert!(dns
            .wait_for_propagation("_acme-challenge.example.com", "token-value")
            .is_err());
    }

    #[test]
    fn create_txt_record_can_be_made_to_fail_for_retry_tests() {
        let dns = FakeDnsProvider::new();
        dns.set_fail_create(true);
        assert!(dns
            .create_txt_record("_acme-challenge.example.com", "token-value")
            .is_err());
    }
}
