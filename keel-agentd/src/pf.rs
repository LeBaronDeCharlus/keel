use thiserror::Error;

#[derive(Debug, Error)]
pub enum PfError {
    #[error("pfctl command failed: {0}")]
    Command(String),
}

pub trait PfController {
    fn ensure_redirect_rules(
        &self,
        public_iface: &str,
        ingress_bridge_addr: &str,
    ) -> Result<(), PfError>;
}

#[derive(Default)]
pub struct FakePfController {
    applied: std::sync::Mutex<Vec<(String, String)>>,
    fail: std::sync::Mutex<bool>,
}

impl FakePfController {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_fail(&self, fail: bool) {
        *self.fail.lock().unwrap() = fail;
    }

    pub fn applied_rules(&self) -> Vec<(String, String)> {
        self.applied.lock().unwrap().clone()
    }
}

impl PfController for FakePfController {
    fn ensure_redirect_rules(
        &self,
        public_iface: &str,
        ingress_bridge_addr: &str,
    ) -> Result<(), PfError> {
        if *self.fail.lock().unwrap() {
            return Err(PfError::Command("simulated pfctl failure".to_string()));
        }
        self.applied
            .lock()
            .unwrap()
            .push((public_iface.to_string(), ingress_bridge_addr.to_string()));
        Ok(())
    }
}

pub struct PfctlController;

impl PfctlController {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PfctlController {
    fn default() -> Self {
        Self::new()
    }
}

impl PfController for PfctlController {
    fn ensure_redirect_rules(
        &self,
        public_iface: &str,
        ingress_bridge_addr: &str,
    ) -> Result<(), PfError> {
        let rules = format!(
            "rdr pass on {public_iface} inet proto tcp from any to {public_iface} port 80 -> {ingress_bridge_addr} port 80\nrdr pass on {public_iface} inet proto tcp from any to {public_iface} port 443 -> {ingress_bridge_addr} port 443\n"
        );
        let rules_path = std::path::Path::new("/usr/local/etc/keel/pf-ingress.conf");
        std::fs::create_dir_all(rules_path.parent().unwrap())
            .map_err(|e| PfError::Command(e.to_string()))?;
        std::fs::write(rules_path, &rules).map_err(|e| PfError::Command(e.to_string()))?;
        let output = std::process::Command::new("pfctl")
            .args(["-a", "keel-ingress", "-f"])
            .arg(rules_path)
            .output()
            .map_err(|e| PfError::Command(e.to_string()))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(PfError::Command(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_redirect_rules_records_the_applied_rule() {
        let pf = FakePfController::new();
        pf.ensure_redirect_rules("em0", "10.0.0.2").unwrap();
        assert_eq!(
            pf.applied_rules(),
            vec![("em0".to_string(), "10.0.0.2".to_string())]
        );
    }

    #[test]
    fn ensure_redirect_rules_can_be_made_to_fail_for_retry_tests() {
        let pf = FakePfController::new();
        pf.set_fail(true);
        assert!(pf.ensure_redirect_rules("em0", "10.0.0.2").is_err());
    }
}
