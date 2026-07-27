use keel_controlplane::wire::ServiceProxyEntry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct ServiceVipSlot(Arc<Mutex<HashMap<String, (String, u16)>>>);

impl ServiceVipSlot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_all(&self, entries: &[ServiceProxyEntry]) {
        let mut map = self.0.lock().unwrap();
        map.clear();
        for entry in entries {
            map.insert(entry.name.clone(), (entry.vip.clone(), entry.port));
        }
    }

    pub fn get(&self, name: &str) -> Option<(String, u16)> {
        self.0.lock().unwrap().get(name).cloned()
    }

    pub fn names(&self) -> HashSet<String> {
        self.0.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, vip: &str, port: u16) -> ServiceProxyEntry {
        ServiceProxyEntry {
            name: name.to_string(),
            vip: vip.to_string(),
            port,
            replicas: vec![],
        }
    }

    #[test]
    fn a_fresh_slot_knows_no_services() {
        let slot = ServiceVipSlot::new();
        assert_eq!(slot.get("hugo-site"), None);
        assert!(slot.names().is_empty());
    }

    #[test]
    fn set_all_then_get_returns_the_vip_and_port() {
        let slot = ServiceVipSlot::new();
        slot.set_all(&[entry("hugo-site", "10.0.0.9", 8080)]);
        assert_eq!(slot.get("hugo-site"), Some(("10.0.0.9".to_string(), 8080)));
    }

    #[test]
    fn set_all_replaces_the_previous_table_rather_than_merging() {
        let slot = ServiceVipSlot::new();
        slot.set_all(&[entry("hugo-site", "10.0.0.9", 8080)]);
        slot.set_all(&[entry("umami", "10.0.0.10", 3000)]);
        assert_eq!(slot.get("hugo-site"), None);
        assert_eq!(slot.get("umami"), Some(("10.0.0.10".to_string(), 3000)));
    }

    #[test]
    fn names_lists_every_currently_known_service() {
        let slot = ServiceVipSlot::new();
        slot.set_all(&[
            entry("hugo-site", "10.0.0.9", 8080),
            entry("umami", "10.0.0.10", 3000),
        ]);
        assert_eq!(
            slot.names(),
            HashSet::from(["hugo-site".to_string(), "umami".to_string()])
        );
    }

    #[test]
    fn clones_share_the_same_underlying_slot() {
        let slot = ServiceVipSlot::new();
        let clone = slot.clone();
        clone.set_all(&[entry("hugo-site", "10.0.0.9", 8080)]);
        assert_eq!(slot.get("hugo-site"), Some(("10.0.0.9".to_string(), 8080)));
    }
}
