use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

/// Which addresses are currently assigned to a replica, per node -- lives
/// next to `Placements`/`Services`, persisted the same way (see
/// `keel-controlplane/src/store.rs`), populated when a replica is scheduled
/// and freed when it's torn down.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UsedAddresses {
    used_by_node: HashMap<String, HashSet<Ipv4Addr>>,
    by_jail: HashMap<String, (String, Ipv4Addr)>,
}

impl UsedAddresses {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_used(&self, node_id: &str, addr: Ipv4Addr) -> bool {
        self.used_by_node.get(node_id).is_some_and(|set| set.contains(&addr))
    }

    pub fn record(&mut self, jail_name: String, node_id: String, addr: Ipv4Addr) {
        self.release(&jail_name);
        self.used_by_node.entry(node_id.clone()).or_default().insert(addr);
        self.by_jail.insert(jail_name, (node_id, addr));
    }

    pub fn release(&mut self, jail_name: &str) {
        if let Some((node_id, addr)) = self.by_jail.remove(jail_name) {
            if let Some(set) = self.used_by_node.get_mut(&node_id) {
                set.remove(&addr);
            }
        }
    }

    pub fn address_of(&self, jail_name: &str) -> Option<Ipv4Addr> {
        self.by_jail.get(jail_name).map(|(_, addr)| *addr)
    }
}

/// The first address in `pod_cidr` not already used on `node_id`, starting
/// from network-plus-2. `Ipv4Net::hosts()` already excludes the network and
/// broadcast addresses; the first host address (network-plus-1) is further
/// skipped here because `keel-net`'s `bridge_gateway` (Milestone 14)
/// permanently reserves it as the node's `keel0` bridge gateway.
pub fn first_free_address(pod_cidr: Ipv4Net, node_id: &str, used: &UsedAddresses) -> Option<Ipv4Addr> {
    pod_cidr.hosts().skip(1).find(|addr| !used.is_used(node_id, *addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cidr(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    fn addr(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    #[test]
    fn first_free_address_skips_network_and_network_plus_one() {
        let used = UsedAddresses::new();
        assert_eq!(first_free_address(cidr("10.0.60.0/24"), "node-1", &used), Some(addr("10.0.60.2")));
    }

    #[test]
    fn first_free_address_skips_addresses_already_recorded_used_on_that_node() {
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        assert_eq!(first_free_address(cidr("10.0.60.0/24"), "node-1", &used), Some(addr("10.0.60.3")));
    }

    #[test]
    fn first_free_address_on_a_different_node_is_unaffected_by_another_nodes_usage() {
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        assert_eq!(first_free_address(cidr("10.0.60.0/24"), "node-2", &used), Some(addr("10.0.60.2")));
    }

    #[test]
    fn record_then_release_frees_the_address_again() {
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        used.release("web-0");
        assert_eq!(first_free_address(cidr("10.0.60.0/24"), "node-1", &used), Some(addr("10.0.60.2")));
    }

    #[test]
    fn re_recording_a_jail_on_a_different_node_frees_its_old_address() {
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        used.record("web-0".to_string(), "node-2".to_string(), addr("10.0.60.5"));

        // The old (node-1, 10.0.60.2) pair must no longer be considered used.
        assert_eq!(first_free_address(cidr("10.0.60.0/24"), "node-1", &used), Some(addr("10.0.60.2")));
        // The jail now resolves to its new node/address.
        assert_eq!(used.address_of("web-0"), Some(addr("10.0.60.5")));
    }

    #[test]
    fn releasing_a_never_recorded_jail_is_a_safe_no_op() {
        let mut used = UsedAddresses::new();
        used.release("never-seen");
        assert_eq!(used.address_of("never-seen"), None);
    }

    #[test]
    fn address_of_returns_none_for_an_unrecorded_jail() {
        let used = UsedAddresses::new();
        assert_eq!(used.address_of("web-0"), None);
    }

    #[test]
    fn address_of_returns_the_recorded_address() {
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        assert_eq!(used.address_of("web-0"), Some(addr("10.0.60.2")));
    }

    #[test]
    fn a_full_pod_cidr_returns_none() {
        // A /30 has exactly 2 host addresses (per `hosts()`): network+1 and
        // network+2. Skipping network+1 leaves only network+2; once that's
        // used, nothing remains.
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        assert_eq!(first_free_address(cidr("10.0.60.0/30"), "node-1", &used), None);
    }

    #[test]
    fn used_addresses_round_trips_through_yaml() {
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        let path = std::env::temp_dir().join(format!("keel-controlplane-used-addresses-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &used).unwrap();
        let loaded: UsedAddresses = crate::store::load_or_default(&path);
        assert_eq!(loaded.address_of("web-0"), Some(addr("10.0.60.2")));
        assert!(loaded.is_used("node-1", addr("10.0.60.2")));
    }
}
