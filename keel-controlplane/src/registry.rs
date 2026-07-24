use crate::subnet::derive_pod_cidr;
use crate::wire::{NodeState, NodeStatus};
use ipnet::Ipv4Net;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const DEAD_THRESHOLD: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
struct NodeRecord {
    addr: String,
    replicate_addr: Option<String>,
    last_heartbeat: Instant,
    capacity_cpu: f64,
    capacity_memory: u64,
    committed_cpu: f64,
    committed_memory: u64,
    pod_cidr: Ipv4Net,
    running_jails: HashMap<String, bool>,
}

#[derive(Debug)]
pub struct Registry {
    cluster_cidr: Ipv4Net,
    nodes: HashMap<String, NodeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown node '{0}'")]
pub struct UnknownNode(pub String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    #[error("unknown node '{0}'")]
    Unknown(String),
    #[error("node '{id}' is dead (last seen {last_seen_secs}s ago)")]
    Dead { id: String, last_seen_secs: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("node '{node_id}' derived pod_cidr {derived} which collides with already-registered node '{conflicting_node_id}'")]
pub struct PodCidrCollision {
    pub node_id: String,
    pub derived: String,
    pub conflicting_node_id: String,
}

impl Registry {
    pub fn new(cluster_cidr: Ipv4Net) -> Self {
        Self { cluster_cidr, nodes: HashMap::new() }
    }

    pub fn register(
        &mut self,
        id: String,
        addr: String,
        replicate_addr: Option<String>,
        capacity_cpu: f64,
        capacity_memory: u64,
        now: Instant,
    ) -> Result<Ipv4Net, PodCidrCollision> {
        let pod_cidr = derive_pod_cidr(&id, &self.cluster_cidr);
        if let Some((conflicting_id, _)) =
            self.nodes.iter().find(|(other_id, record)| other_id.as_str() != id.as_str() && record.pod_cidr == pod_cidr)
        {
            return Err(PodCidrCollision {
                node_id: id,
                derived: pod_cidr.to_string(),
                conflicting_node_id: conflicting_id.clone(),
            });
        }
        self.nodes.insert(
            id,
            NodeRecord {
                addr,
                replicate_addr,
                last_heartbeat: now,
                capacity_cpu,
                capacity_memory,
                committed_cpu: 0.0,
                committed_memory: 0,
                pod_cidr,
                running_jails: HashMap::new(),
            },
        );
        Ok(pod_cidr)
    }

    pub fn heartbeat(
        &mut self,
        id: &str,
        committed_cpu: f64,
        committed_memory: u64,
        jails: Vec<crate::wire::JailHealth>,
        now: Instant,
    ) -> Result<(), UnknownNode> {
        match self.nodes.get_mut(id) {
            Some(record) => {
                record.last_heartbeat = now;
                record.committed_cpu = committed_cpu;
                record.committed_memory = committed_memory;
                record.running_jails = jails.into_iter().map(|j| (j.name, j.running)).collect();
                Ok(())
            }
            None => Err(UnknownNode(id.to_string())),
        }
    }

    /// Whether `node_id`'s most recent heartbeat reported `jail_name` as
    /// running. `false` for an unknown node, an unknown jail, or a node
    /// that has never heartbeated with jail health at all.
    pub fn is_jail_running(&self, node_id: &str, jail_name: &str) -> bool {
        self.nodes.get(node_id).and_then(|r| r.running_jails.get(jail_name)).copied().unwrap_or(false)
    }

    /// The node's assigned `pod_cidr`, typed -- for consumers (address
    /// auto-assignment) that need to do arithmetic on it rather than just
    /// display it, unlike `list()`'s stringified `NodeStatus.pod_cidr`.
    pub fn pod_cidr(&self, node_id: &str) -> Option<Ipv4Net> {
        self.nodes.get(node_id).map(|r| r.pod_cidr)
    }

    /// The node's advertised replication-listener address ("host:port"),
    /// distinct from its main HTTP `addr` -- `None` until the node has
    /// registered with a `replicate_addr` at least once.
    pub fn replicate_addr(&self, node_id: &str) -> Option<String> {
        self.nodes.get(node_id).and_then(|r| r.replicate_addr.clone())
    }

    pub fn list(&self, now: Instant) -> Vec<NodeStatus> {
        let mut statuses: Vec<NodeStatus> = self
            .nodes
            .iter()
            .map(|(id, record)| {
                let elapsed = now.saturating_duration_since(record.last_heartbeat);
                NodeStatus {
                    id: id.clone(),
                    addr: record.addr.clone(),
                    pod_cidr: record.pod_cidr.to_string(),
                    status: if elapsed < DEAD_THRESHOLD { NodeState::Alive } else { NodeState::Dead },
                    last_seen_secs: elapsed.as_secs(),
                    capacity_cpu: record.capacity_cpu,
                    capacity_memory: record.capacity_memory,
                    committed_cpu: record.committed_cpu,
                    committed_memory: record.committed_memory,
                    ingresses: vec![],
                }
            })
            .collect();
        statuses.sort_by(|a, b| a.id.cmp(&b.id));
        statuses
    }

    pub fn resolve(&self, id: &str, now: Instant) -> Result<String, ResolveError> {
        let record = self.nodes.get(id).ok_or_else(|| ResolveError::Unknown(id.to_string()))?;
        let elapsed = now.saturating_duration_since(record.last_heartbeat);
        if elapsed >= DEAD_THRESHOLD {
            return Err(ResolveError::Dead { id: id.to_string(), last_seen_secs: elapsed.as_secs() });
        }
        Ok(record.addr.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster_cidr() -> Ipv4Net {
        "10.0.0.0/16".parse().unwrap()
    }

    fn small_cluster_cidr() -> Ipv4Net {
        "10.0.0.0/22".parse().unwrap()
    }

    #[test]
    fn register_then_list_shows_the_node_as_alive() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "192.168.64.4".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();

        let statuses = registry.list(now);
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].id, "node-1");
        assert_eq!(statuses[0].addr, "192.168.64.4");
        assert_eq!(statuses[0].status, NodeState::Alive);
        assert_eq!(statuses[0].last_seen_secs, 0);
    }

    #[test]
    fn register_returns_the_derived_pod_cidr() {
        let mut registry = Registry::new(test_cluster_cidr());
        let pod_cidr = registry.register("node-1".to_string(), "192.168.64.4".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, Instant::now()).unwrap();
        assert_eq!(pod_cidr, "10.0.131.0/24".parse::<Ipv4Net>().unwrap());
    }

    #[test]
    fn list_includes_pod_cidr_per_node() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "192.168.64.4".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();
        assert_eq!(registry.list(now)[0].pod_cidr, "10.0.131.0/24");
    }

    #[test]
    fn a_colliding_registration_is_rejected_and_names_both_nodes() {
        let mut registry = Registry::new(small_cluster_cidr());
        let now = Instant::now();
        registry.register("node-4".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();

        let err = registry
            .register("node-8".to_string(), "10.0.0.2".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now)
            .unwrap_err();
        assert_eq!(err.node_id, "node-8");
        assert_eq!(err.conflicting_node_id, "node-4");
        assert_eq!(err.derived, "10.0.0.0/24");

        // The first node's assignment must be untouched by the rejected second registration.
        let statuses = registry.list(now);
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].id, "node-4");
        assert_eq!(statuses[0].pod_cidr, "10.0.0.0/24");
    }

    #[test]
    fn reregistering_an_existing_id_refreshes_its_address_and_heartbeat() {
        let mut registry = Registry::new(test_cluster_cidr());
        let t0 = Instant::now();
        let first_pod_cidr = registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t0).unwrap();

        let t1 = t0 + Duration::from_secs(5);
        let second_pod_cidr = registry.register("node-1".to_string(), "10.0.0.2".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t1).unwrap();

        assert_eq!(first_pod_cidr, second_pod_cidr, "re-registering the same node-id must derive the same pod_cidr");

        let statuses = registry.list(t1);
        assert_eq!(statuses.len(), 1, "re-registering must not create a second entry");
        assert_eq!(statuses[0].addr, "10.0.0.2");
        assert_eq!(statuses[0].last_seen_secs, 0);
    }

    #[test]
    fn heartbeat_on_a_known_node_updates_its_last_heartbeat() {
        let mut registry = Registry::new(test_cluster_cidr());
        let t0 = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t0).unwrap();

        let t1 = t0 + Duration::from_secs(10);
        registry.heartbeat("node-1", 0.0, 0, vec![], t1).unwrap();

        let statuses = registry.list(t1);
        assert_eq!(statuses[0].last_seen_secs, 0);
    }

    #[test]
    fn heartbeat_on_an_unknown_node_returns_unknown_node_error() {
        let mut registry = Registry::new(test_cluster_cidr());
        let err = registry.heartbeat("missing", 0.0, 0, vec![], Instant::now()).unwrap_err();
        assert_eq!(err, UnknownNode("missing".to_string()));
        assert_eq!(err.to_string(), "unknown node 'missing'");
    }

    #[test]
    fn list_reports_dead_once_a_node_exceeds_the_dead_threshold() {
        let mut registry = Registry::new(test_cluster_cidr());
        let t0 = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t0).unwrap();

        let just_under = t0 + Duration::from_secs(14);
        assert_eq!(registry.list(just_under)[0].status, NodeState::Alive);

        let at_threshold = t0 + DEAD_THRESHOLD;
        assert_eq!(registry.list(at_threshold)[0].status, NodeState::Dead);
    }

    #[test]
    fn list_is_sorted_by_id() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-2".to_string(), "10.0.0.2".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();

        let statuses = registry.list(now);
        assert_eq!(statuses.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(), vec!["node-1", "node-2"]);
    }

    #[test]
    fn list_on_an_empty_registry_is_empty() {
        let registry = Registry::new(test_cluster_cidr());
        assert_eq!(registry.list(Instant::now()), vec![]);
    }

    #[test]
    fn resolve_on_an_unknown_node_returns_unknown_error() {
        let registry = Registry::new(test_cluster_cidr());
        let err = registry.resolve("missing", Instant::now()).unwrap_err();
        assert_eq!(err, ResolveError::Unknown("missing".to_string()));
    }

    #[test]
    fn resolve_on_an_alive_node_returns_its_address() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();
        assert_eq!(registry.resolve("node-1", now), Ok("10.0.0.1".to_string()));
    }

    #[test]
    fn resolve_on_a_dead_node_returns_dead_error_with_elapsed_seconds() {
        let mut registry = Registry::new(test_cluster_cidr());
        let t0 = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t0).unwrap();

        let at_threshold = t0 + DEAD_THRESHOLD;
        let err = registry.resolve("node-1", at_threshold).unwrap_err();
        assert_eq!(err, ResolveError::Dead { id: "node-1".to_string(), last_seen_secs: DEAD_THRESHOLD.as_secs() });
    }

    #[test]
    fn register_initializes_committed_resources_to_zero() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();

        let statuses = registry.list(now);
        assert_eq!(statuses[0].capacity_cpu, 4.0);
        assert_eq!(statuses[0].capacity_memory, 8 * 1024 * 1024 * 1024);
        assert_eq!(statuses[0].committed_cpu, 0.0);
        assert_eq!(statuses[0].committed_memory, 0);
    }

    #[test]
    fn heartbeat_updates_committed_resources_without_changing_capacity() {
        let mut registry = Registry::new(test_cluster_cidr());
        let t0 = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t0).unwrap();

        let t1 = t0 + Duration::from_secs(5);
        registry.heartbeat("node-1", 2.0, 1024 * 1024 * 1024, vec![], t1).unwrap();

        let statuses = registry.list(t1);
        assert_eq!(statuses[0].committed_cpu, 2.0);
        assert_eq!(statuses[0].committed_memory, 1024 * 1024 * 1024);
        assert_eq!(statuses[0].capacity_cpu, 4.0, "heartbeat must not change capacity");
        assert_eq!(statuses[0].capacity_memory, 8 * 1024 * 1024 * 1024, "heartbeat must not change capacity");
    }

    #[test]
    fn heartbeat_records_per_jail_running_status() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();

        registry
            .heartbeat(
                "node-1",
                0.0,
                0,
                vec![
                    crate::wire::JailHealth { name: "web-0".to_string(), running: true },
                    crate::wire::JailHealth { name: "web-1".to_string(), running: false },
                ],
                now,
            )
            .unwrap();

        assert!(registry.is_jail_running("node-1", "web-0"));
        assert!(!registry.is_jail_running("node-1", "web-1"));
    }

    #[test]
    fn is_jail_running_on_an_unreported_jail_is_false() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();
        assert!(!registry.is_jail_running("node-1", "web-0"));
    }

    #[test]
    fn is_jail_running_on_an_unknown_node_is_false() {
        let registry = Registry::new(test_cluster_cidr());
        assert!(!registry.is_jail_running("missing", "web-0"));
    }

    #[test]
    fn a_later_heartbeat_replaces_the_previous_jail_health_report_wholesale() {
        let mut registry = Registry::new(test_cluster_cidr());
        let t0 = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t0).unwrap();
        registry
            .heartbeat("node-1", 0.0, 0, vec![crate::wire::JailHealth { name: "web-0".to_string(), running: true }], t0)
            .unwrap();

        let t1 = t0 + Duration::from_secs(5);
        registry.heartbeat("node-1", 0.0, 0, vec![], t1).unwrap();

        assert!(!registry.is_jail_running("node-1", "web-0"), "a heartbeat with no jails must clear the previous report");
    }

    #[test]
    fn pod_cidr_returns_the_registered_nodes_block() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();
        assert_eq!(registry.pod_cidr("node-1"), Some("10.0.131.0/24".parse().unwrap()));
    }

    #[test]
    fn pod_cidr_on_an_unknown_node_is_none() {
        let registry = Registry::new(test_cluster_cidr());
        assert_eq!(registry.pod_cidr("missing"), None);
    }

    #[test]
    fn replicate_addr_round_trips_through_register_and_the_accessor() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry
            .register("node-1".to_string(), "10.0.0.1".to_string(), Some("10.0.0.1:7622".to_string()), 4.0, 8 * 1024 * 1024 * 1024, now)
            .unwrap();
        assert_eq!(registry.replicate_addr("node-1"), Some("10.0.0.1:7622".to_string()));
    }

    #[test]
    fn replicate_addr_is_none_for_a_node_that_never_advertised_one() {
        let mut registry = Registry::new(test_cluster_cidr());
        let now = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();
        assert_eq!(registry.replicate_addr("node-1"), None);
    }

    #[test]
    fn replicate_addr_on_an_unregistered_node_is_none() {
        let registry = Registry::new(test_cluster_cidr());
        assert_eq!(registry.replicate_addr("missing"), None);
    }
}
