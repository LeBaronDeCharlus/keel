use crate::placements::Placements;
use crate::scheduler::{self, NodeResources};
use ipnet::Ipv4Net;
use keel_spec::JailTemplate;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::Ipv4Addr;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRecord {
    pub desired_replicas: u32,
    pub template: JailTemplate,
    pub vip: Ipv4Addr,
    pub port: u16,
}

#[derive(Debug)]
pub struct Services {
    service_cidr: Ipv4Net,
    by_name: HashMap<String, ServiceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    Unmanaged,
    Service(String),
}

impl std::fmt::Display for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Owner::Unmanaged => write!(f, "an unmanaged jail"),
            Owner::Service(name) => write!(f, "service '{name}'"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApplyServiceError {
    #[error("name '{name}' is already in use by {owner}")]
    NameConflict { name: String, owner: Owner },
    #[error("service '{0}' template is immutable once created; delete and re-apply instead")]
    TemplateChanged(String),
    #[error("service '{0}' port is immutable once created; delete and re-apply instead")]
    PortChanged(String),
    #[error("no free VIP available in the service CIDR for service '{0}'")]
    VipPoolExhausted(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown service '{0}'")]
pub struct UnknownService(pub String);

#[derive(Default, Serialize, Deserialize)]
struct ServicesState {
    by_name: HashMap<String, ServiceRecord>,
}

impl Services {
    /// Loads persisted service definitions from `state_dir`, pairing them
    /// with `service_cidr` from the current process's own config - never
    /// from disk, so a changed `--service-cidr` flag across a restart can
    /// never be silently overridden by stale persisted data.
    pub fn load(state_dir: &std::path::Path, service_cidr: Ipv4Net) -> Self {
        let state: ServicesState = crate::store::load_or_default(&state_dir.join("services.yaml"));
        Self {
            service_cidr,
            by_name: state.by_name,
        }
    }

    pub fn persist(&self, state_dir: &std::path::Path) {
        let state = ServicesState {
            by_name: self.by_name.clone(),
        };
        if let Err(e) = crate::store::save(&state_dir.join("services.yaml"), &state) {
            eprintln!("keel-controlplane: failed to persist services.yaml: {e}");
        }
    }

    pub fn new(service_cidr: Ipv4Net) -> Self {
        Self {
            service_cidr,
            by_name: HashMap::new(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&ServiceRecord> {
        self.by_name.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.by_name.keys().map(|s| s.as_str())
    }

    pub fn list(&self) -> Vec<(&str, &ServiceRecord)> {
        let mut entries: Vec<(&str, &ServiceRecord)> =
            self.by_name.iter().map(|(k, v)| (k.as_str(), v)).collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        entries
    }

    /// Creates the service if `name` is new (allocating and probing a VIP
    /// within `service_cidr`), or updates `desired_replicas` if it already
    /// exists and `template`/`port` are both unchanged. Rejects a
    /// `template` or `port` change on an existing service (only `replicas`
    /// may change) -- both are as much a part of a service's identity as
    /// each other, so both get the same immutable-once-created treatment.
    pub fn apply(
        &mut self,
        name: String,
        desired_replicas: u32,
        template: JailTemplate,
        port: u16,
    ) -> Result<(), ApplyServiceError> {
        if let Some(existing) = self.by_name.get(&name) {
            if existing.template != template {
                return Err(ApplyServiceError::TemplateChanged(name));
            }
            if existing.port != port {
                return Err(ApplyServiceError::PortChanged(name));
            }
            let vip = existing.vip;
            self.by_name.insert(
                name,
                ServiceRecord {
                    desired_replicas,
                    template,
                    vip,
                    port,
                },
            );
            return Ok(());
        }

        let taken: HashSet<Ipv4Addr> = self.by_name.values().map(|r| r.vip).collect();
        let host_count: u32 = 1u32 << (32 - self.service_cidr.prefix_len());
        let vip = (0..host_count)
            .map(|attempt| crate::subnet::derive_service_vip(&name, &self.service_cidr, attempt))
            .find(|addr| !taken.contains(addr))
            .ok_or_else(|| ApplyServiceError::VipPoolExhausted(name.clone()))?;

        self.by_name.insert(
            name,
            ServiceRecord {
                desired_replicas,
                template,
                vip,
                port,
            },
        );
        Ok(())
    }

    pub fn remove(&mut self, name: &str) -> Option<ServiceRecord> {
        self.by_name.remove(name)
    }
}

/// `"<service_name>-<index>"`.
pub fn replica_name(service_name: &str, index: u32) -> String {
    format!("{service_name}-{index}")
}

/// If `jail_name` is `"<service_name>-<index>"` for a plain non-negative
/// integer index, returns that index. Rejects non-canonical forms (leading
/// `+`, leading zeros) so that e.g. `"web-03"` is not mistaken for index 3.
pub fn replica_index(service_name: &str, jail_name: &str) -> Option<u32> {
    let suffix = jail_name.strip_prefix(service_name)?.strip_prefix('-')?;
    let index: u32 = suffix.parse().ok()?;
    if index.to_string() == suffix {
        Some(index)
    } else {
        None
    }
}

/// Returns the current owner of a name already present in `placements`, or
/// `None` if it has no existing placement at all. A name belongs to a
/// service if it matches that service's deterministic replica pattern;
/// otherwise, if it's placed at all, it's an unmanaged plain `kind: Jail`.
pub fn owner_of(name: &str, placements: &Placements, services: &Services) -> Option<Owner> {
    placements.get(name)?;
    for service_name in services.names() {
        if replica_index(service_name, name).is_some() {
            return Some(Owner::Service(service_name.to_string()));
        }
    }
    Some(Owner::Unmanaged)
}

/// Given a service's desired replica count and the set of indices currently
/// healthy (Alive node + running jail, per the caller), returns the indices
/// to schedule (lowest missing first) and the indices to tear down (highest
/// healthy first), matching `diff_replicas(3, {}) == ([0,1,2], [])` and
/// `diff_replicas(1, {0,1,2}) == ([], [2,1])`.
pub fn diff_replicas(desired: u32, healthy_indices: &BTreeSet<u32>) -> (Vec<u32>, Vec<u32>) {
    let healthy_count = healthy_indices.len() as u32;
    if healthy_count < desired {
        let missing = desired - healthy_count;
        let to_add = (0u32..)
            .filter(|i| !healthy_indices.contains(i))
            .take(missing as usize)
            .collect();
        (to_add, Vec::new())
    } else if healthy_count > desired {
        let excess = healthy_count - desired;
        let to_remove = healthy_indices
            .iter()
            .rev()
            .take(excess as usize)
            .copied()
            .collect();
        (Vec::new(), to_remove)
    } else {
        (Vec::new(), Vec::new())
    }
}

/// Every node currently hosting at least one of `service_name`'s replicas,
/// per `Placements` (matching replica names by the `<service_name>-`
/// prefix via `replica_index`).
pub fn nodes_hosting_service(service_name: &str, placements: &Placements) -> HashSet<String> {
    placements
        .iter()
        .filter(|(jail_name, _)| replica_index(service_name, jail_name).is_some())
        .map(|(_, node_id)| node_id.to_string())
        .collect()
}

/// Prefers a node not in `busy_nodes` (same-service spreading); falls back
/// to `pick_node`'s plain headroom-based bin-packing over the *unfiltered*
/// `candidates` once every candidate is busy. `scheduler::pick_node` itself
/// is unchanged -- this is purely a filter applied by its caller.
pub fn pick_node_for_service(
    candidates: Vec<NodeResources>,
    busy_nodes: &HashSet<String>,
) -> Result<String, scheduler::ScheduleError> {
    let filtered: Vec<NodeResources> = candidates
        .iter()
        .filter(|n| !busy_nodes.contains(&n.id))
        .cloned()
        .collect();
    if !filtered.is_empty() {
        scheduler::pick_node(&filtered)
    } else {
        scheduler::pick_node(&candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_spec::{ResourcesSpec, RestartPolicy, TemplateNetworkSpec};

    fn test_service_cidr() -> Ipv4Net {
        "10.0.250.0/24".parse().unwrap()
    }

    fn template() -> JailTemplate {
        JailTemplate {
            image: "base/14.2-web".to_string(),
            command: vec!["/usr/local/bin/myapp".to_string()],
            network: TemplateNetworkSpec {
                vnet: true,
                bridge: "keel0".to_string(),
            },
            resources: ResourcesSpec {
                cpu: "1".to_string(),
                memory: "256M".to_string(),
            },
            restart_policy: RestartPolicy::Always,
            volumes: vec![],
        }
    }

    #[test]
    fn apply_creates_a_new_service() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 3, template(), 8080)
            .unwrap();
        assert_eq!(services.get("web").unwrap().desired_replicas, 3);
        assert_eq!(services.get("web").unwrap().port, 8080);
    }

    #[test]
    fn apply_again_with_the_same_template_and_port_scales_up_or_down() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 3, template(), 8080)
            .unwrap();
        services
            .apply("web".to_string(), 5, template(), 8080)
            .unwrap();
        assert_eq!(services.get("web").unwrap().desired_replicas, 5);
        services
            .apply("web".to_string(), 0, template(), 8080)
            .unwrap();
        assert_eq!(services.get("web").unwrap().desired_replicas, 0);
    }

    #[test]
    fn apply_with_a_changed_template_is_rejected() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 3, template(), 8080)
            .unwrap();
        let mut changed = template();
        changed.image = "base/different-image".to_string();
        assert_eq!(
            services.apply("web".to_string(), 3, changed, 8080),
            Err(ApplyServiceError::TemplateChanged("web".to_string()))
        );
    }

    #[test]
    fn apply_with_a_changed_port_is_rejected() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 3, template(), 8080)
            .unwrap();
        assert_eq!(
            services.apply("web".to_string(), 3, template(), 9090),
            Err(ApplyServiceError::PortChanged("web".to_string()))
        );
    }

    #[test]
    fn apply_preserves_the_same_vip_across_a_scale_only_reapply() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 1, template(), 8080)
            .unwrap();
        let first_vip = services.get("web").unwrap().vip;
        services
            .apply("web".to_string(), 5, template(), 8080)
            .unwrap();
        assert_eq!(services.get("web").unwrap().vip, first_vip);
    }

    #[test]
    fn apply_gives_two_different_services_two_different_vips() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 1, template(), 8080)
            .unwrap();
        services
            .apply("api".to_string(), 1, template(), 8080)
            .unwrap();
        assert_ne!(
            services.get("web").unwrap().vip,
            services.get("api").unwrap().vip
        );
    }

    #[test]
    fn apply_on_a_fully_exhausted_service_cidr_is_rejected() {
        // A /30 has exactly 4 host addresses; the 5th distinct service name
        // must exhaust the pool.
        let mut services = Services::new("10.0.250.0/30".parse().unwrap());
        for i in 0..4 {
            services
                .apply(format!("svc-{i}"), 1, template(), 8080)
                .unwrap();
        }
        assert_eq!(
            services.apply("one-too-many".to_string(), 1, template(), 8080),
            Err(ApplyServiceError::VipPoolExhausted(
                "one-too-many".to_string()
            ))
        );
    }

    #[test]
    fn remove_deletes_the_service() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 3, template(), 8080)
            .unwrap();
        assert!(services.remove("web").is_some());
        assert!(services.get("web").is_none());
    }

    #[test]
    fn list_is_sorted_by_name() {
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 1, template(), 8080)
            .unwrap();
        services
            .apply("api".to_string(), 1, template(), 8080)
            .unwrap();
        let names: Vec<&str> = services.list().iter().map(|(n, _)| *n).collect();
        assert_eq!(names, vec!["api", "web"]);
    }

    #[test]
    fn replica_name_and_index_round_trip() {
        assert_eq!(replica_name("web", 2), "web-2");
        assert_eq!(replica_index("web", "web-2"), Some(2));
        assert_eq!(replica_index("web", "other-2"), None);
        assert_eq!(replica_index("web", "web-not-a-number"), None);
    }

    #[test]
    fn replica_index_rejects_a_leading_zero() {
        assert_eq!(replica_index("web", "web-03"), None);
    }

    #[test]
    fn replica_index_rejects_a_leading_plus() {
        assert_eq!(replica_index("web", "web-+3"), None);
    }

    #[test]
    fn owner_of_an_unplaced_name_is_none() {
        let placements = Placements::new();
        let services = Services::new(test_service_cidr());
        assert_eq!(owner_of("web-0", &placements, &services), None);
    }

    #[test]
    fn owner_of_a_placed_name_matching_a_known_service_is_that_service() {
        let mut placements = Placements::new();
        placements.set("web-0".to_string(), "node-1".to_string());
        let mut services = Services::new(test_service_cidr());
        services
            .apply("web".to_string(), 1, template(), 8080)
            .unwrap();
        assert_eq!(
            owner_of("web-0", &placements, &services),
            Some(Owner::Service("web".to_string()))
        );
    }

    #[test]
    fn owner_of_a_placed_name_matching_no_service_is_unmanaged() {
        let mut placements = Placements::new();
        placements.set("web-1".to_string(), "node-1".to_string());
        let services = Services::new(test_service_cidr());
        assert_eq!(
            owner_of("web-1", &placements, &services),
            Some(Owner::Unmanaged)
        );
    }

    #[test]
    fn diff_replicas_with_nothing_placed_adds_from_zero() {
        assert_eq!(diff_replicas(3, &BTreeSet::new()), (vec![0, 1, 2], vec![]));
    }

    #[test]
    fn diff_replicas_reuses_only_the_missing_index() {
        let healthy = BTreeSet::from([0, 1]);
        assert_eq!(diff_replicas(3, &healthy), (vec![2], vec![]));
    }

    #[test]
    fn diff_replicas_skips_an_index_that_is_unhealthy_and_reschedules_it() {
        let healthy = BTreeSet::from([1]);
        assert_eq!(diff_replicas(2, &healthy), (vec![0], vec![]));
    }

    #[test]
    fn diff_replicas_scales_down_from_the_highest_index() {
        let healthy = BTreeSet::from([0, 1, 2]);
        assert_eq!(diff_replicas(1, &healthy), (vec![], vec![2, 1]));
    }

    #[test]
    fn diff_replicas_scaled_to_zero_tears_down_everything() {
        let healthy = BTreeSet::from([0, 1, 2]);
        assert_eq!(diff_replicas(0, &healthy), (vec![], vec![2, 1, 0]));
    }

    #[test]
    fn diff_replicas_at_the_desired_count_does_nothing() {
        let healthy = BTreeSet::from([0, 1]);
        assert_eq!(diff_replicas(2, &healthy), (vec![], vec![]));
    }

    fn node(id: &str, capacity_cpu: f64, capacity_memory: u64) -> NodeResources {
        NodeResources {
            id: id.to_string(),
            capacity_cpu,
            capacity_memory,
            committed_cpu: 0.0,
            committed_memory: 0,
        }
    }

    #[test]
    fn nodes_hosting_service_finds_only_this_services_placements() {
        let mut placements = Placements::new();
        placements.set("web-0".to_string(), "node-1".to_string());
        placements.set("web-1".to_string(), "node-2".to_string());
        placements.set("other-jail".to_string(), "node-3".to_string());
        let busy = nodes_hosting_service("web", &placements);
        assert_eq!(
            busy,
            HashSet::from(["node-1".to_string(), "node-2".to_string()])
        );
    }

    #[test]
    fn pick_node_for_service_avoids_a_busy_node_when_an_alternative_exists() {
        let candidates = vec![node("node-1", 4.0, 100), node("node-2", 4.0, 100)];
        let busy = HashSet::from(["node-1".to_string()]);
        assert_eq!(
            pick_node_for_service(candidates, &busy),
            Ok("node-2".to_string())
        );
    }

    #[test]
    fn pick_node_for_service_falls_back_to_bin_packing_once_every_node_is_busy() {
        let candidates = vec![node("node-1", 4.0, 100), node("node-2", 4.0, 100)];
        let busy = HashSet::from(["node-1".to_string(), "node-2".to_string()]);
        // Both busy: falls back to the unfiltered candidate list, which
        // `pick_node`'s own tie-break (ascending id) picks node-1.
        assert_eq!(
            pick_node_for_service(candidates, &busy),
            Ok("node-1".to_string())
        );
    }

    #[test]
    fn pick_node_for_service_with_no_candidates_at_all_is_no_available_nodes() {
        assert_eq!(
            pick_node_for_service(vec![], &HashSet::new()),
            Err(scheduler::ScheduleError::NoAvailableNodes)
        );
    }

    #[test]
    fn load_uses_the_passed_in_service_cidr_not_whatever_was_persisted_last() {
        let dir = std::env::temp_dir().join(format!(
            "keel-controlplane-services-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let mut original = Services::new("10.0.250.0/24".parse().unwrap());
        original
            .apply("web".to_string(), 1, template(), 8080)
            .unwrap();
        original.persist(&dir);

        // A restart with a *different* --service-cidr flag: the loaded
        // Services must use the new cidr, not silently keep serving the old
        // one from disk.
        let reloaded = Services::load(&dir, "10.0.251.0/24".parse().unwrap());
        assert_eq!(
            reloaded.get("web").unwrap().desired_replicas,
            1,
            "the persisted service itself must still be there"
        );

        let mut fresh_apply = reloaded;
        // A fresh, never-before-seen service name must get a VIP inside the
        // *new* cidr, proving service_cidr truly came from the argument.
        fresh_apply
            .apply("api".to_string(), 1, template(), 8080)
            .unwrap();
        let vip = fresh_apply.get("api").unwrap().vip;
        assert!(
            "10.0.251.0/24".parse::<Ipv4Net>().unwrap().contains(&vip),
            "expected the new service's VIP {vip} inside the newly-passed-in service_cidr, not the persisted one"
        );
    }

    #[test]
    fn load_on_a_missing_state_dir_returns_an_empty_services() {
        let dir = std::env::temp_dir().join(format!(
            "keel-controlplane-services-test-missing-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let services = Services::load(&dir, test_service_cidr());
        assert_eq!(services.list(), vec![]);
    }
}
