use crate::control_plane_client::ControlPlaneClient;
use keel_agentd::wire::VolumeStatus;
use keel_agentd::JailStatus;
use keel_controlplane::wire::{NodeStatus, ServiceReplica, ServiceSummary};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct Snapshot {
    pub nodes: Vec<NodeSnapshot>,
    pub services: Vec<ServiceSnapshot>,
    pub stale: bool,
    pub stale_as_of_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NodeSnapshot {
    pub status: NodeStatus,
    pub jails: Vec<JailStatus>,
    pub volumes: Vec<VolumeStatus>,
    pub data_stale: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ServiceSnapshot {
    pub summary: ServiceSummary,
    /// Replica placement only - `ServiceSummary` (in `summary`, above)
    /// already carries `vip`/`port`, so `fetch_service`'s response (just
    /// `Vec<ServiceReplica>`, confirmed against the real
    /// `GET /services/{name}` route) is all that's needed here.
    pub detail: Option<Vec<ServiceReplica>>,
    pub data_stale: bool,
}

/// One poll cycle against `client`, falling back to `previous`'s data
/// wherever a fetch fails. A failed `fetch_nodes` (the control plane
/// itself is unreachable) returns `previous` untouched except for
/// `stale`/`stale_as_of_unix`, matching this milestone's "last good
/// snapshot kept and served with a stale banner" requirement. A failed
/// per-node jails/volumes fetch or per-service detail fetch only marks
/// that node/service `data_stale`, keeping its own last-known data,
/// without disturbing the rest of the snapshot.
pub fn poll_once(client: &dyn ControlPlaneClient, previous: &Snapshot, now_unix: i64) -> Snapshot {
    let node_statuses = match client.fetch_nodes() {
        Ok(statuses) => statuses,
        Err(_) => {
            let mut stale = previous.clone();
            stale.stale = true;
            stale.stale_as_of_unix = Some(previous.stale_as_of_unix.unwrap_or(now_unix));
            return stale;
        }
    };

    let nodes: Vec<NodeSnapshot> = node_statuses
        .into_iter()
        .map(|status| {
            let previous_node = previous.nodes.iter().find(|n| n.status.id == status.id);
            let jails_result = client.fetch_jails(&status.id);
            let volumes_result = client.fetch_volumes(&status.id);
            let data_stale = jails_result.is_err() || volumes_result.is_err();
            let jails = jails_result.unwrap_or_else(|_| previous_node.map(|n| n.jails.clone()).unwrap_or_default());
            let volumes = volumes_result.unwrap_or_else(|_| previous_node.map(|n| n.volumes.clone()).unwrap_or_default());
            NodeSnapshot { status, jails, volumes, data_stale }
        })
        .collect();

    let service_summaries =
        client.fetch_services().unwrap_or_else(|_| previous.services.iter().map(|s| s.summary.clone()).collect());
    let services: Vec<ServiceSnapshot> = service_summaries
        .into_iter()
        .map(|summary| {
            let previous_service = previous.services.iter().find(|s| s.summary.name == summary.name);
            let detail_result = client.fetch_service(&summary.name);
            let data_stale = detail_result.is_err();
            let detail = detail_result.ok().or_else(|| previous_service.and_then(|s| s.detail.clone()));
            ServiceSnapshot { summary, detail, data_stale }
        })
        .collect();

    Snapshot { nodes, services, stale: false, stale_as_of_unix: None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_client::FakeControlPlaneClient;
    use keel_agentd::{BackoffStatus, JailRecord};
    use keel_controlplane::wire::{NodeState, NodeStatus, ServiceReplica, ServiceSummary};
    use keel_spec::{JailSpec, Metadata, NetworkSpec, RestartPolicy, ResourcesSpec, Spec};

    fn sample_node(id: &str) -> NodeStatus {
        NodeStatus {
            id: id.to_string(),
            addr: "192.168.64.4:7621".to_string(),
            pod_cidr: "10.0.4.0/24".to_string(),
            status: NodeState::Alive,
            last_seen_secs: 1,
            capacity_cpu: 4.0,
            capacity_memory: 8 * 1024 * 1024 * 1024,
            committed_cpu: 1.0,
            committed_memory: 1024 * 1024 * 1024,
            ingresses: vec![],
        }
    }

    /// A minimal jail, distinguishable across calls only by `name`, used to
    /// tell "this node's own previous jails" apart from "another node's
    /// previous jails" or "freshly fetched jails".
    fn sample_jail(name: &str) -> JailStatus {
        JailStatus {
            record: JailRecord {
                spec: JailSpec {
                    api_version: "keel/v1".to_string(),
                    kind: "Jail".to_string(),
                    metadata: Metadata { name: name.to_string() },
                    spec: Spec {
                        image: "base/14.2-web".to_string(),
                        command: vec!["/usr/local/bin/myapp".to_string()],
                        network: NetworkSpec {
                            vnet: true,
                            bridge: "keel0".to_string(),
                            address: "10.0.0.5/24".to_string(),
                        },
                        resources: ResourcesSpec { cpu: "2".to_string(), memory: "512M".to_string() },
                        restart_policy: RestartPolicy::Always,
                        volumes: vec![],
                        replicate_to: None,
                    },
                },
                epair_ordinal: 1,
                deleting: false,
            },
            running: true,
            backoff: BackoffStatus::default(),
        }
    }

    #[test]
    fn a_fully_healthy_poll_is_not_stale_anywhere() {
        let client = FakeControlPlaneClient::new();
        client.set_nodes(vec![sample_node("node-1")]);
        client.set_services(vec![ServiceSummary { name: "web".to_string(), desired_replicas: 1, vip: "10.0.250.7".to_string(), port: 8080 }]);
        client.set_service("web", vec![]);

        let snapshot = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!snapshot.stale);
        assert_eq!(snapshot.nodes.len(), 1);
        assert!(!snapshot.nodes[0].data_stale);
        assert_eq!(snapshot.services.len(), 1);
        assert!(!snapshot.services[0].data_stale);
    }

    #[test]
    fn an_unreachable_control_plane_keeps_the_previous_snapshot_and_marks_it_stale() {
        let client = FakeControlPlaneClient::new();
        client.set_nodes(vec![sample_node("node-1")]);
        let first = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!first.stale);

        client.fail_nodes();
        let second = poll_once(&client, &first, 2_000);
        assert!(second.stale);
        assert_eq!(second.stale_as_of_unix, Some(2_000));
        assert_eq!(second.nodes, first.nodes, "the last-good node data must be preserved unchanged");
    }

    #[test]
    fn a_failed_per_node_jails_fetch_marks_only_that_node_stale_and_keeps_its_last_good_jails() {
        let client = FakeControlPlaneClient::new();
        client.set_nodes(vec![sample_node("node-1")]);
        client.set_jails("node-1", vec![]);
        let first = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!first.nodes[0].data_stale);

        client.fail_jails("node-1");
        let second = poll_once(&client, &first, 2_000);
        assert!(!second.stale, "a per-node failure must not mark the whole snapshot stale");
        assert!(second.nodes[0].data_stale);
        assert_eq!(second.nodes[0].jails, first.nodes[0].jails);
    }

    #[test]
    fn a_failed_service_detail_fetch_marks_only_that_service_stale() {
        let client = FakeControlPlaneClient::new();
        client.set_services(vec![ServiceSummary { name: "web".to_string(), desired_replicas: 1, vip: "10.0.250.7".to_string(), port: 8080 }]);
        client.set_service("web", vec![]);
        let first = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!first.services[0].data_stale);

        client.fail_service("web");
        let second = poll_once(&client, &first, 2_000);
        assert!(!second.stale);
        assert!(second.services[0].data_stale);
        assert_eq!(second.services[0].detail, first.services[0].detail);
    }

    #[test]
    fn a_second_consecutive_control_plane_failure_does_not_bump_stale_as_of() {
        let client = FakeControlPlaneClient::new();
        client.set_nodes(vec![sample_node("node-1")]);
        let first = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!first.stale);

        client.fail_nodes();
        let second = poll_once(&client, &first, 2_000);
        assert!(second.stale);
        assert_eq!(second.stale_as_of_unix, Some(2_000));

        // Still unreachable a poll later: the snapshot has been stale since
        // 2_000, not since 3_000, so `stale_as_of_unix` must keep pointing
        // at the *first* failure, not the most recent one.
        let third = poll_once(&client, &second, 3_000);
        assert!(third.stale);
        assert_eq!(third.stale_as_of_unix, Some(2_000), "stale_as_of_unix must preserve the first failure time, not the latest");
        assert_eq!(third.nodes, first.nodes, "the last-good node data must still be preserved unchanged");
    }

    #[test]
    fn a_failed_per_node_jails_fetch_in_a_multi_node_snapshot_only_marks_that_node_stale() {
        let client = FakeControlPlaneClient::new();
        client.set_nodes(vec![sample_node("node-1"), sample_node("node-2")]);
        client.set_jails("node-1", vec![sample_jail("old-1")]);
        client.set_jails("node-2", vec![sample_jail("old-2")]);
        let first = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!first.nodes[0].data_stale);
        assert!(!first.nodes[1].data_stale);

        // The control plane now reports nodes in the opposite order, node-1's
        // jails fetch starts failing, and node-2 gets a fresh jails list. If
        // `poll_once` ever regressed from matching `previous.nodes` by
        // `status.id` to matching by position, this reordering would make
        // node-1 pick up node-2's stale jails instead of its own.
        client.set_nodes(vec![sample_node("node-2"), sample_node("node-1")]);
        client.set_jails("node-2", vec![sample_jail("new-2")]);
        client.fail_jails("node-1");
        let second = poll_once(&client, &first, 2_000);
        assert!(!second.stale, "a per-node failure must not mark the whole snapshot stale");

        let node1 = second.nodes.iter().find(|n| n.status.id == "node-1").unwrap();
        let node2 = second.nodes.iter().find(|n| n.status.id == "node-2").unwrap();
        let previous_node1 = first.nodes.iter().find(|n| n.status.id == "node-1").unwrap();
        assert!(node1.data_stale);
        assert_eq!(node1.jails, previous_node1.jails, "node-1 must keep its own last-good jails, not node-2's");
        assert!(!node2.data_stale, "node-2's successful fetch must not be marked stale");
        assert_eq!(node2.jails, vec![sample_jail("new-2")], "node-2 must show the freshly fetched jails, not stale data");
    }

    #[test]
    fn a_failed_service_detail_fetch_in_a_multi_service_snapshot_only_marks_that_service_stale() {
        let client = FakeControlPlaneClient::new();
        client.set_services(vec![
            ServiceSummary { name: "web".to_string(), desired_replicas: 1, vip: "10.0.250.7".to_string(), port: 8080 },
            ServiceSummary { name: "api".to_string(), desired_replicas: 1, vip: "10.0.250.8".to_string(), port: 9090 },
        ]);
        client.set_service("web", vec![]);
        client.set_service(
            "api",
            vec![ServiceReplica { name: "api-0".to_string(), node: "node-1".to_string(), address: "10.0.4.5".to_string() }],
        );
        let first = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!first.services[0].data_stale);
        assert!(!first.services[1].data_stale);

        // The control plane now reports services in the opposite order,
        // "web"'s detail fetch starts failing, and "api" gets a fresh detail
        // (a different replica address, standing in for updated proxy
        // state). If `poll_once` ever regressed from matching
        // `previous.services` by `summary.name` to matching by position,
        // this reordering would make "web" pick up "api"'s stale detail
        // instead of its own.
        client.set_services(vec![
            ServiceSummary { name: "api".to_string(), desired_replicas: 1, vip: "10.0.250.8".to_string(), port: 9090 },
            ServiceSummary { name: "web".to_string(), desired_replicas: 1, vip: "10.0.250.7".to_string(), port: 8080 },
        ]);
        client.set_service(
            "api",
            vec![ServiceReplica { name: "api-0".to_string(), node: "node-1".to_string(), address: "10.0.4.9".to_string() }],
        );
        client.fail_service("web");
        let second = poll_once(&client, &first, 2_000);
        assert!(!second.stale, "a per-service failure must not mark the whole snapshot stale");

        let web = second.services.iter().find(|s| s.summary.name == "web").unwrap();
        let api = second.services.iter().find(|s| s.summary.name == "api").unwrap();
        let previous_web = first.services.iter().find(|s| s.summary.name == "web").unwrap();
        assert!(web.data_stale);
        assert_eq!(web.detail, previous_web.detail, "web must keep its own last-good detail, not api's");
        assert!(!api.data_stale, "api's successful fetch must not be marked stale");
        assert_eq!(
            api.detail,
            Some(vec![ServiceReplica { name: "api-0".to_string(), node: "node-1".to_string(), address: "10.0.4.9".to_string() }]),
            "api must show the freshly fetched detail, not stale data"
        );
    }
}
