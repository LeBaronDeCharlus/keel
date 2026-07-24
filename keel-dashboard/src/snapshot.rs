use crate::control_plane_client::ControlPlaneClient;
use keel_agentd::wire::VolumeStatus;
use keel_agentd::JailStatus;
use keel_controlplane::wire::{NodeStatus, ServiceProxyEntry, ServiceSummary};
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
    pub detail: Option<ServiceProxyEntry>,
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
    use keel_controlplane::wire::{NodeState, NodeStatus, ServiceProxyEntry, ServiceSummary};

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

    #[test]
    fn a_fully_healthy_poll_is_not_stale_anywhere() {
        let client = FakeControlPlaneClient::new();
        client.set_nodes(vec![sample_node("node-1")]);
        client.set_services(vec![ServiceSummary { name: "web".to_string(), desired_replicas: 1, vip: "10.0.250.7".to_string(), port: 8080 }]);
        client.set_service("web", ServiceProxyEntry { name: "web".to_string(), vip: "10.0.250.7".to_string(), port: 8080, replicas: vec![] });

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
        client.set_service("web", ServiceProxyEntry { name: "web".to_string(), vip: "10.0.250.7".to_string(), port: 8080, replicas: vec![] });
        let first = poll_once(&client, &Snapshot::default(), 1_000);
        assert!(!first.services[0].data_stale);

        client.fail_service("web");
        let second = poll_once(&client, &first, 2_000);
        assert!(!second.stale);
        assert!(second.services[0].data_stale);
        assert_eq!(second.services[0].detail, first.services[0].detail);
    }
}
