use crate::control_plane_client::ControlPlaneClient;
use crate::snapshot::{poll_once, Snapshot};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Spawns the background poller and returns the shared, ever-refreshing
/// snapshot. The returned handle is read-only from the caller's side - the
/// browser-facing HTTP layer only ever reads it, never blocking on a live
/// control-plane round trip.
pub fn spawn(client: Box<dyn ControlPlaneClient>, poll_interval: Duration) -> Arc<RwLock<Snapshot>> {
    let snapshot = Arc::new(RwLock::new(Snapshot::default()));
    let snapshot_for_thread = Arc::clone(&snapshot);
    thread::spawn(move || loop {
        let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        let previous = snapshot_for_thread.read().unwrap().clone();
        let next = poll_once(client.as_ref(), &previous, now_unix);
        *snapshot_for_thread.write().unwrap() = next;
        thread::sleep(poll_interval);
    });
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_client::FakeControlPlaneClient;
    use keel_controlplane::wire::{NodeState, NodeStatus};

    #[test]
    fn spawn_populates_the_snapshot_from_the_client_within_a_few_poll_intervals() {
        let client = FakeControlPlaneClient::new();
        client.set_nodes(vec![NodeStatus {
            id: "node-1".to_string(),
            addr: "192.168.64.4:7621".to_string(),
            pod_cidr: "10.0.4.0/24".to_string(),
            status: NodeState::Alive,
            last_seen_secs: 1,
            capacity_cpu: 4.0,
            capacity_memory: 8 * 1024 * 1024 * 1024,
            committed_cpu: 1.0,
            committed_memory: 1024 * 1024 * 1024,
            ingresses: vec![],
        }]);

        let snapshot = spawn(Box::new(client), Duration::from_millis(20));

        let mut attempts = 0;
        loop {
            if !snapshot.read().unwrap().nodes.is_empty() {
                break;
            }
            attempts += 1;
            assert!(attempts < 100, "snapshot was never populated by the poller");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(snapshot.read().unwrap().nodes[0].status.id, "node-1");
    }
}
