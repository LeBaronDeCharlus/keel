use crate::replication::{self, ACK_NEED_FULL};
use crate::tls::ReloadingTls;
use crate::worker::Command;
use keel_zfs::ZfsManager;
use rustls::{ClientConnection, StreamOwned};
use std::io::Read;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Spawned once per stateful+replicated jail name from `Command::Apply`
/// (see `worker.rs`) when its spec has both `volumes` and `replicate_to`
/// set. Ticks every `interval`: re-reads `replicate_to` from the live
/// `JailRecord` (so a `PUT /jails/<name>/replicate-to` takes effect on the
/// very next tick with no signal/restart), snapshots the volume, and sends
/// a full or incremental stream to the standby. Exits once the record
/// itself is gone (the replica was deleted) -- checked every tick via
/// `Command::Get`, since nothing should keep replicating a deleted
/// replica's already-orphaned dataset forever.
pub fn spawn<Z: ZfsManager + Clone + Send + 'static>(
    replica_name: String,
    volume_name: String,
    pool: String,
    zfs: Z,
    commands: Sender<Command>,
    interval: Duration,
    tls: Arc<ReloadingTls>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let dataset = crate::record::volume_dataset_path(&pool, &volume_name);
        let mut last_confirmed_sent: Option<String> = None;
        let mut tick: u64 = 0;
        loop {
            thread::sleep(interval);
            tick += 1;

            let (tx, rx) = std::sync::mpsc::channel();
            if commands
                .send(Command::Get(Some(replica_name.clone()), tx))
                .is_err()
            {
                return;
            }
            let Ok(statuses) = rx.recv() else { return };
            let Some(status) = statuses.into_iter().next() else {
                eprintln!("keel-agentd: replica '{replica_name}' no longer exists, stopping its replication loop");
                return;
            };
            let Some(replicate_to) = status.record.spec.spec.replicate_to.clone() else {
                continue; // retargeted away to nothing (shouldn't normally happen); just wait
            };
            let generation = status.record.spec.spec.generation;

            let snapshot_id = format!("keel-repl-{tick}");
            if let Err(e) = zfs.snapshot(&dataset, &snapshot_id) {
                eprintln!(
                    "keel-agentd: failed to snapshot '{dataset}' for replica '{replica_name}': {e}"
                );
                continue;
            }

            match send_once(
                &zfs,
                &replica_name,
                &dataset,
                &snapshot_id,
                last_confirmed_sent.as_deref(),
                &replicate_to,
                generation,
                &tls.client_config(),
            ) {
                Ok(()) => {
                    // Prune the previous incremental base now that a new one
                    // has been confirmed: keep exactly one snapshot per
                    // replica at steady state, no unbounded growth.
                    if let Some(previous) = last_confirmed_sent.as_deref() {
                        if let Err(e) = zfs.destroy_snapshot(&dataset, previous) {
                            eprintln!("keel-agentd: failed to prune previous snapshot '{previous}' for '{dataset}': {e}");
                        }
                    }
                    last_confirmed_sent = Some(snapshot_id);
                }
                Err(SendOnceError::NeedFull) => {
                    eprintln!("keel-agentd: standby for replica '{replica_name}' rejected the incremental base; will send full next tick");
                    last_confirmed_sent = None;
                }
                Err(SendOnceError::Fenced) => {
                    // The standby has already accepted a higher generation
                    // than ours -- we've been promoted away from without
                    // ever learning it (a real partition, not just a
                    // failed connection). Replicating to this standby is no
                    // longer ours to do; stop for good, the same as the
                    // "record deleted" exit path above.
                    eprintln!(
                        "keel-agentd: replica '{replica_name}' generation {generation} was rejected as stale by its standby, stopping its replication loop"
                    );
                    return;
                }
                Err(SendOnceError::Io(e)) => {
                    eprintln!(
                        "keel-agentd: failed to replicate '{replica_name}' to {replicate_to}: {e}"
                    );
                }
            }
        }
    })
}

enum SendOnceError {
    NeedFull,
    Fenced,
    Io(String),
}

// each parameter is independently needed to drive the ZFS send and dial the replica target; bundling into a struct would be over-engineering for this single call site
#[allow(clippy::too_many_arguments)]
fn send_once<Z: ZfsManager>(
    zfs: &Z,
    replica_name: &str,
    dataset: &str,
    snapshot_id: &str,
    base: Option<&str>,
    replicate_to: &str,
    generation: u64,
    client_config: &Arc<rustls::ClientConfig>,
) -> Result<(), SendOnceError> {
    let server_name = crate::tls::server_name_from_addr(replicate_to).map_err(SendOnceError::Io)?;
    let tcp_stream =
        TcpStream::connect(replicate_to).map_err(|e| SendOnceError::Io(e.to_string()))?;
    let conn = ClientConnection::new(Arc::clone(client_config), server_name)
        .map_err(|e| SendOnceError::Io(e.to_string()))?;
    let mut stream = StreamOwned::new(conn, tcp_stream);
    replication::write_header(&mut stream, replica_name, snapshot_id, base, generation)
        .map_err(|e| SendOnceError::Io(e.to_string()))?;
    let mut ack = [0u8; 1];
    stream
        .read_exact(&mut ack)
        .map_err(|e| SendOnceError::Io(e.to_string()))?;
    if ack[0] == ACK_NEED_FULL {
        return Err(SendOnceError::NeedFull);
    }
    if ack[0] == replication::ACK_STALE_GENERATION {
        return Err(SendOnceError::Fenced);
    }
    zfs.send_snapshot(dataset, snapshot_id, base, &mut stream)
        .map_err(|e| SendOnceError::Io(e.to_string()))?;
    stream.sock.shutdown(std::net::Shutdown::Write).ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconciler::Reconciler;
    use crate::worker;
    use keel_jail::{FakeJailRuntime, FakeMountManager};
    use keel_net::FakeNetManager;
    use keel_spec::{Metadata, NetworkSpec, ResourcesSpec, RestartPolicy, Spec, VolumeMount};
    use keel_zfs::FakeZfsManager;
    use std::path::PathBuf;

    fn test_state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-agentd-replication-loop-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn test_reloading_tls() -> Arc<ReloadingTls> {
        ReloadingTls::spawn(
            fixture("fixture-node.crt"),
            fixture("fixture-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap()
    }

    fn stateful_spec(name: &str) -> keel_spec::JailSpec {
        keel_spec::JailSpec {
            api_version: "keel/v1".to_string(),
            kind: "Jail".to_string(),
            metadata: Metadata {
                name: name.to_string(),
            },
            spec: Spec {
                image: "base/14.2-web".to_string(),
                command: vec!["/usr/local/bin/myapp".to_string()],
                network: NetworkSpec {
                    vnet: true,
                    bridge: "keel0".to_string(),
                    address: "10.0.0.5/24".to_string(),
                },
                resources: ResourcesSpec {
                    cpu: "1".to_string(),
                    memory: "256M".to_string(),
                },
                restart_policy: RestartPolicy::Always,
                volumes: vec![VolumeMount {
                    name: format!("{name}-data"),
                    mount_path: "/var/db".to_string(),
                    size: "1G".to_string(),
                }],
                replicate_to: None,
                generation: 0,
            },
        }
    }

    #[test]
    fn a_tick_snapshots_and_sends_a_full_replication_on_first_contact() {
        let dir = test_state_dir("a_tick_snapshots_and_sends_a_full_replication_on_first_contact");
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) =
            worker::spawn(reconciler, zfs.clone(), "zroot".to_string(), None);

        let (apply_tx, apply_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::Apply(stateful_spec("db-0"), apply_tx))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();

        let receiver_zfs = FakeZfsManager::new();
        let receiver_dir = std::env::temp_dir().join("keel-agentd-replication-loop-test-receiver-a_tick_snapshots_and_sends_a_full_replication_on_first_contact");
        let _ = std::fs::remove_dir_all(&receiver_dir);
        let targets = crate::ReplicaTargetRegistry::load(receiver_dir).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let receiver_zfs_clone = receiver_zfs.clone();
        let targets_clone = targets.clone();
        std::thread::spawn(move || {
            crate::replication::run(
                listener,
                receiver_zfs_clone,
                "zroot".to_string(),
                targets_clone,
                test_reloading_tls(),
            )
        });

        let (rt_tx, rt_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::SetReplicateTo(
                "db-0".to_string(),
                Some(addr),
                rt_tx,
            ))
            .unwrap();
        rt_rx.recv().unwrap().unwrap();

        let _handle = spawn(
            "db-0".to_string(),
            "db-0-data".to_string(),
            "zroot".to_string(),
            zfs.clone(),
            commands.clone(),
            Duration::from_millis(50),
            test_reloading_tls(),
        );

        std::thread::sleep(Duration::from_millis(300));
        assert!(receiver_zfs
            .dataset_exists("zroot/keel/volumes/db-0-data")
            .unwrap());
        // The wire protocol's `replica_name` header field carries the plain
        // replica/jail name ("db-0"), matching `Standbys`, `PendingFences`,
        // `Placements`, and force-repin's own probe -- see
        // `replication::handle_connection`, which reconstructs the volume
        // name ("db-0-data") from this same field.
        assert!(targets
            .get("db-0")
            .is_some_and(|t| t.last_snapshot.is_some()));
    }

    #[test]
    fn a_successful_send_prunes_the_previous_confirmed_snapshot() {
        // After two successful ticks, exactly one snapshot should survive on
        // the sender's dataset: the second tick's send (which used tick 1's
        // snapshot as its incremental base) must prune tick 1's snapshot
        // once it's confirmed no longer needed, per the design spec's "keep
        // exactly one" invariant.
        let dir = test_state_dir("a_successful_send_prunes_the_previous_confirmed_snapshot");
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) =
            worker::spawn(reconciler, zfs.clone(), "zroot".to_string(), None);

        let (apply_tx, apply_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::Apply(stateful_spec("db-2"), apply_tx))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();

        let receiver_zfs = FakeZfsManager::new();
        let receiver_dir = std::env::temp_dir().join("keel-agentd-replication-loop-test-receiver-a_successful_send_prunes_the_previous_confirmed_snapshot");
        let _ = std::fs::remove_dir_all(&receiver_dir);
        let targets = crate::ReplicaTargetRegistry::load(receiver_dir).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let receiver_zfs_clone = receiver_zfs.clone();
        let targets_clone = targets.clone();
        std::thread::spawn(move || {
            crate::replication::run(
                listener,
                receiver_zfs_clone,
                "zroot".to_string(),
                targets_clone,
                test_reloading_tls(),
            )
        });

        let (rt_tx, rt_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::SetReplicateTo(
                "db-2".to_string(),
                Some(addr),
                rt_tx,
            ))
            .unwrap();
        rt_rx.recv().unwrap().unwrap();

        // 200ms interval: ticks land at ~200ms and ~400ms. Asserting at
        // 450ms falls comfortably after the second tick's send completes
        // but well before the third (~600ms), so exactly two successful
        // sends have happened.
        let _handle = spawn(
            "db-2".to_string(),
            "db-2-data".to_string(),
            "zroot".to_string(),
            zfs.clone(),
            commands.clone(),
            Duration::from_millis(200),
            test_reloading_tls(),
        );
        std::thread::sleep(Duration::from_millis(450));

        let dataset = "zroot/keel/volumes/db-2-data";
        let mut discard = Vec::new();
        assert!(
            matches!(zfs.send_snapshot(dataset, "keel-repl-1", None, &mut discard), Err(keel_zfs::ZfsError::NotFound(_))),
            "expected the first tick's snapshot to have been pruned after the second tick's successful send"
        );
        assert!(
            zfs.send_snapshot(dataset, "keel-repl-2", None, &mut discard)
                .is_ok(),
            "expected the second tick's snapshot to still exist as the current incremental base"
        );
    }

    #[test]
    fn the_loop_exits_once_its_replica_record_is_deleted() {
        // Exercises the "record deleted" exit path directly rather than only
        // assuming it: deletes the replica right after applying it (before
        // spawning the loop), then asserts the spawned thread's JoinHandle
        // finishes on its own within a bounded wait -- if the loop kept
        // looping forever (bug: exit condition never firing), `join()` would
        // hang and this test would time out instead of completing.
        let dir = test_state_dir("the_loop_exits_once_its_replica_record_is_deleted");
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) =
            worker::spawn(reconciler, zfs.clone(), "zroot".to_string(), None);

        let (apply_tx, apply_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::Apply(stateful_spec("db-1"), apply_tx))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();

        let (del_tx, del_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::Delete("db-1".to_string(), del_tx))
            .unwrap();
        del_rx.recv().unwrap().unwrap();

        let handle = spawn(
            "db-1".to_string(),
            "db-1-data".to_string(),
            "zroot".to_string(),
            zfs.clone(),
            commands.clone(),
            Duration::from_millis(20),
            test_reloading_tls(),
        );

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = handle.join();
            let _ = done_tx.send(());
        });
        done_rx.recv_timeout(Duration::from_secs(5)).expect(
            "expected the replication loop thread to exit once its replica record was deleted",
        );
    }

    #[test]
    fn the_loop_exits_once_the_standby_rejects_its_generation_as_stale() {
        // Simulates a partitioned former-primary that never learned it was
        // fenced: its own spec still carries generation 0 (stateful_spec's
        // default), but the standby has already accepted a higher
        // generation from the real, promoted primary. The loop must stop
        // entirely rather than keep retrying forever against a standby
        // that will never accept it again.
        let dir = test_state_dir("the_loop_exits_once_the_standby_rejects_its_generation_as_stale");
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_worker_handle, commands) =
            worker::spawn(reconciler, zfs.clone(), "zroot".to_string(), None);

        let (apply_tx, apply_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::Apply(stateful_spec("db-4"), apply_tx))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();

        let receiver_zfs = FakeZfsManager::new();
        let receiver_dir = std::env::temp_dir().join("keel-agentd-replication-loop-test-receiver-the_loop_exits_once_the_standby_rejects_its_generation_as_stale");
        let _ = std::fs::remove_dir_all(&receiver_dir);
        let targets = crate::ReplicaTargetRegistry::load(receiver_dir).unwrap();
        // Seed the standby as if it already accepted a real primary at
        // generation 1 -- our sender's spec is still generation 0.
        targets.ensure_for_test("db-4", "zroot/keel/volumes/db-4-data", "127.0.0.1:0");
        targets.accept_generation_for_test("db-4", 1);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let receiver_zfs_clone = receiver_zfs.clone();
        let targets_clone = targets.clone();
        std::thread::spawn(move || {
            crate::replication::run(
                listener,
                receiver_zfs_clone,
                "zroot".to_string(),
                targets_clone,
                test_reloading_tls(),
            )
        });

        let (rt_tx, rt_rx) = std::sync::mpsc::channel();
        commands
            .send(Command::SetReplicateTo(
                "db-4".to_string(),
                Some(addr),
                rt_tx,
            ))
            .unwrap();
        rt_rx.recv().unwrap().unwrap();

        let handle = spawn(
            "db-4".to_string(),
            "db-4-data".to_string(),
            "zroot".to_string(),
            zfs.clone(),
            commands.clone(),
            Duration::from_millis(20),
            test_reloading_tls(),
        );

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = handle.join();
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("expected the replication loop thread to exit once its generation was rejected as stale");
    }
}
