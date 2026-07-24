use crate::reconciler::{ReconcileError, Reconciler};
use crate::tls::ReloadingTls;
use crate::wire::JailStatus;
use keel_jail::{JailRuntime, MountManager};
use keel_net::NetManager;
use keel_spec::{IngressSpec, JailSpec};
use keel_zfs::ZfsManager;
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub enum Command {
    Apply(JailSpec, Sender<Result<(), ReconcileError>>),
    Get(Option<String>, Sender<Vec<JailStatus>>),
    Delete(String, Sender<Result<(), ReconcileError>>),
    Tick,
    CommittedResources(Sender<(f64, u64)>),
    AddRoute(String, String, Sender<Result<(), keel_net::NetError>>),
    RemoveRoute(String, Sender<Result<(), keel_net::NetError>>),
    AddServiceAlias(String, String, Sender<Result<(), keel_net::NetError>>),
    RemoveServiceAlias(String, String, Sender<Result<(), keel_net::NetError>>),
    GetVolume(String, Sender<Result<(), ReconcileError>>),
    DeleteVolume(String, Sender<Result<(), ReconcileError>>),
    ListVolumes(Sender<Result<Vec<crate::wire::VolumeStatus>, ReconcileError>>),
    SetReplicateTo(String, Option<String>, Sender<Result<(), ReconcileError>>),
    /// Re-spawns a replication loop for every stateful+replicated jail whose
    /// record was loaded from on-disk state without ever going through
    /// `Command::Apply` in this process (i.e. right after a restart). Sent
    /// once at startup by `main.rs`, right after `worker::spawn` returns.
    ResumeReplicationLoops(Sender<()>),
    ApplyIngress(IngressSpec, Sender<Result<(), ReconcileError>>),
    GetIngress(Option<String>, Sender<Vec<crate::wire::IngressStatus>>),
    DeleteIngress(String, Sender<Result<(), ReconcileError>>),
}

/// `replicate_tls` is the TLS material the replication *sender* side
/// (`replication_loop::spawn`) authenticates itself with -- `None` in tests
/// that never apply a stateful+replicated spec (the vast majority), `Some`
/// wherever a real cluster or a test specifically exercises replication,
/// mirroring the same `ReloadingTls` the replication *listener* itself
/// verifies incoming connections against.
pub fn spawn<J, Z, N, M>(
    mut reconciler: Reconciler<J, Z, N, M>,
    zfs: Z,
    pool: String,
    replicate_tls: Option<Arc<ReloadingTls>>,
) -> (JoinHandle<()>, Sender<Command>)
where
    J: JailRuntime + Send + 'static,
    Z: ZfsManager + Clone + Send + 'static,
    N: NetManager + Send + 'static,
    M: MountManager + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Command>();
    let commands_for_thread = tx.clone();
    let handle = thread::spawn(move || {
        let mut replicating: std::collections::HashSet<String> = std::collections::HashSet::new();
        for command in rx {
            handle_command(&mut reconciler, command, &zfs, &pool, &commands_for_thread, &mut replicating, &replicate_tls);
        }
    });
    (handle, tx)
}

#[allow(clippy::too_many_arguments)]
fn handle_command<J: JailRuntime, Z: ZfsManager + Clone + Send + 'static, N: NetManager, M: MountManager>(
    reconciler: &mut Reconciler<J, Z, N, M>,
    command: Command,
    zfs: &Z,
    pool: &str,
    commands: &Sender<Command>,
    replicating: &mut std::collections::HashSet<String>,
    replicate_tls: &Option<Arc<ReloadingTls>>,
) {
    match command {
        Command::Apply(spec, reply) => {
            let is_stateful_and_replicated = !spec.spec.volumes.is_empty() && spec.spec.replicate_to.is_some();
            let name = spec.metadata.name.clone();
            let result = reconciler.apply(spec);
            // Reconcile immediately so a client's apply/delete call
            // observes its effects by the time it gets a response,
            // rather than waiting for the next timer tick. The resulting
            // per-jail failures (if any) are already surfaced via each
            // jail's backoff status on a later `get`, so they're
            // discarded here (same treatment as a plain `Tick`).
            let _ = reconciler.reconcile(Instant::now());
            if result.is_ok() && is_stateful_and_replicated && replicating.insert(name.clone()) {
                match replicate_tls {
                    Some(tls) => {
                        let volume_name = format!("{name}-data");
                        crate::replication_loop::spawn(name, volume_name, pool.to_string(), zfs.clone(), commands.clone(), Duration::from_secs(30), Arc::clone(tls));
                    }
                    None => eprintln!("keel-agentd: '{name}' is stateful and replicated, but no replication TLS configuration is available -- not starting its replication loop"),
                }
            }
            let _ = reply.send(result);
        }
        Command::Delete(name, reply) => {
            let result = reconciler.delete(&name);
            if result.is_ok() {
                // Only once the delete has genuinely completed (not merely
                // been attempted -- `delete` returns `Err` and leaves the
                // record marked `deleting` on a transient failure, to be
                // resumed by a later reconcile) -- otherwise a name could be
                // freed up for re-insertion while its old replication loop
                // is still shutting down.
                replicating.remove(&name);
            }
            let _ = reconciler.reconcile(Instant::now());
            let _ = reply.send(result);
        }
        Command::Get(name, reply) => {
            let now = Instant::now();
            let statuses = match name {
                Some(n) => reconciler.get(&n, now).into_iter().collect(),
                None => reconciler.list(now),
            };
            let _ = reply.send(statuses);
        }
        Command::Tick => {
            for (name, error) in reconciler.reconcile(Instant::now()) {
                eprintln!("keel-agentd: reconcile error for jail '{name}': {error}");
            }
        }
        Command::CommittedResources(reply) => {
            let _ = reply.send(reconciler.committed_resources());
        }
        Command::AddRoute(subnet, gateway_addr, reply) => {
            let _ = reply.send(reconciler.add_route(&subnet, &gateway_addr));
        }
        Command::RemoveRoute(subnet, reply) => {
            let _ = reply.send(reconciler.remove_route(&subnet));
        }
        Command::AddServiceAlias(bridge, address, reply) => {
            let _ = reply.send(reconciler.add_alias(&bridge, &address));
        }
        Command::RemoveServiceAlias(bridge, address, reply) => {
            let _ = reply.send(reconciler.remove_alias(&bridge, &address));
        }
        Command::GetVolume(name, reply) => {
            let _ = reply.send(reconciler.get_volume(&name));
        }
        Command::DeleteVolume(name, reply) => {
            let _ = reply.send(reconciler.delete_volume(&name));
        }
        Command::ListVolumes(reply) => {
            let _ = reply.send(reconciler.list_volumes());
        }
        Command::SetReplicateTo(name, replicate_to, reply) => {
            let _ = reply.send(reconciler.set_replicate_to(&name, replicate_to));
        }
        Command::ResumeReplicationLoops(reply) => {
            for status in reconciler.list(Instant::now()) {
                let spec = &status.record.spec.spec;
                let name = status.record.spec.metadata.name.clone();
                if !spec.volumes.is_empty() && spec.replicate_to.is_some() && replicating.insert(name.clone()) {
                    match replicate_tls {
                        Some(tls) => {
                            let volume_name = format!("{name}-data");
                            crate::replication_loop::spawn(name, volume_name, pool.to_string(), zfs.clone(), commands.clone(), Duration::from_secs(30), Arc::clone(tls));
                        }
                        None => eprintln!("keel-agentd: '{name}' is stateful and replicated, but no replication TLS configuration is available -- not resuming its replication loop"),
                    }
                }
            }
            let _ = reply.send(());
        }
        Command::ApplyIngress(spec, reply) => {
            let result = reconciler.apply_ingress(spec);
            let _ = reconciler.reconcile(Instant::now());
            let _ = reply.send(result);
        }
        Command::GetIngress(name, reply) => {
            let statuses = match name {
                Some(n) => reconciler
                    .get_ingress(&n)
                    .map(|record| crate::wire::IngressStatus { record })
                    .into_iter()
                    .collect(),
                None => reconciler.list_ingress().into_iter().map(|record| crate::wire::IngressStatus { record }).collect(),
            };
            let _ = reply.send(statuses);
        }
        Command::DeleteIngress(name, reply) => {
            let result = reconciler.delete_ingress(&name);
            let _ = reconciler.reconcile(Instant::now());
            let _ = reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_jail::{FakeJailRuntime, FakeMountManager};
    use keel_net::FakeNetManager;
    use keel_spec::{Metadata, NetworkSpec, RestartPolicy, ResourcesSpec, Spec, VolumeMount};
    use keel_zfs::FakeZfsManager;
    use std::path::PathBuf;

    fn sample_spec(name: &str) -> JailSpec {
        JailSpec {
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
                generation: 0,
            },
        }
    }

    fn test_state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-agentd-worker-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn spawn_test_worker(name: &str) -> Sender<Command> {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            test_state_dir(name),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (_handle, commands) = spawn(reconciler, zfs, "zroot".to_string(), None);
        commands
    }

    #[test]
    fn apply_command_persists_and_reconciles_immediately() {
        let commands = spawn_test_worker("apply_command_persists_and_reconciles_immediately");

        let (reply_tx, reply_rx) = mpsc::channel();
        commands.send(Command::Apply(sample_spec("web-1"), reply_tx)).unwrap();
        assert!(reply_rx.recv().unwrap().is_ok());

        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::Get(Some("web-1".to_string()), get_tx)).unwrap();
        let statuses = get_rx.recv().unwrap();
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].running, "expected apply to trigger an immediate reconcile that provisions the jail");
    }

    #[test]
    fn invalid_apply_command_returns_an_error_without_crashing_the_worker() {
        let commands = spawn_test_worker("invalid_apply_command_returns_an_error_without_crashing_the_worker");
        let mut invalid = sample_spec("web-1");
        invalid.metadata.name = "Invalid_Name".to_string();

        let (reply_tx, reply_rx) = mpsc::channel();
        commands.send(Command::Apply(invalid, reply_tx)).unwrap();
        assert!(matches!(reply_rx.recv().unwrap(), Err(ReconcileError::InvalidSpec(_))));

        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::Get(None, get_tx)).unwrap();
        assert!(get_rx.recv().unwrap().is_empty());
    }

    #[test]
    fn delete_command_removes_the_record() {
        let commands = spawn_test_worker("delete_command_removes_the_record");

        let (apply_tx, apply_rx) = mpsc::channel();
        commands.send(Command::Apply(sample_spec("web-1"), apply_tx)).unwrap();
        apply_rx.recv().unwrap().unwrap();

        let (delete_tx, delete_rx) = mpsc::channel();
        commands.send(Command::Delete("web-1".to_string(), delete_tx)).unwrap();
        assert!(delete_rx.recv().unwrap().is_ok());

        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::Get(None, get_tx)).unwrap();
        assert!(get_rx.recv().unwrap().is_empty());
    }

    #[test]
    fn delete_command_on_unknown_name_returns_not_found() {
        let commands = spawn_test_worker("delete_command_on_unknown_name_returns_not_found");
        let (delete_tx, delete_rx) = mpsc::channel();
        commands.send(Command::Delete("missing".to_string(), delete_tx)).unwrap();
        assert!(matches!(delete_rx.recv().unwrap(), Err(ReconcileError::NotFound(_))));
    }

    #[test]
    fn tick_command_is_processed_without_blocking_subsequent_commands() {
        let commands = spawn_test_worker("tick_command_is_processed_without_blocking_subsequent_commands");
        commands.send(Command::Tick).unwrap();

        // mpsc is FIFO: this Get is only answered once Tick has already
        // been processed, proving Tick doesn't hang or crash the worker.
        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::Get(None, get_tx)).unwrap();
        assert!(get_rx.recv().unwrap().is_empty());
    }

    #[test]
    fn committed_resources_command_returns_the_reconcilers_totals() {
        let commands = spawn_test_worker("committed_resources_command_returns_the_reconcilers_totals");

        let (apply_tx, apply_rx) = mpsc::channel();
        commands.send(Command::Apply(sample_spec("web-1"), apply_tx)).unwrap();
        apply_rx.recv().unwrap().unwrap();

        let (tx, rx) = mpsc::channel();
        commands.send(Command::CommittedResources(tx)).unwrap();
        // sample_spec's fixed resources: cpu "2", memory "512M".
        assert_eq!(rx.recv().unwrap(), (2.0, 512 * 1024 * 1024));
    }

    #[test]
    fn add_route_command_calls_through_to_the_net_manager() {
        let commands = spawn_test_worker("add_route_command_calls_through_to_the_net_manager");

        let (tx, rx) = mpsc::channel();
        commands.send(Command::AddRoute("10.0.5.0/24".to_string(), "192.168.64.5".to_string(), tx)).unwrap();
        assert!(rx.recv().unwrap().is_ok());
    }

    #[test]
    fn remove_route_command_calls_through_to_the_net_manager() {
        let commands = spawn_test_worker("remove_route_command_calls_through_to_the_net_manager");

        let (add_tx, add_rx) = mpsc::channel();
        commands.send(Command::AddRoute("10.0.5.0/24".to_string(), "192.168.64.5".to_string(), add_tx)).unwrap();
        add_rx.recv().unwrap().unwrap();

        let (rm_tx, rm_rx) = mpsc::channel();
        commands.send(Command::RemoveRoute("10.0.5.0/24".to_string(), rm_tx)).unwrap();
        assert!(rm_rx.recv().unwrap().is_ok());
    }

    #[test]
    fn add_service_alias_command_round_trips() {
        let zfs = FakeZfsManager::new();
        let reconciler = crate::Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            std::env::temp_dir().join("keel-agentd-worker-test-add_service_alias_command_round_trips"),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let _net = FakeNetManager::new();
        // Reconciler owns its own NetManager instance internally; assert
        // through the command channel's observable success instead of a
        // second handle to the same fake.
        let (_worker_handle, commands) = spawn(reconciler, zfs, "zroot".to_string(), None);

        let (tx, rx) = mpsc::channel();
        commands.send(Command::AddServiceAlias("keel0".to_string(), "10.0.250.7".to_string(), tx)).unwrap();
        assert!(rx.recv().unwrap().is_ok());

        let (tx2, rx2) = mpsc::channel();
        commands.send(Command::RemoveServiceAlias("keel0".to_string(), "10.0.250.7".to_string(), tx2)).unwrap();
        assert!(rx2.recv().unwrap().is_ok());
    }

    #[test]
    fn get_volume_and_delete_volume_commands_round_trip() {
        let commands = spawn_test_worker("get_volume_and_delete_volume_commands_round_trip");

        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::GetVolume("web-data".to_string(), get_tx)).unwrap();
        assert!(matches!(get_rx.recv().unwrap(), Err(ReconcileError::Zfs(keel_zfs::ZfsError::NotFound(_)))));

        let (del_tx, del_rx) = mpsc::channel();
        commands.send(Command::DeleteVolume("web-data".to_string(), del_tx)).unwrap();
        assert!(matches!(del_rx.recv().unwrap(), Err(ReconcileError::Zfs(keel_zfs::ZfsError::NotFound(_)))));
    }

    #[test]
    fn list_volumes_command_returns_every_provisioned_volume() {
        let commands = spawn_test_worker("list_volumes_command_returns_every_provisioned_volume");

        let (apply_tx, apply_rx) = mpsc::channel();
        commands
            .send(Command::Apply(
                keel_spec::JailSpec {
                    api_version: "keel/v1".to_string(),
                    kind: "Jail".to_string(),
                    metadata: keel_spec::Metadata { name: "web-1".to_string() },
                    spec: keel_spec::Spec {
                        image: "base/14.2-web".to_string(),
                        command: vec!["/usr/local/bin/myapp".to_string()],
                        network: keel_spec::NetworkSpec { vnet: true, bridge: "keel0".to_string(), address: "10.0.0.5/24".to_string() },
                        resources: keel_spec::ResourcesSpec { cpu: "1".to_string(), memory: "256M".to_string() },
                        restart_policy: keel_spec::RestartPolicy::Always,
                        volumes: vec![keel_spec::VolumeMount {
                            name: "web-data".to_string(),
                            mount_path: "/data".to_string(),
                            size: "1G".to_string(),
                        }],
                        replicate_to: None,
                        generation: 0,
                    },
                },
                apply_tx,
            ))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();

        let (tx, rx) = mpsc::channel();
        commands.send(Command::ListVolumes(tx)).unwrap();
        let volumes = rx.recv().unwrap().unwrap();
        assert_eq!(volumes, vec![crate::wire::VolumeStatus { name: "web-data".to_string() }]);
    }

    fn sample_ingress_spec(name: &str) -> IngressSpec {
        IngressSpec {
            api_version: "keel/v1".to_string(),
            kind: "Ingress".to_string(),
            metadata: Metadata { name: name.to_string() },
            spec: keel_spec::IngressSpecBody {
                host: "example.com".to_string(),
                backend: keel_spec::IngressBackend { service: "hugo-site".to_string(), port: 8080 },
                tls: keel_spec::IngressTls { email: "admin@example.com".to_string() },
            },
        }
    }

    #[test]
    fn apply_ingress_command_persists_and_lists_it() {
        let commands = spawn_test_worker("apply_ingress_command_persists_and_lists_it");

        let (reply_tx, reply_rx) = mpsc::channel();
        commands.send(Command::ApplyIngress(sample_ingress_spec("blog"), reply_tx)).unwrap();
        assert!(reply_rx.recv().unwrap().is_ok());

        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::GetIngress(Some("blog".to_string()), get_tx)).unwrap();
        let statuses = get_rx.recv().unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].record.spec.spec.host, "example.com");
    }

    #[test]
    fn get_ingress_command_with_no_name_lists_everything() {
        let commands = spawn_test_worker("get_ingress_command_with_no_name_lists_everything");
        let (apply_tx, apply_rx) = mpsc::channel();
        commands.send(Command::ApplyIngress(sample_ingress_spec("blog"), apply_tx)).unwrap();
        apply_rx.recv().unwrap().unwrap();

        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::GetIngress(None, get_tx)).unwrap();
        assert_eq!(get_rx.recv().unwrap().len(), 1);
    }

    #[test]
    fn get_ingress_command_on_unknown_name_returns_empty() {
        let commands = spawn_test_worker("get_ingress_command_on_unknown_name_returns_empty");
        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::GetIngress(Some("missing".to_string()), get_tx)).unwrap();
        assert!(get_rx.recv().unwrap().is_empty());
    }

    #[test]
    fn delete_ingress_command_removes_the_record() {
        let commands = spawn_test_worker("delete_ingress_command_removes_the_record");
        let (apply_tx, apply_rx) = mpsc::channel();
        commands.send(Command::ApplyIngress(sample_ingress_spec("blog"), apply_tx)).unwrap();
        apply_rx.recv().unwrap().unwrap();

        let (delete_tx, delete_rx) = mpsc::channel();
        commands.send(Command::DeleteIngress("blog".to_string(), delete_tx)).unwrap();
        assert!(delete_rx.recv().unwrap().is_ok());

        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::GetIngress(None, get_tx)).unwrap();
        assert!(get_rx.recv().unwrap().is_empty());
    }

    #[test]
    fn delete_ingress_command_on_unknown_name_returns_not_found() {
        let commands = spawn_test_worker("delete_ingress_command_on_unknown_name_returns_not_found");
        let (delete_tx, delete_rx) = mpsc::channel();
        commands.send(Command::DeleteIngress("missing".to_string(), delete_tx)).unwrap();
        assert!(matches!(delete_rx.recv().unwrap(), Err(ReconcileError::NotFound(_))));
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn test_reloading_tls() -> Arc<ReloadingTls> {
        ReloadingTls::spawn(fixture("fixture-node.crt"), fixture("fixture-node.key"), fixture("ca.crt"), fixture("crl.pem"), Duration::from_secs(3600))
            .unwrap()
    }

    fn stateful_replicated_spec(name: &str, replicate_to: &str) -> JailSpec {
        JailSpec {
            api_version: "keel/v1".to_string(),
            kind: "Jail".to_string(),
            metadata: Metadata { name: name.to_string() },
            spec: Spec {
                image: "base/14.2-web".to_string(),
                command: vec!["/usr/local/bin/myapp".to_string()],
                network: NetworkSpec { vnet: true, bridge: "keel0".to_string(), address: "10.0.0.6/24".to_string() },
                resources: ResourcesSpec { cpu: "1".to_string(), memory: "256M".to_string() },
                restart_policy: RestartPolicy::Always,
                volumes: vec![VolumeMount { name: format!("{name}-data"), mount_path: "/var/db".to_string(), size: "1G".to_string() }],
                replicate_to: Some(replicate_to.to_string()),
                generation: 0,
            },
        }
    }

    #[test]
    fn deleting_a_stateful_replicated_jail_clears_it_from_the_replicating_guard() {
        // Reproduces "replication is permanently lost after delete+recreate
        // of the same jail name in one daemon run": `replicating` gates
        // `replication_loop::spawn` against double-spawning, but if `Delete`
        // never clears a name out of it, re-applying the same
        // stateful+replicated name later in the same process silently
        // spawns no new loop at all. Drives `handle_command` directly
        // (rather than through the 30s-interval-hardcoded `spawn()` path)
        // so this stays a fast, direct test of the guard itself.
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");
        let mut reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs.clone(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            test_state_dir("deleting_a_stateful_replicated_jail_clears_it_from_the_replicating_guard"),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let (commands, _rx) = mpsc::channel();
        let mut replicating: std::collections::HashSet<String> = std::collections::HashSet::new();
        let tls = Some(test_reloading_tls());

        let (apply_tx, apply_rx) = mpsc::channel();
        handle_command(&mut reconciler, Command::Apply(stateful_replicated_spec("db-0", "10.0.0.9:7622"), apply_tx), &zfs, "zroot", &commands, &mut replicating, &tls);
        apply_rx.recv().unwrap().unwrap();
        assert!(replicating.contains("db-0"), "expected the initial apply to start tracking its replication loop");

        let (delete_tx, delete_rx) = mpsc::channel();
        handle_command(&mut reconciler, Command::Delete("db-0".to_string(), delete_tx), &zfs, "zroot", &commands, &mut replicating, &tls);
        delete_rx.recv().unwrap().unwrap();
        assert!(!replicating.contains("db-0"), "delete must clear the name out of the replicating guard");

        // Re-applying the same name later must be able to start a fresh
        // loop -- i.e. `replicating.insert` must return `true` again, not
        // silently no-op forever because the name was never removed.
        let (reapply_tx, reapply_rx) = mpsc::channel();
        handle_command(&mut reconciler, Command::Apply(stateful_replicated_spec("db-0", "10.0.0.9:7622"), reapply_tx), &zfs, "zroot", &commands, &mut replicating, &tls);
        reapply_rx.recv().unwrap().unwrap();
        assert!(replicating.contains("db-0"), "expected the re-apply to start tracking a new replication loop");
    }

    #[test]
    fn resume_replication_loops_starts_a_loop_for_a_record_persisted_before_a_restart() {
        // Proves the fix for "replication never resumes after a restart":
        // apply a stateful+replicated spec directly against a `Reconciler`
        // (bypassing `worker::spawn`'s `Command::Apply` path entirely, so
        // no replication loop is spawned yet -- simulating a record that
        // was already on disk from a previous process), then build a
        // *fresh* `worker::spawn` over a *fresh* `Reconciler::new` pointed
        // at that same state_dir (so it loads the persisted record with an
        // empty `replicating` set, exactly as a real restart would), send
        // `Command::ResumeReplicationLoops`, and confirm a replication loop
        // actually starts and completes a real send.
        let dir = test_state_dir("resume_replication_loops_starts_a_loop_for_a_record_persisted_before_a_restart");
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/14.2-web");

        let receiver_zfs = FakeZfsManager::new();
        let receiver_dir = std::env::temp_dir()
            .join("keel-agentd-worker-test-receiver-resume_replication_loops_starts_a_loop_for_a_record_persisted_before_a_restart");
        let _ = std::fs::remove_dir_all(&receiver_dir);
        let targets = crate::ReplicaTargetRegistry::load(receiver_dir).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let receiver_zfs_clone = receiver_zfs.clone();
        let targets_clone = targets.clone();
        let reloading_tls = test_reloading_tls();
        std::thread::spawn(move || crate::replication::run(listener, receiver_zfs_clone, "zroot".to_string(), targets_clone, reloading_tls));

        // The "previous process": apply and reconcile directly against a
        // `Reconciler`, never touching `worker::spawn` or `Command::Apply`,
        // so nothing spawns a replication loop here.
        {
            let mut reconciler = crate::reconciler::Reconciler::new(
                FakeJailRuntime::new(),
                zfs.clone(),
                FakeNetManager::new(),
                FakeMountManager::new(),
                "zroot".to_string(),
                dir.clone(),
                Box::new(keel_ingress::FakeAcmeClient::new()),
                Box::new(keel_ingress::FakeDnsProvider::new()),
                Box::new(crate::nginx::FakeNginxController::new()),
                crate::ServiceVipSlot::new(),
            )
            .unwrap();
            reconciler.apply(stateful_replicated_spec("db-resume", &addr)).unwrap();
            let failures = reconciler.reconcile(Instant::now());
            assert!(failures.is_empty(), "expected the initial reconcile to provision the jail and its volume cleanly: {failures:?}");
        }

        // "The restart": a fresh `Reconciler::new` over the same state_dir
        // (loads the persisted record with an empty `replicating` set) and
        // a fresh `worker::spawn` over it.
        let restarted_reconciler = crate::reconciler::Reconciler::new(
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
        let (_worker_handle, commands) = spawn(restarted_reconciler, zfs.clone(), "zroot".to_string(), Some(test_reloading_tls()));

        let (resume_tx, resume_rx) = mpsc::channel();
        commands.send(Command::ResumeReplicationLoops(resume_tx)).unwrap();
        resume_rx.recv().unwrap();

        // The resumed loop uses the same 30s tick interval as a
        // freshly-`Apply`'d replica's loop, so poll for the send's effect
        // (rather than a single blind sleep) with a generous upper bound.
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        loop {
            if receiver_zfs.dataset_exists("zroot/keel/volumes/db-resume-data").unwrap_or(false) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "expected a replication loop resumed after a simulated restart to complete a real send within the deadline"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
        assert!(targets.get("db-resume").is_some_and(|t| t.last_snapshot.is_some()));
    }
}
