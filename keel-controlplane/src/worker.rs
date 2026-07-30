use crate::addresses::{self, UsedAddresses};
use crate::cordoned::Cordoned;
use crate::pending_fences::PendingFences;
use crate::placements::Placements;
use crate::registry::{PodCidrCollision, Registry, ResolveError, UnknownNode};
use crate::scheduler::{self, ScheduleError};
use crate::services::{self, Owner, Services};
use crate::standbys::Standbys;
use crate::wire;
use crate::wire::{NodeState, NodeStatus};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleOrResolveError {
    #[error(transparent)]
    Schedule(#[from] ScheduleError),
    #[error(transparent)]
    Resolve(#[from] ResolveError),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    #[error("no known placement for jail '{0}'")]
    NotPlaced(String),
    #[error(transparent)]
    Resolve(#[from] ResolveError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForceRepinPrep {
    pub old_node_id: String,
    pub old_node_last_known_addr: Option<String>,
    pub standby_node_id: String,
    pub standby_addr: String,
    pub template: keel_spec::JailTemplate,
    pub fresh_standby_node_id: String,
    pub fresh_standby_addr: String,
    pub address: std::net::Ipv4Addr,
    pub prefix_len: u8,
    /// The generation the promoted standby's `JailSpec` must carry -- see
    /// `Placements::generation`'s doc comment. Always strictly higher than
    /// whatever generation the old primary was running, so the old
    /// primary's replication loop (if it's merely partitioned rather than
    /// actually dead, and so never learns of this promotion at all) gets
    /// rejected by the standby's own wire-protocol check the moment it
    /// tries to reconnect.
    pub next_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ForceRepinError {
    #[error("no known placement for replica '{0}'")]
    NotPlaced(String),
    #[error("'{0}' is not a stateful replica with a standby")]
    NotStateful(String),
    #[error("current primary node '{0}' still resolves as alive")]
    PrimaryStillAlive(String),
    #[error("standby node is unreachable: {0}")]
    StandbyUnresolvable(ResolveError),
    #[error("no alive node available to serve as a fresh standby")]
    NoFreshStandby,
    #[error("no free address available for the promoted primary")]
    NoFreeAddress,
}

// Schedule/TearDown are both short-lived per-reconcile values, not stored in bulk; boxing the
// larger variant would add an indirection with no measurable benefit here.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaAction {
    Schedule {
        replica_name: String,
        node_id: String,
        node_addr: String,
        template: keel_spec::JailTemplate,
        address: std::net::Ipv4Addr,
        prefix_len: u8,
        standby_node_id: Option<String>,
        standby_addr: Option<String>,
        generation: u64,
    },
    TearDown {
        replica_name: String,
        node_id: String,
        node_addr: String,
    },
}

pub enum Command {
    Register(
        String,
        String,
        Option<String>,
        f64,
        u64,
        Sender<Result<ipnet::Ipv4Net, PodCidrCollision>>,
    ),
    Heartbeat(
        String,
        f64,
        u64,
        Vec<crate::wire::JailHealth>,
        Vec<crate::wire::IngressHealth>,
        Sender<Result<(), UnknownNode>>,
    ),
    List(Sender<Vec<NodeStatus>>),
    Resolve(String, Sender<Result<String, ResolveError>>),
    ResolveOrSchedule(
        String,
        Sender<Result<(String, String), ScheduleOrResolveError>>,
    ),
    ResolvePlacement(String, Sender<Result<(String, String), PlacementError>>),
    RecordPlacement(String, String, Sender<()>),
    RemovePlacement(String, Sender<()>),
    OwnerOf(String, Sender<Option<Owner>>),
    ApplyService(
        String,
        u32,
        keel_spec::JailTemplate,
        u16,
        Sender<Result<(), services::ApplyServiceError>>,
    ),
    /// Computes a point-in-time `Vec<ReplicaAction>` from the current
    /// `placements`/`registry`/`used_addresses` snapshot, but reserves
    /// nothing: nothing is recorded in shared state here. The caller
    /// (`reconcile_and_execute` in `http.rs`) only records the outcome, via
    /// `RecordPlacement`/`RecordReplicaAddress`, after each computed action
    /// has been executed and confirmed with a real network round-trip
    /// (`forward()`) to the target node. Because `keel-controlplane` handles
    /// each incoming connection on its own thread, two heartbeats (or a
    /// heartbeat racing a `Service` apply) that arrive close together can
    /// each send `ReconcileServices` and get a snapshot computed before
    /// either has recorded its results.
    ///
    /// This is a known, accepted limitation of the reconcile-then-execute
    /// design from the Milestone 15 spec, not a bug to fix here. In the
    /// common case it is harmless: if nothing else changes between the two
    /// computations, both are deterministic and pick the same node/address
    /// for the same replica, so the duplicate `PUT` is an idempotent no-op
    /// and the duplicate `RecordPlacement`/`RecordReplicaAddress` calls just
    /// overwrite the same values. It is not self-correcting, however, if a
    /// resource-committing write (e.g. another node's heartbeat updating its
    /// own `committed_cpu`/`committed_memory`) lands between the two racing
    /// computations: the scheduler's node ranking can then differ between
    /// them, so the two computations can pick *different* nodes for the same
    /// missing replica index. Both `forward()` calls can succeed
    /// independently, creating two real jails for one logical replica on two
    /// different nodes; since `RecordPlacement`/`RecordReplicaAddress` are
    /// simple last-write-wins overwrites, only one placement survives in the
    /// control plane's bookkeeping, and the other node's jail (plus the
    /// address it consumed) becomes permanently untracked -- no later
    /// reconcile pass detects it, since reconciliation only ever looks at
    /// what's already recorded in `placements`, never at a node's actual
    /// full jail set. The practical impact is bounded to one extra idle
    /// jail on one node (discoverable directly via that node's own
    /// `keel-agentd` `GET /jails`), consistent with this project's existing
    /// tolerance for eventual-consistency gaps elsewhere (see Milestone
    /// 9/10's "no hard admission guarantee" / "no overcommit protection
    /// beyond the ranking itself").
    ReconcileServices(Sender<Vec<ReplicaAction>>),
    DiscoverService(
        String,
        Sender<Result<Vec<wire::ServiceReplica>, services::UnknownService>>,
    ),
    ListServices(Sender<Vec<wire::ServiceSummary>>),
    ListServiceProxyEntries(Sender<Vec<wire::ServiceProxyEntry>>),
    DeleteService(
        String,
        Sender<Result<Vec<ReplicaAction>, services::UnknownService>>,
    ),
    RecordReplicaAddress(String, String, std::net::Ipv4Addr, Sender<()>),
    ReleaseReplicaAddress(String, Sender<()>),
    RecordStandby(String, String, Sender<()>),
    RemoveStandby(String, Sender<()>),
    RecordPendingFence(String, String, Sender<()>),
    PendingFencesForNode(String, Sender<Vec<String>>),
    RemovePendingFence(String, Sender<()>),
    PrepareForceRepin(String, Sender<Result<ForceRepinPrep, ForceRepinError>>),
    PrepareDrainRepin(String, Sender<Result<ForceRepinPrep, ForceRepinError>>),
    Cordon(String, Sender<Result<(), UnknownNode>>),
    Uncordon(String, Sender<Result<(), UnknownNode>>),
}

#[allow(clippy::too_many_arguments)]
pub fn spawn(
    mut registry: Registry,
    mut placements: Placements,
    mut services: Services,
    mut used_addresses: UsedAddresses,
    mut standbys: Standbys,
    mut pending_fences: PendingFences,
    mut cordoned: Cordoned,
    state_dir: PathBuf,
) -> (JoinHandle<()>, Sender<Command>) {
    let (tx, rx) = mpsc::channel::<Command>();
    let handle = thread::spawn(move || {
        for command in rx {
            run_catching_panics(std::panic::AssertUnwindSafe(|| {
                handle_command(
                    &mut registry,
                    &mut placements,
                    &mut services,
                    &mut used_addresses,
                    &mut standbys,
                    &mut pending_fences,
                    &mut cordoned,
                    &state_dir,
                    command,
                );
            }));
        }
    });
    (handle, tx)
}

/// Runs `f`, catching any panic so the worker's command loop can move on to
/// the next command instead of the whole thread dying forever on the first
/// bug anywhere in `handle_command` -- previously an unrecovered panic here
/// silently and permanently broke every future command, with the HTTP
/// server still up and looking alive. The panicking command's own reply
/// channel is simply dropped (its caller already has to handle a closed
/// channel as a defined failure mode); every command after it is still
/// served normally.
fn run_catching_panics(f: impl FnOnce() + std::panic::UnwindSafe) {
    if let Err(payload) = std::panic::catch_unwind(f) {
        let message = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        eprintln!("keel-controlplane: worker thread recovered from a panic while handling a command: {message}");
    }
}

fn persist_placements(placements: &Placements, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("placements.yaml"), placements) {
        eprintln!("keel-controlplane: failed to persist placements.yaml: {e}");
    }
}

fn persist_used_addresses(used_addresses: &UsedAddresses, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("used_addresses.yaml"), used_addresses) {
        eprintln!("keel-controlplane: failed to persist used_addresses.yaml: {e}");
    }
}

fn persist_standbys(standbys: &Standbys, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("standbys.yaml"), standbys) {
        eprintln!("keel-controlplane: failed to persist standbys.yaml: {e}");
    }
}

fn persist_pending_fences(pending_fences: &PendingFences, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("pending_fences.yaml"), pending_fences) {
        eprintln!("keel-controlplane: failed to persist pending_fences.yaml: {e}");
    }
}

fn persist_cordoned(cordoned: &Cordoned, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("cordoned.yaml"), cordoned) {
        eprintln!("keel-controlplane: failed to persist cordoned state: {e}");
    }
}

fn is_schedulable(status: &wire::NodeStatus, cordoned: &Cordoned) -> bool {
    status.status == NodeState::Alive && !cordoned.is_cordoned(&status.id)
}

#[allow(clippy::too_many_arguments)]
fn handle_command(
    registry: &mut Registry,
    placements: &mut Placements,
    services: &mut Services,
    used_addresses: &mut UsedAddresses,
    standbys: &mut Standbys,
    pending_fences: &mut PendingFences,
    cordoned: &mut Cordoned,
    state_dir: &Path,
    command: Command,
) {
    match command {
        Command::Register(id, addr, replicate_addr, capacity_cpu, capacity_memory, reply) => {
            let result = registry.register(
                id,
                addr,
                replicate_addr,
                capacity_cpu,
                capacity_memory,
                Instant::now(),
            );
            let _ = reply.send(result);
        }
        Command::Heartbeat(id, committed_cpu, committed_memory, jails, ingresses, reply) => {
            let result = registry.heartbeat(
                &id,
                committed_cpu,
                committed_memory,
                jails,
                ingresses,
                Instant::now(),
            );
            let _ = reply.send(result);
        }
        Command::List(reply) => {
            let _ = reply.send(registry.list(Instant::now()));
        }
        Command::Resolve(id, reply) => {
            let result = registry.resolve(&id, Instant::now());
            let _ = reply.send(result);
        }
        Command::ResolveOrSchedule(jail_name, reply) => {
            let now = Instant::now();
            let result = if let Some(node_id) = placements.get(&jail_name).map(|s| s.to_string()) {
                registry
                    .resolve(&node_id, now)
                    .map(|addr| (node_id, addr))
                    .map_err(ScheduleOrResolveError::from)
            } else {
                let nodes: Vec<scheduler::NodeResources> = registry
                    .list(now)
                    .into_iter()
                    .filter(|status| is_schedulable(status, cordoned))
                    .map(|status| scheduler::NodeResources {
                        id: status.id,
                        capacity_cpu: status.capacity_cpu,
                        capacity_memory: status.capacity_memory,
                        committed_cpu: status.committed_cpu,
                        committed_memory: status.committed_memory,
                    })
                    .collect();
                scheduler::pick_node(&nodes)
                    .map_err(ScheduleOrResolveError::from)
                    .and_then(|node_id| {
                        registry
                            .resolve(&node_id, now)
                            .map(|addr| (node_id, addr))
                            .map_err(ScheduleOrResolveError::from)
                    })
            };
            let _ = reply.send(result);
        }
        Command::ResolvePlacement(jail_name, reply) => {
            let result = match placements.get(&jail_name).map(|s| s.to_string()) {
                None => Err(PlacementError::NotPlaced(jail_name)),
                Some(node_id) => registry
                    .resolve(&node_id, Instant::now())
                    .map(|addr| (node_id, addr))
                    .map_err(PlacementError::from),
            };
            let _ = reply.send(result);
        }
        Command::RecordPlacement(jail_name, node_id, reply) => {
            placements.set(jail_name, node_id);
            persist_placements(placements, state_dir);
            let _ = reply.send(());
        }
        Command::RemovePlacement(jail_name, reply) => {
            placements.remove(&jail_name);
            persist_placements(placements, state_dir);
            let _ = reply.send(());
        }
        Command::OwnerOf(name, reply) => {
            let _ = reply.send(services::owner_of(&name, placements, services));
        }
        Command::ApplyService(name, replicas, template, port, reply) => {
            let result = (|| {
                for i in 0..replicas {
                    let candidate = services::replica_name(&name, i);
                    if let Some(owner) = services::owner_of(&candidate, placements, services) {
                        let is_self = matches!(&owner, Owner::Service(other) if other == &name);
                        if !is_self {
                            return Err(services::ApplyServiceError::NameConflict {
                                name: candidate,
                                owner,
                            });
                        }
                    }
                }
                services.apply(name, replicas, template, port)
            })();
            if result.is_ok() {
                services.persist(state_dir);
            }
            let _ = reply.send(result);
        }
        Command::ReconcileServices(reply) => {
            // See the doc comment on the `Command::ReconcileServices` variant
            // above for the known compute/execute race and why it's an
            // accepted limitation for this milestone rather than a bug.
            let now = Instant::now();
            let mut alive_nodes: Vec<scheduler::NodeResources> = registry
                .list(now)
                .into_iter()
                .filter(|s| is_schedulable(s, cordoned))
                .map(|s| scheduler::NodeResources {
                    id: s.id,
                    capacity_cpu: s.capacity_cpu,
                    capacity_memory: s.capacity_memory,
                    committed_cpu: s.committed_cpu,
                    committed_memory: s.committed_memory,
                })
                .collect();

            let mut actions = Vec::new();
            let mut working_used = used_addresses.clone();

            for (service_name, record) in services.list() {
                let placed: Vec<(u32, String, String)> = placements
                    .iter()
                    .filter_map(|(jail_name, node_id)| {
                        services::replica_index(service_name, jail_name)
                            .map(|idx| (idx, jail_name.to_string(), node_id.to_string()))
                    })
                    .collect();
                // Deliberately NOT also requiring `is_jail_running`: a
                // replica whose node is Alive still counts as present even
                // while crash-looping, since that node's own keel-agentd is
                // already retrying it locally via its own Milestone-4
                // crash-loop backoff. Rescheduling it elsewhere on top of
                // that would fight the local backoff and orphan the
                // original, untracked, on its old node. Only a node that's
                // actually unreachable (registry.resolve fails, whether
                // Dead or never-registered) makes local recovery impossible
                // and warrants scheduling a replacement. `GET /services`'s
                // own Alive+running check (unchanged, see DiscoverService)
                // still excludes a crash-looping replica from what's
                // actually advertised as usable.
                let present_indices: BTreeSet<u32> = if record.template.volumes.is_empty() {
                    placed
                        .iter()
                        .filter(|(_, _, node_id)| registry.resolve(node_id, now).is_ok())
                        .map(|(idx, _, _)| *idx)
                        .collect()
                } else {
                    // Stateful: a placement is "present" regardless of
                    // whether its node currently resolves. A replica pinned
                    // to a Dead node is neither torn down nor replaced
                    // elsewhere, it simply waits for that node to come
                    // back, since keel-agentd persists its own jail records
                    // to disk and will reconcile the replica back to
                    // running on its own once its process (or the node)
                    // returns, with no control-plane involvement. This is
                    // the entire node-pinning mechanism: everything
                    // downstream (diff_replicas, to_add/to_remove,
                    // ReplicaAction execution) is unchanged.
                    placed.iter().map(|(idx, _, _)| *idx).collect()
                };

                let (to_add, to_remove) =
                    services::diff_replicas(record.desired_replicas, &present_indices);
                let mut busy = services::nodes_hosting_service(service_name, placements);

                for idx in to_add {
                    let replica_name = services::replica_name(service_name, idx);
                    let Ok(node_id) = services::pick_node_for_service(alive_nodes.clone(), &busy)
                    else {
                        continue;
                    };
                    let Some(pod_cidr) = registry.pod_cidr(&node_id) else {
                        continue;
                    };
                    let Some(address) =
                        addresses::first_free_address(pod_cidr, &node_id, &working_used)
                    else {
                        continue;
                    };
                    let Ok(node_addr) = registry.resolve(&node_id, now) else {
                        continue;
                    };
                    working_used.record(replica_name.clone(), node_id.clone(), address);
                    busy.insert(node_id.clone());
                    // Reflect this replica's own cost immediately, so the
                    // next pick in this same pass (whether another replica
                    // of this service once every node is busy, or another
                    // service reconciled right after) sees this node's real
                    // remaining headroom instead of the stale, pre-pass
                    // value -- otherwise every remaining missing replica in
                    // the pass can keep piling onto whichever node looked
                    // best before the pass started, unaware of what it just
                    // committed here.
                    if let Some(n) = alive_nodes.iter_mut().find(|n| n.id == node_id) {
                        if let Ok(cpu_cost) =
                            keel_spec::parse_cpu_cores(&record.template.resources.cpu)
                        {
                            n.committed_cpu += cpu_cost;
                        }
                        if let Ok(memory_cost) =
                            keel_spec::parse_memory_bytes(&record.template.resources.memory)
                        {
                            n.committed_memory += memory_cost;
                        }
                    }

                    let (standby_node_id, standby_addr) = if record.template.volumes.is_empty() {
                        (None, None)
                    } else {
                        services::pick_node_for_service(alive_nodes.clone(), &busy)
                            .ok()
                            .filter(|standby_id| standby_id != &node_id)
                            .and_then(|standby_id| {
                                registry
                                    .replicate_addr(&standby_id)
                                    .map(|addr| (standby_id, addr))
                            })
                            .map(|(id, addr)| (Some(id), Some(addr)))
                            .unwrap_or((None, None))
                    };

                    // The generation this placement *will* become once
                    // `RecordPlacement` actually lands (see
                    // `Placements::set`) -- not yet true here, since
                    // execute_replica_actions only calls RecordPlacement
                    // after a successful forward(). If this attempt never
                    // lands, the next retry computes this same value again,
                    // since the counter only advances on a real `set`.
                    let generation = placements.generation(&replica_name) + 1;

                    actions.push(ReplicaAction::Schedule {
                        replica_name,
                        node_id,
                        node_addr,
                        template: record.template.clone(),
                        address,
                        prefix_len: pod_cidr.prefix_len(),
                        standby_node_id,
                        standby_addr,
                        generation,
                    });
                }

                for idx in to_remove {
                    let replica_name = services::replica_name(service_name, idx);
                    let Some(node_id) = placements.get(&replica_name).map(|s| s.to_string()) else {
                        continue;
                    };
                    let Ok(node_addr) = registry.resolve(&node_id, now) else {
                        continue;
                    };
                    actions.push(ReplicaAction::TearDown {
                        replica_name,
                        node_id,
                        node_addr,
                    });
                }
            }

            let _ = reply.send(actions);
        }
        Command::DiscoverService(name, reply) => {
            let result = if services.get(&name).is_none() {
                Err(services::UnknownService(name.clone()))
            } else {
                Ok(healthy_replicas(
                    &name,
                    placements,
                    registry,
                    used_addresses,
                    Instant::now(),
                ))
            };
            let _ = reply.send(result);
        }
        Command::ListServices(reply) => {
            let summaries: Vec<wire::ServiceSummary> = services
                .list()
                .into_iter()
                .map(|(name, record)| wire::ServiceSummary {
                    name: name.to_string(),
                    desired_replicas: record.desired_replicas,
                    vip: record.vip.to_string(),
                    port: record.port,
                })
                .collect();
            let _ = reply.send(summaries);
        }
        Command::ListServiceProxyEntries(reply) => {
            let now = Instant::now();
            let entries: Vec<wire::ServiceProxyEntry> = services
                .list()
                .into_iter()
                .map(|(name, record)| wire::ServiceProxyEntry {
                    name: name.to_string(),
                    vip: record.vip.to_string(),
                    port: record.port,
                    replicas: healthy_replicas(name, placements, registry, used_addresses, now),
                })
                .collect();
            let _ = reply.send(entries);
        }
        Command::DeleteService(name, reply) => {
            let result = if services.get(&name).is_none() {
                Err(services::UnknownService(name))
            } else {
                let now = Instant::now();
                let actions: Vec<ReplicaAction> = placements
                    .iter()
                    .filter_map(|(jail_name, node_id)| {
                        services::replica_index(&name, jail_name)?;
                        let node_addr = registry.resolve(node_id, now).ok()?;
                        Some(ReplicaAction::TearDown {
                            replica_name: jail_name.to_string(),
                            node_id: node_id.to_string(),
                            node_addr,
                        })
                    })
                    .collect();
                services.remove(&name);
                services.persist(state_dir);
                Ok(actions)
            };
            let _ = reply.send(result);
        }
        Command::RecordReplicaAddress(jail_name, node_id, address, reply) => {
            used_addresses.record(jail_name, node_id, address);
            persist_used_addresses(used_addresses, state_dir);
            let _ = reply.send(());
        }
        Command::ReleaseReplicaAddress(jail_name, reply) => {
            used_addresses.release(&jail_name);
            persist_used_addresses(used_addresses, state_dir);
            let _ = reply.send(());
        }
        Command::RecordStandby(replica_name, node_id, reply) => {
            standbys.set(replica_name, node_id);
            persist_standbys(standbys, state_dir);
            let _ = reply.send(());
        }
        Command::RemoveStandby(replica_name, reply) => {
            standbys.remove(&replica_name);
            persist_standbys(standbys, state_dir);
            let _ = reply.send(());
        }
        Command::RecordPendingFence(replica_name, node_id, reply) => {
            pending_fences.set(replica_name, node_id);
            persist_pending_fences(pending_fences, state_dir);
            let _ = reply.send(());
        }
        Command::PendingFencesForNode(node_id, reply) => {
            let _ = reply.send(pending_fences.for_node(&node_id));
        }
        Command::RemovePendingFence(replica_name, reply) => {
            pending_fences.remove(&replica_name);
            persist_pending_fences(pending_fences, state_dir);
            let _ = reply.send(());
        }
        Command::PrepareForceRepin(replica_name, reply) => {
            let result = prepare_repin(
                &replica_name,
                false,
                registry,
                placements,
                standbys,
                services,
                used_addresses,
                cordoned,
            );
            let _ = reply.send(result);
        }
        Command::PrepareDrainRepin(replica_name, reply) => {
            let result = prepare_repin(
                &replica_name,
                true,
                registry,
                placements,
                standbys,
                services,
                used_addresses,
                cordoned,
            );
            let _ = reply.send(result);
        }
        Command::Cordon(node_id, reply) => {
            let result = if registry.pod_cidr(&node_id).is_some() {
                cordoned.cordon(node_id);
                persist_cordoned(cordoned, state_dir);
                Ok(())
            } else {
                Err(UnknownNode(node_id))
            };
            let _ = reply.send(result);
        }
        Command::Uncordon(node_id, reply) => {
            let result = if registry.pod_cidr(&node_id).is_some() {
                cordoned.uncordon(&node_id);
                persist_cordoned(cordoned, state_dir);
                Ok(())
            } else {
                Err(UnknownNode(node_id))
            };
            let _ = reply.send(result);
        }
    }
}

/// Shared by `PrepareForceRepin` and `PrepareDrainRepin`: computes everything
/// needed to promote a stateful replica's standby and re-place its primary
/// elsewhere. The two differ only in whether a still-`Alive` primary is
/// tolerated -- `allow_alive_primary` is the split-brain guard from
/// `PrepareForceRepin`, kept for the ordinary (non-drain) path, and bypassed
/// for `drain` since drain fences the old primary synchronously as part of
/// the same operation (Task 6) rather than leaving it running independently.
#[allow(clippy::too_many_arguments)]
fn prepare_repin(
    replica_name: &str,
    allow_alive_primary: bool,
    registry: &Registry,
    placements: &Placements,
    standbys: &Standbys,
    services: &Services,
    used_addresses: &UsedAddresses,
    cordoned: &Cordoned,
) -> Result<ForceRepinPrep, ForceRepinError> {
    let now = Instant::now();
    let old_node_id = placements
        .get(replica_name)
        .map(|s| s.to_string())
        .ok_or_else(|| ForceRepinError::NotPlaced(replica_name.to_string()))?;
    // Checked before the primary-aliveness check below: a non-stateful
    // replica (no recorded standby at all) must report NotStateful
    // regardless of whether its sole node happens to be Alive or Dead,
    // rather than reporting PrimaryStillAlive first and never surfacing
    // NotStateful for an Alive-but-stateless replica.
    let standby_node_id = standbys
        .get(replica_name)
        .map(|s| s.to_string())
        .ok_or_else(|| ForceRepinError::NotStateful(replica_name.to_string()))?;
    if !allow_alive_primary && registry.resolve(&old_node_id, now).is_ok() {
        return Err(ForceRepinError::PrimaryStillAlive(old_node_id));
    }
    // No aliveness check here on purpose (see last_known_addr's own doc
    // comment): the whole point of the immediate fencing push is attempting
    // to reach a node the check just above called Dead, in case it's
    // actually alive and only failing to heartbeat to the control plane
    // specifically.
    let old_node_last_known_addr = registry.last_known_addr(&old_node_id);
    // Deliberately Registry::resolve(), not replicate_addr(): this is the
    // address the control plane forwards the readiness GET and the
    // provisioning PUT to (this node's normal HTTP API), not a replication
    // target embedded in a spec. Do not "fix" this to replicate_addr() by
    // symmetry with fresh_standby_addr below -- they serve different
    // purposes.
    let standby_addr = registry
        .resolve(&standby_node_id, now)
        .map_err(ForceRepinError::StandbyUnresolvable)?;

    let service_name = services::owner_of(replica_name, placements, services)
        .and_then(|owner| match owner {
            Owner::Service(name) => Some(name),
            Owner::Unmanaged => None,
        })
        .ok_or_else(|| ForceRepinError::NotStateful(replica_name.to_string()))?;
    let template = services
        .get(&service_name)
        .ok_or_else(|| ForceRepinError::NotStateful(replica_name.to_string()))?
        .template
        .clone();

    let alive_nodes: Vec<scheduler::NodeResources> = registry
        .list(now)
        .into_iter()
        .filter(|s| is_schedulable(s, cordoned))
        .map(|s| scheduler::NodeResources {
            id: s.id,
            capacity_cpu: s.capacity_cpu,
            capacity_memory: s.capacity_memory,
            committed_cpu: s.committed_cpu,
            committed_memory: s.committed_memory,
        })
        .collect();
    let mut exclude = std::collections::HashSet::new();
    exclude.insert(old_node_id.clone());
    exclude.insert(standby_node_id.clone());
    let fresh_standby_node_id = services::pick_node_for_service(alive_nodes, &exclude)
        .ok()
        .filter(|id| !exclude.contains(id))
        .ok_or(ForceRepinError::NoFreshStandby)?;
    // replicate_addr(), not resolve(): this value is embedded into the
    // promoted primary's spec.spec.replicate_to, telling its replication
    // loop where to connect -- it must be the fresh standby's
    // replication-listener address (Task 8b), not its main HTTP address.
    let fresh_standby_addr = registry
        .replicate_addr(&fresh_standby_node_id)
        .ok_or(ForceRepinError::NoFreshStandby)?;

    let pod_cidr = registry
        .pod_cidr(&standby_node_id)
        .ok_or(ForceRepinError::NoFreeAddress)?;
    let address = addresses::first_free_address(pod_cidr, &standby_node_id, used_addresses)
        .ok_or(ForceRepinError::NoFreeAddress)?;
    let next_generation = placements.generation(replica_name) + 1;

    Ok(ForceRepinPrep {
        old_node_id,
        old_node_last_known_addr,
        standby_node_id,
        standby_addr,
        template,
        fresh_standby_node_id,
        fresh_standby_addr,
        address,
        prefix_len: pod_cidr.prefix_len(),
        next_generation,
    })
}

/// The exact health filter `GET /services/<name>` (`Command::DiscoverService`)
/// and the heartbeat response body (`Command::ListServiceProxyEntries`)
/// both need: a replica whose node is `Alive` *and* whose last-reported
/// heartbeat marked it `running`. Shared as one function so the two can
/// never drift apart.
fn healthy_replicas(
    name: &str,
    placements: &Placements,
    registry: &Registry,
    used_addresses: &UsedAddresses,
    now: Instant,
) -> Vec<wire::ServiceReplica> {
    let mut replicas: Vec<wire::ServiceReplica> = placements
        .iter()
        .filter_map(|(jail_name, node_id)| {
            services::replica_index(name, jail_name)?;
            if registry.resolve(node_id, now).is_ok()
                && registry.is_jail_running(node_id, jail_name)
            {
                let address = used_addresses.address_of(jail_name)?;
                Some(wire::ServiceReplica {
                    name: jail_name.to_string(),
                    node: node_id.to_string(),
                    address: address.to_string(),
                })
            } else {
                None
            }
        })
        .collect();
    replicas.sort_by(|a, b| a.name.cmp(&b.name));
    replicas
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addresses::UsedAddresses;
    use crate::services::{ApplyServiceError, Owner, Services};
    use keel_spec::{JailTemplate, ResourcesSpec, RestartPolicy, TemplateNetworkSpec, VolumeMount};

    #[test]
    fn run_catching_panics_contains_a_panic_instead_of_propagating_it() {
        // Proves the actual mechanism `spawn`'s command loop relies on: a
        // panic inside the wrapped closure is caught here, not unwound
        // further up the stack. If this were not true, a single bad
        // command would still kill the whole worker thread forever, which
        // is exactly the bug this exists to fix.
        //
        // Rust's default panic hook still prints "thread ... panicked at
        // ..." to stderr for the deliberate panic below even though it's
        // caught (the hook runs before unwinding starts, catch_unwind
        // can't suppress it) -- expected, harmless noise from this one
        // test, not swapped out globally since `cargo test` runs this
        // binary's tests concurrently and a global hook change here could
        // swallow an unrelated test's genuine panic message.
        run_catching_panics(std::panic::AssertUnwindSafe(|| {
            panic!("deliberate panic for this test");
        }));
        // Reaching this line at all is the assertion: the panic above did
        // not propagate out of `run_catching_panics`.
    }

    #[test]
    fn run_catching_panics_still_runs_a_non_panicking_closure_normally() {
        let mut ran = false;
        run_catching_panics(std::panic::AssertUnwindSafe(|| {
            ran = true;
        }));
        assert!(
            ran,
            "a closure that doesn't panic must still run to completion"
        );
    }

    fn test_cluster_cidr() -> ipnet::Ipv4Net {
        "10.0.0.0/16".parse().unwrap()
    }

    fn test_service_cidr() -> ipnet::Ipv4Net {
        "10.0.250.0/24".parse().unwrap()
    }

    fn fresh_state_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "keel-controlplane-worker-test-{}-{id}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn test_node_status(id: &str, status: NodeState) -> wire::NodeStatus {
        wire::NodeStatus {
            id: id.to_string(),
            addr: "192.168.64.4".to_string(),
            pod_cidr: "10.0.4.0/24".to_string(),
            status,
            last_seen_secs: 0,
            capacity_cpu: 0.0,
            capacity_memory: 0,
            committed_cpu: 0.0,
            committed_memory: 0,
            ingresses: vec![],
        }
    }

    #[test]
    fn is_schedulable_excludes_dead_and_cordoned_nodes() {
        let cordoned = {
            let mut c = Cordoned::new();
            c.cordon("node-cordoned".to_string());
            c
        };
        let alive_schedulable = test_node_status("node-1", NodeState::Alive);
        let alive_cordoned = test_node_status("node-cordoned", NodeState::Alive);
        let dead_uncordoned = test_node_status("node-2", NodeState::Dead);

        assert!(is_schedulable(&alive_schedulable, &cordoned));
        assert!(!is_schedulable(&alive_cordoned, &cordoned));
        assert!(!is_schedulable(&dead_uncordoned, &cordoned));
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
    fn register_command_makes_the_node_visible_in_list() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (reg_tx, reg_rx) = mpsc::channel();
        commands
            .send(Command::Register(
                "node-1".to_string(),
                "10.0.0.1".to_string(),
                None,
                4.0,
                8 * 1024 * 1024 * 1024,
                reg_tx,
            ))
            .unwrap();
        reg_rx.recv().unwrap().unwrap();

        let (list_tx, list_rx) = mpsc::channel();
        commands.send(Command::List(list_tx)).unwrap();
        let statuses = list_rx.recv().unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].id, "node-1");
    }

    #[test]
    fn heartbeat_command_on_unknown_id_returns_an_error() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (hb_tx, hb_rx) = mpsc::channel();
        commands
            .send(Command::Heartbeat(
                "missing".to_string(),
                0.0,
                0,
                vec![],
                vec![],
                hb_tx,
            ))
            .unwrap();
        assert!(hb_rx.recv().unwrap().is_err());
    }

    #[test]
    fn heartbeat_command_on_a_registered_node_succeeds() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (reg_tx, reg_rx) = mpsc::channel();
        commands
            .send(Command::Register(
                "node-1".to_string(),
                "10.0.0.1".to_string(),
                None,
                4.0,
                8 * 1024 * 1024 * 1024,
                reg_tx,
            ))
            .unwrap();
        reg_rx.recv().unwrap().unwrap();

        let (hb_tx, hb_rx) = mpsc::channel();
        commands
            .send(Command::Heartbeat(
                "node-1".to_string(),
                0.0,
                0,
                vec![],
                vec![],
                hb_tx,
            ))
            .unwrap();
        assert!(hb_rx.recv().unwrap().is_ok());
    }

    #[test]
    fn apply_service_command_carries_the_port_through() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                1,
                template(),
                8080,
                tx,
            ))
            .unwrap();
        rx.recv().unwrap().unwrap();

        let (list_tx, list_rx) = mpsc::channel();
        commands.send(Command::ListServices(list_tx)).unwrap();
        let summaries = list_rx.recv().unwrap();
        assert_eq!(summaries[0].port, 8080);
        assert_ne!(summaries[0].vip, "0.0.0.0", "expected a real derived VIP");
    }

    #[test]
    fn list_service_proxy_entries_reflects_only_alive_and_running_replicas() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);

        let (apply_tx, apply_rx) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                1,
                template(),
                8080,
                apply_tx,
            ))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();

        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();

        // Not yet marked running via a heartbeat -> not yet "healthy".
        let (entries_tx, entries_rx) = mpsc::channel();
        commands
            .send(Command::ListServiceProxyEntries(entries_tx))
            .unwrap();
        let entries = entries_rx.recv().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "web");
        assert_eq!(entries[0].port, 8080);
        assert!(
            entries[0].replicas.is_empty(),
            "web-0 has no recorded address/running-jail signal yet"
        );
    }

    #[test]
    fn list_service_proxy_entries_is_empty_with_no_services() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        let (tx, rx) = mpsc::channel();
        commands.send(Command::ListServiceProxyEntries(tx)).unwrap();
        assert_eq!(rx.recv().unwrap(), vec![]);
    }

    #[test]
    fn list_command_on_a_fresh_worker_is_empty() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (list_tx, list_rx) = mpsc::channel();
        commands.send(Command::List(list_tx)).unwrap();
        assert_eq!(list_rx.recv().unwrap(), vec![]);
    }

    #[test]
    fn resolve_command_on_a_registered_alive_node_returns_its_address() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (reg_tx, reg_rx) = mpsc::channel();
        commands
            .send(Command::Register(
                "node-1".to_string(),
                "10.0.0.1".to_string(),
                None,
                4.0,
                8 * 1024 * 1024 * 1024,
                reg_tx,
            ))
            .unwrap();
        reg_rx.recv().unwrap().unwrap();

        let (resolve_tx, resolve_rx) = mpsc::channel();
        commands
            .send(Command::Resolve("node-1".to_string(), resolve_tx))
            .unwrap();
        assert_eq!(resolve_rx.recv().unwrap(), Ok("10.0.0.1".to_string()));
    }

    #[test]
    fn resolve_command_on_an_unknown_node_returns_an_error() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (resolve_tx, resolve_rx) = mpsc::channel();
        commands
            .send(Command::Resolve("missing".to_string(), resolve_tx))
            .unwrap();
        assert!(resolve_rx.recv().unwrap().is_err());
    }

    fn register_node(
        commands: &Sender<Command>,
        id: &str,
        addr: &str,
        capacity_cpu: f64,
        capacity_memory: u64,
    ) {
        let (reg_tx, reg_rx) = mpsc::channel();
        commands
            .send(Command::Register(
                id.to_string(),
                addr.to_string(),
                None,
                capacity_cpu,
                capacity_memory,
                reg_tx,
            ))
            .unwrap();
        reg_rx.recv().unwrap().unwrap();
    }

    fn register_node_with_replicate_addr(
        commands: &Sender<Command>,
        id: &str,
        addr: &str,
        replicate_addr: &str,
        capacity_cpu: f64,
        capacity_memory: u64,
    ) {
        let (reg_tx, reg_rx) = mpsc::channel();
        commands
            .send(Command::Register(
                id.to_string(),
                addr.to_string(),
                Some(replicate_addr.to_string()),
                capacity_cpu,
                capacity_memory,
                reg_tx,
            ))
            .unwrap();
        reg_rx.recv().unwrap().unwrap();
    }

    fn heartbeat_node(
        commands: &Sender<Command>,
        id: &str,
        committed_cpu: f64,
        committed_memory: u64,
    ) {
        let (hb_tx, hb_rx) = mpsc::channel();
        commands
            .send(Command::Heartbeat(
                id.to_string(),
                committed_cpu,
                committed_memory,
                vec![],
                vec![],
                hb_tx,
            ))
            .unwrap();
        hb_rx.recv().unwrap().unwrap();
    }

    #[test]
    fn resolve_or_schedule_on_a_fresh_jail_name_with_equal_headroom_breaks_ties_by_ascending_id() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ResolveOrSchedule("web-1".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Ok(("node-1".to_string(), "10.0.0.1".to_string()))
        );
    }

    #[test]
    fn resolve_or_schedule_on_a_fresh_jail_name_schedules_onto_the_node_with_more_headroom() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 100);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 100);
        heartbeat_node(&commands, "node-1", 3.0, 10); // 25% cpu headroom
        heartbeat_node(&commands, "node-2", 1.0, 10); // 75% cpu headroom

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ResolveOrSchedule("web-1".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Ok(("node-2".to_string(), "10.0.0.2".to_string()))
        );
    }

    #[test]
    fn resolve_or_schedule_on_an_already_placed_jail_is_sticky() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);

        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-1".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ResolveOrSchedule("web-1".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Ok(("node-1".to_string(), "10.0.0.1".to_string()))
        );
    }

    #[test]
    fn resolve_or_schedule_with_no_alive_nodes_returns_no_available_nodes() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ResolveOrSchedule("web-1".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Err(ScheduleOrResolveError::Schedule(
                ScheduleError::NoAvailableNodes
            ))
        );
    }

    #[test]
    fn resolve_placement_on_an_unplaced_jail_returns_not_placed() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ResolvePlacement("web-1".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Err(PlacementError::NotPlaced("web-1".to_string()))
        );
    }

    #[test]
    fn record_then_remove_placement_is_reflected_by_resolve_placement() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);

        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-1".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ResolvePlacement("web-1".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Ok(("node-1".to_string(), "10.0.0.1".to_string()))
        );

        let (rem_tx, rem_rx) = mpsc::channel();
        commands
            .send(Command::RemovePlacement("web-1".to_string(), rem_tx))
            .unwrap();
        rem_rx.recv().unwrap();

        let (tx2, rx2) = mpsc::channel();
        commands
            .send(Command::ResolvePlacement("web-1".to_string(), tx2))
            .unwrap();
        assert_eq!(
            rx2.recv().unwrap(),
            Err(PlacementError::NotPlaced("web-1".to_string()))
        );
    }

    #[test]
    fn apply_service_command_creates_a_new_service() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                3,
                template(),
                8080,
                tx,
            ))
            .unwrap();
        assert_eq!(rx.recv().unwrap(), Ok(()));
    }

    #[test]
    fn apply_service_command_rejects_a_template_change_on_an_existing_service() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (tx1, rx1) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                3,
                template(),
                8080,
                tx1,
            ))
            .unwrap();
        rx1.recv().unwrap().unwrap();

        let mut changed = template();
        changed.image = "base/different-image".to_string();
        let (tx2, rx2) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                3,
                changed,
                8080,
                tx2,
            ))
            .unwrap();
        assert_eq!(
            rx2.recv().unwrap(),
            Err(ApplyServiceError::TemplateChanged("web".to_string()))
        );
    }

    #[test]
    fn apply_service_command_rejects_a_name_already_used_by_an_unmanaged_jail() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                1,
                template(),
                8080,
                tx,
            ))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Err(ApplyServiceError::NameConflict {
                name: "web-0".to_string(),
                owner: Owner::Unmanaged
            })
        );
    }

    #[test]
    fn apply_service_command_reapplying_the_same_service_with_more_replicas_does_not_conflict_with_itself(
    ) {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (tx1, rx1) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                1,
                template(),
                8080,
                tx1,
            ))
            .unwrap();
        rx1.recv().unwrap().unwrap();

        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();

        let (tx2, rx2) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                3,
                template(),
                8080,
                tx2,
            ))
            .unwrap();
        assert_eq!(rx2.recv().unwrap(), Ok(()));
    }

    #[test]
    fn owner_of_command_on_an_unplaced_name_is_none() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::OwnerOf("web-0".to_string(), tx))
            .unwrap();
        assert_eq!(rx.recv().unwrap(), None);
    }

    #[test]
    fn owner_of_command_on_a_service_replica_names_that_service() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;

        let (apply_tx, apply_rx) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                1,
                template(),
                8080,
                apply_tx,
            ))
            .unwrap();
        apply_rx.recv().unwrap().unwrap();
        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::OwnerOf("web-0".to_string(), tx))
            .unwrap();
        assert_eq!(rx.recv().unwrap(), Some(Owner::Service("web".to_string())));
    }

    fn apply_service(commands: &Sender<Command>, name: &str, replicas: u32) {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                name.to_string(),
                replicas,
                template(),
                8080,
                tx,
            ))
            .unwrap();
        rx.recv().unwrap().unwrap();
    }

    fn stateful_template() -> JailTemplate {
        let mut t = template();
        t.volumes = vec![VolumeMount {
            name: "data".to_string(),
            mount_path: "/data".to_string(),
            size: "1G".to_string(),
        }];
        t
    }

    fn apply_service_with_template(
        commands: &Sender<Command>,
        name: &str,
        replicas: u32,
        template: JailTemplate,
    ) {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                name.to_string(),
                replicas,
                template,
                8080,
                tx,
            ))
            .unwrap();
        rx.recv().unwrap().unwrap();
    }

    fn record_placement(commands: &Sender<Command>, jail_name: &str, node_id: &str) {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                jail_name.to_string(),
                node_id.to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();
    }

    fn reconcile(commands: &Sender<Command>) -> Vec<ReplicaAction> {
        let (tx, rx) = mpsc::channel();
        commands.send(Command::ReconcileServices(tx)).unwrap();
        rx.recv().unwrap()
    }

    fn heartbeat_with_jails(
        commands: &Sender<Command>,
        id: &str,
        jails: Vec<crate::wire::JailHealth>,
    ) {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::Heartbeat(
                id.to_string(),
                0.0,
                0,
                jails,
                vec![],
                tx,
            ))
            .unwrap();
        rx.recv().unwrap().unwrap();
    }

    fn running(name: &str) -> crate::wire::JailHealth {
        crate::wire::JailHealth {
            name: name.to_string(),
            running: true,
        }
    }

    #[test]
    fn reconcile_services_after_a_simulated_restart_does_not_duplicate_an_already_placed_replica() {
        let state_dir = fresh_state_dir();

        // "Before the restart": apply a service, let it schedule, and record
        // its placement and address exactly as http.rs's real
        // execute_replica_actions would after a successful forward().
        {
            let commands = spawn(
                Registry::new(test_cluster_cidr()),
                Placements::new(),
                Services::new(test_service_cidr()),
                UsedAddresses::new(),
                Standbys::new(),
                PendingFences::new(),
                Cordoned::new(),
                state_dir.clone(),
            )
            .1;
            register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
            apply_service(&commands, "web", 1);
            record_placement(&commands, "web-0", "node-1");
            let (addr_tx, addr_rx) = mpsc::channel();
            commands
                .send(Command::RecordReplicaAddress(
                    "web-0".to_string(),
                    "node-1".to_string(),
                    "10.0.60.2".parse().unwrap(),
                    addr_tx,
                ))
                .unwrap();
            addr_rx.recv().unwrap();
            heartbeat_with_jails(&commands, "node-1", vec![running("web-0")]);
        }

        // "The restart": a fresh spawn() loading from the same state_dir,
        // with no explicit re-apply/re-record of anything.
        let placements: Placements =
            crate::store::load_or_default(&state_dir.join("placements.yaml"));
        let used_addresses: UsedAddresses =
            crate::store::load_or_default(&state_dir.join("used_addresses.yaml"));
        let services = Services::load(&state_dir, test_service_cidr());
        let restarted_commands = spawn(
            Registry::new(test_cluster_cidr()),
            placements,
            services,
            used_addresses,
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            state_dir,
        )
        .1;
        register_node(
            &restarted_commands,
            "node-1",
            "10.0.0.1",
            4.0,
            8 * 1024 * 1024 * 1024,
        );
        heartbeat_with_jails(&restarted_commands, "node-1", vec![running("web-0")]);

        assert_eq!(
            reconcile(&restarted_commands),
            vec![],
            "the already-placed replica must not be seen as missing and rescheduled after a restart"
        );
    }

    #[test]
    fn every_remaining_mutating_command_persists_and_survives_a_simulated_restart() {
        let state_dir = fresh_state_dir();
        {
            let commands = spawn(
                Registry::new(test_cluster_cidr()),
                Placements::new(),
                Services::new(test_service_cidr()),
                UsedAddresses::new(),
                Standbys::new(),
                PendingFences::new(),
                Cordoned::new(),
                state_dir.clone(),
            )
            .1;
            register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);

            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordStandby(
                    "db-0".to_string(),
                    "node-2".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();

            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordPendingFence(
                    "db-1".to_string(),
                    "node-3".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();

            // A second, separately-recorded fence, so its *absence* after
            // restart is provably due to RemovePendingFence, not just never
            // having been recorded in the first place.
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordPendingFence(
                    "db-2".to_string(),
                    "node-4".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RemovePendingFence("db-2".to_string(), tx))
                .unwrap();
            rx.recv().unwrap();

            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordReplicaAddress(
                    "db-3".to_string(),
                    "node-1".to_string(),
                    "10.0.60.5".parse().unwrap(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordReplicaAddress(
                    "db-4".to_string(),
                    "node-1".to_string(),
                    "10.0.60.6".parse().unwrap(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::ReleaseReplicaAddress("db-4".to_string(), tx))
                .unwrap();
            rx.recv().unwrap();

            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordPlacement(
                    "db-5".to_string(),
                    "node-1".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordPlacement(
                    "db-6".to_string(),
                    "node-1".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RemovePlacement("db-6".to_string(), tx))
                .unwrap();
            rx.recv().unwrap();

            apply_service(&commands, "web", 1);
            apply_service(&commands, "api", 1);
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::DeleteService("api".to_string(), tx))
                .unwrap();
            rx.recv().unwrap().unwrap();
        }

        let standbys: Standbys = crate::store::load_or_default(&state_dir.join("standbys.yaml"));
        assert_eq!(
            standbys.get("db-0"),
            Some("node-2"),
            "RecordStandby must survive a restart"
        );

        let pending_fences: PendingFences =
            crate::store::load_or_default(&state_dir.join("pending_fences.yaml"));
        assert_eq!(
            pending_fences.for_node("node-3"),
            vec!["db-1".to_string()],
            "RecordPendingFence must survive a restart"
        );
        assert_eq!(
            pending_fences.for_node("node-4"),
            Vec::<String>::new(),
            "RemovePendingFence must survive a restart"
        );

        let used_addresses: UsedAddresses =
            crate::store::load_or_default(&state_dir.join("used_addresses.yaml"));
        assert_eq!(
            used_addresses.address_of("db-3"),
            Some("10.0.60.5".parse().unwrap()),
            "RecordReplicaAddress must survive a restart"
        );
        assert_eq!(
            used_addresses.address_of("db-4"),
            None,
            "ReleaseReplicaAddress must survive a restart"
        );

        let placements: Placements =
            crate::store::load_or_default(&state_dir.join("placements.yaml"));
        assert_eq!(
            placements.get("db-5"),
            Some("node-1"),
            "RecordPlacement must survive a restart"
        );
        assert_eq!(
            placements.get("db-6"),
            None,
            "RemovePlacement must survive a restart"
        );

        let services = Services::load(&state_dir, test_service_cidr());
        assert!(
            services.get("web").is_some(),
            "ApplyService must survive a restart"
        );
        assert!(
            services.get("api").is_none(),
            "DeleteService must survive a restart"
        );
    }

    #[test]
    fn reconcile_services_tracks_headroom_spent_earlier_in_the_same_pass() {
        // Both nodes start with identical, empty capacity. `svc-a` is
        // scheduled first in this pass (services are reconciled in name
        // order) and consumes most of node-1's capacity. Without per-pass
        // headroom tracking, `svc-b`'s pick a moment later still sees
        // node-1's *original* (stale) headroom score and ties with the
        // untouched node-2, with the alphabetical tie-break piling svc-b
        // onto the already-heavily-loaded node-1 -- even though node-2 is
        // still completely idle. With tracking, node-1's just-spent
        // headroom is visible immediately, and svc-b correctly lands on
        // node-2 instead.
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);

        let mut heavy = template();
        heavy.resources.cpu = "3".to_string();
        apply_service_with_template(&commands, "svc-a", 1, heavy);

        let mut light = template();
        light.resources.cpu = "1".to_string();
        apply_service_with_template(&commands, "svc-b", 1, light);

        let actions = reconcile(&commands);
        assert_eq!(actions.len(), 2);
        let node_for = |replica_name: &str| {
            actions
                .iter()
                .find_map(|a| match a {
                    ReplicaAction::Schedule {
                        replica_name: r,
                        node_id,
                        ..
                    } if r == replica_name => Some(node_id.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| {
                    panic!("no Schedule action found for {replica_name}, got: {actions:?}")
                })
        };
        let svc_a_node = node_for("svc-a-0");
        let svc_b_node = node_for("svc-b-0");
        assert_ne!(
            svc_a_node, svc_b_node,
            "svc-b must not pile onto the same node svc-a just loaded in this pass while an idle node sits available, got: {actions:?}"
        );
    }

    #[test]
    fn reconcile_services_schedules_every_replica_of_a_brand_new_service_across_distinct_nodes() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 2);

        let actions = reconcile(&commands);
        assert_eq!(actions.len(), 2);
        let node_ids: std::collections::HashSet<String> = actions
            .iter()
            .map(|a| match a {
                ReplicaAction::Schedule { node_id, .. } => node_id.clone(),
                ReplicaAction::TearDown { .. } => panic!("expected only Schedule actions"),
            })
            .collect();
        assert_eq!(
            node_ids.len(),
            2,
            "expected the two replicas spread across two distinct nodes, got: {actions:?}"
        );
    }

    #[test]
    fn reconcile_services_is_idempotent_once_replicas_are_recorded_placed_and_reported_healthy() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 1);
        reconcile(&commands); // computed, but not yet "recorded" as actually placed

        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();
        heartbeat_with_jails(&commands, "node-1", vec![running("web-0")]);

        assert_eq!(
            reconcile(&commands),
            vec![],
            "a fully healthy, fully-placed service needs no further actions"
        );
    }

    #[test]
    fn reconcile_services_leaves_a_crash_looping_replica_on_a_still_alive_node_alone() {
        // A replica whose node is Alive is never rescheduled elsewhere just
        // because it's crash-looping -- that node's own keel-agentd is
        // already retrying it locally via its Milestone-4 crash-loop
        // backoff. Rescheduling on top of that would fight the local
        // backoff and orphan the original, untracked, on its old node.
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 1);
        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();
        heartbeat_with_jails(
            &commands,
            "node-1",
            vec![crate::wire::JailHealth {
                name: "web-0".to_string(),
                running: false,
            }],
        );

        assert_eq!(
            reconcile(&commands),
            vec![],
            "a crash-looping replica on a still-Alive node must be left to local backoff, not rescheduled"
        );
    }

    #[test]
    fn reconcile_services_reschedules_a_replica_whose_node_is_unreachable() {
        // web-0 is "placed" on a node that was never registered at all --
        // registry.resolve() fails for it exactly the way it would for a
        // genuinely Dead node, so this exercises the same "node itself is
        // unreachable, local backoff can't help" path without needing to
        // wait out the real Dead-node heartbeat timeout in a test.
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 1);
        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-unreachable".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();

        let actions = reconcile(&commands);
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], ReplicaAction::Schedule { replica_name, node_id, .. } if replica_name == "web-0" && node_id == "node-1"),
            "expected web-0 rescheduled onto the one real Alive node, got: {actions:?}"
        );
    }

    #[test]
    fn reconcile_services_leaves_a_stateful_replica_pinned_to_a_dead_node_alone() {
        // Same "node never registered" trick as the stateless-unreachable
        // test above: registry.resolve() fails for it exactly like a
        // genuinely Dead node, without waiting out the real heartbeat
        // timeout.
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service_with_template(&commands, "db", 1, stateful_template());
        record_placement(&commands, "db-0", "node-unreachable");

        assert_eq!(
            reconcile(&commands),
            vec![],
            "a stateful replica pinned to an unreachable node must be neither torn down nor rescheduled"
        );
    }

    #[test]
    fn reconcile_services_stateful_scale_down_skips_a_dead_pinned_replica_until_its_node_is_alive_again(
    ) {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service_with_template(&commands, "db", 2, stateful_template());
        record_placement(&commands, "db-0", "node-1");
        record_placement(&commands, "db-1", "node-unreachable");
        heartbeat_with_jails(&commands, "node-1", vec![running("db-0")]);

        apply_service_with_template(&commands, "db", 1, stateful_template()); // scale down to 1, removing index 1
        assert_eq!(
            reconcile(&commands),
            vec![],
            "scale-down of a stateful replica pinned to an unreachable node must be skipped this tick, not errored"
        );

        register_node(
            &commands,
            "node-unreachable",
            "10.0.0.9",
            4.0,
            8 * 1024 * 1024 * 1024,
        );
        let actions = reconcile(&commands);
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], ReplicaAction::TearDown { replica_name, node_id, .. } if replica_name == "db-1" && node_id == "node-unreachable"),
            "expected db-1 torn down once its pinned node is reachable again, got: {actions:?}"
        );
    }

    #[test]
    fn reconcile_services_schedules_a_brand_new_stateful_service_across_distinct_nodes() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service_with_template(&commands, "db", 2, stateful_template());

        let actions = reconcile(&commands);
        assert_eq!(actions.len(), 2);
        let node_ids: std::collections::HashSet<String> = actions
            .iter()
            .map(|a| match a {
                ReplicaAction::Schedule { node_id, .. } => node_id.clone(),
                ReplicaAction::TearDown { .. } => panic!("expected only Schedule actions"),
            })
            .collect();
        assert_eq!(
            node_ids.len(),
            2,
            "expected the two replicas spread across two distinct nodes, got: {actions:?}"
        );
    }

    #[test]
    fn reconcile_services_tears_down_from_the_highest_index_on_scale_down() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 3);
        for i in 0..3 {
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordPlacement(
                    format!("web-{i}"),
                    "node-1".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
        }
        heartbeat_with_jails(
            &commands,
            "node-1",
            vec![running("web-0"), running("web-1"), running("web-2")],
        );

        apply_service(&commands, "web", 1); // scale down to 1
        let actions = reconcile(&commands);
        assert_eq!(actions.len(), 2);
        assert!(
            matches!(&actions[0], ReplicaAction::TearDown { replica_name, .. } if replica_name == "web-2")
        );
        assert!(
            matches!(&actions[1], ReplicaAction::TearDown { replica_name, .. } if replica_name == "web-1")
        );
    }

    #[test]
    fn reconcile_services_never_double_assigns_an_address_within_one_pass() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 2); // only one alive node: both replicas land on it

        let actions = reconcile(&commands);
        let addresses: std::collections::HashSet<std::net::Ipv4Addr> = actions
            .iter()
            .map(|a| match a {
                ReplicaAction::Schedule { address, .. } => *address,
                ReplicaAction::TearDown { .. } => panic!("expected only Schedule actions"),
            })
            .collect();
        assert_eq!(
            addresses.len(),
            2,
            "expected two distinct addresses, got: {actions:?}"
        );
    }

    #[test]
    fn discover_service_on_an_unknown_service_returns_unknown_service() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::DiscoverService("missing".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Err(services::UnknownService("missing".to_string()))
        );
    }

    #[test]
    fn discover_service_omits_a_replica_that_is_not_reported_running() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 2);
        for i in 0..2 {
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordPlacement(
                    format!("web-{i}"),
                    "node-1".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
            let (atx, arx) = mpsc::channel();
            commands
                .send(Command::RecordReplicaAddress(
                    format!("web-{i}"),
                    "node-1".to_string(),
                    format!("10.0.131.{}", 2 + i).parse().unwrap(),
                    atx,
                ))
                .unwrap();
            arx.recv().unwrap();
        }
        // web-0 running, web-1 crash-looping.
        heartbeat_with_jails(
            &commands,
            "node-1",
            vec![
                running("web-0"),
                crate::wire::JailHealth {
                    name: "web-1".to_string(),
                    running: false,
                },
            ],
        );

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::DiscoverService("web".to_string(), tx))
            .unwrap();
        let replicas = rx.recv().unwrap().unwrap();
        assert_eq!(
            replicas,
            vec![crate::wire::ServiceReplica {
                name: "web-0".to_string(),
                node: "node-1".to_string(),
                address: "10.0.131.2".to_string()
            }]
        );
    }

    #[test]
    fn list_services_returns_every_service_sorted_by_name() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        apply_service(&commands, "web", 3);
        apply_service(&commands, "api", 1);

        let (tx, rx) = mpsc::channel();
        commands.send(Command::ListServices(tx)).unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            vec![
                crate::wire::ServiceSummary {
                    name: "api".to_string(),
                    desired_replicas: 1,
                    vip: crate::subnet::derive_service_vip("api", &test_service_cidr(), 0)
                        .to_string(),
                    port: 8080,
                },
                crate::wire::ServiceSummary {
                    name: "web".to_string(),
                    desired_replicas: 3,
                    vip: crate::subnet::derive_service_vip("web", &test_service_cidr(), 0)
                        .to_string(),
                    port: 8080,
                },
            ]
        );
    }

    #[test]
    fn delete_service_on_an_unknown_name_returns_unknown_service() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::DeleteService("missing".to_string(), tx))
            .unwrap();
        assert_eq!(
            rx.recv().unwrap(),
            Err(services::UnknownService("missing".to_string()))
        );
    }

    #[test]
    fn delete_service_returns_a_teardown_action_per_current_placement_and_forgets_the_service() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 2);
        for i in 0..2 {
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordPlacement(
                    format!("web-{i}"),
                    "node-1".to_string(),
                    tx,
                ))
                .unwrap();
            rx.recv().unwrap();
        }

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::DeleteService("web".to_string(), tx))
            .unwrap();
        let actions = rx.recv().unwrap().unwrap();
        assert_eq!(actions.len(), 2);

        // DeleteService only forgets the service definition and reports what
        // needs tearing down; it never touches Placements itself. In the real
        // system, Task 8's execute_replica_actions removes each placement
        // only after successfully forwarding that replica's teardown to its
        // node -- simulate that pairing here before checking the name is
        // free again, since nothing at this layer does it automatically.
        for i in 0..2 {
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RemovePlacement(format!("web-{i}"), tx))
                .unwrap();
            rx.recv().unwrap();
        }

        // The service definition itself is gone: a later apply of the same
        // name with a different template is a fresh create, not a rejected
        // template change.
        let mut different = template();
        different.image = "base/different-image".to_string();
        let (tx2, rx2) = mpsc::channel();
        commands
            .send(Command::ApplyService(
                "web".to_string(),
                1,
                different,
                8080,
                tx2,
            ))
            .unwrap();
        assert_eq!(rx2.recv().unwrap(), Ok(()));
    }

    #[test]
    fn record_then_release_replica_address_round_trips() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordReplicaAddress(
                "web-0".to_string(),
                "node-1".to_string(),
                "10.0.60.2".parse().unwrap(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 1);
        let (rec_tx, rec_rx) = mpsc::channel();
        commands
            .send(Command::RecordPlacement(
                "web-0".to_string(),
                "node-1".to_string(),
                rec_tx,
            ))
            .unwrap();
        rec_rx.recv().unwrap();
        heartbeat_with_jails(&commands, "node-1", vec![running("web-0")]);

        let (dtx, drx) = mpsc::channel();
        commands
            .send(Command::DiscoverService("web".to_string(), dtx))
            .unwrap();
        assert_eq!(drx.recv().unwrap().unwrap()[0].address, "10.0.60.2");

        // A real teardown always pairs ReleaseReplicaAddress with
        // RemovePlacement -- both fire together from Task 8's
        // execute_replica_actions right after a successful DELETE forward.
        // Simulate that pairing here rather than releasing in isolation,
        // which can't actually happen against a healthy, still-placed
        // replica in the deployed system.
        let (rtx, rrx) = mpsc::channel();
        commands
            .send(Command::ReleaseReplicaAddress("web-0".to_string(), rtx))
            .unwrap();
        rrx.recv().unwrap();
        let (rp_tx, rp_rx) = mpsc::channel();
        commands
            .send(Command::RemovePlacement("web-0".to_string(), rp_tx))
            .unwrap();
        rp_rx.recv().unwrap();

        let (dtx2, drx2) = mpsc::channel();
        commands
            .send(Command::DiscoverService("web".to_string(), dtx2))
            .unwrap();
        assert_eq!(
            drx2.recv().unwrap().unwrap(),
            vec![],
            "a fully torn-down replica is no longer discoverable"
        );
    }

    #[test]
    fn reconcile_services_picks_a_distinct_standby_for_a_new_stateful_replica() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node_with_replicate_addr(
            &commands,
            "node-1",
            "10.0.0.1",
            "10.0.0.1:7622",
            4.0,
            8 * 1024 * 1024 * 1024,
        );
        register_node_with_replicate_addr(
            &commands,
            "node-2",
            "10.0.0.2",
            "10.0.0.2:7622",
            4.0,
            8 * 1024 * 1024 * 1024,
        );
        apply_service_with_template(&commands, "db", 1, stateful_template());

        let actions = reconcile(&commands);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ReplicaAction::Schedule {
                node_id,
                standby_node_id,
                standby_addr,
                ..
            } => {
                let standby = standby_node_id
                    .as_ref()
                    .expect("expected a standby to be picked for a stateful replica");
                assert_ne!(
                    standby, node_id,
                    "standby must be a different node than the primary"
                );
                assert_eq!(
                    *standby_addr,
                    Some("10.0.0.2:7622".to_string()),
                    "expected the standby's advertised replicate_addr, not its HTTP addr"
                );
            }
            other => panic!("expected a Schedule action, got: {other:?}"),
        }
    }

    #[test]
    fn reconcile_services_picks_no_standby_for_a_stateless_replica() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 1);

        let actions = reconcile(&commands);
        match &actions[0] {
            ReplicaAction::Schedule {
                standby_node_id,
                standby_addr,
                ..
            } => {
                assert_eq!(*standby_node_id, None);
                assert_eq!(*standby_addr, None);
            }
            other => panic!("expected a Schedule action, got: {other:?}"),
        }
    }

    #[test]
    fn reconcile_services_leaves_a_stateful_replica_without_a_standby_when_only_one_node_is_alive()
    {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service_with_template(&commands, "db", 1, stateful_template());

        let actions = reconcile(&commands);
        match &actions[0] {
            ReplicaAction::Schedule {
                standby_node_id,
                standby_addr,
                ..
            } => {
                assert_eq!(
                    *standby_node_id, None,
                    "no second node exists to serve as a standby"
                );
                assert_eq!(*standby_addr, None);
            }
            other => panic!("expected a Schedule action, got: {other:?}"),
        }
    }

    fn prepare_force_repin(
        commands: &Sender<Command>,
        replica_name: &str,
    ) -> Result<ForceRepinPrep, ForceRepinError> {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::PrepareForceRepin(replica_name.to_string(), tx))
            .unwrap();
        rx.recv().unwrap()
    }

    fn prepare_drain_repin(
        commands: &Sender<Command>,
        replica_name: &str,
    ) -> Result<ForceRepinPrep, ForceRepinError> {
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::PrepareDrainRepin(replica_name.to_string(), tx))
            .unwrap();
        rx.recv().unwrap()
    }

    #[test]
    fn prepare_force_repin_on_an_unplaced_name_returns_not_placed() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        assert_eq!(
            prepare_force_repin(&commands, "db-0"),
            Err(ForceRepinError::NotPlaced("db-0".to_string()))
        );
    }

    #[test]
    fn prepare_force_repin_on_a_name_with_no_standby_returns_not_stateful() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 1);
        record_placement(&commands, "web-0", "node-1");

        assert_eq!(
            prepare_force_repin(&commands, "web-0"),
            Err(ForceRepinError::NotStateful("web-0".to_string()))
        );
    }

    #[test]
    fn prepare_force_repin_while_the_primary_still_resolves_alive_is_rejected() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service_with_template(&commands, "db", 1, stateful_template());
        record_placement(&commands, "db-0", "node-1");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-2".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(
            prepare_force_repin(&commands, "db-0"),
            Err(ForceRepinError::PrimaryStillAlive("node-1".to_string()))
        );
    }

    #[test]
    fn prepare_force_repin_still_refuses_against_a_still_alive_primary() {
        // Unchanged regression: the ordinary (non-drain) path keeps refusing,
        // even after PrepareForceRepin's body is refactored to share
        // prepare_repin with PrepareDrainRepin.
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service_with_template(&commands, "db", 1, stateful_template());
        record_placement(&commands, "db-0", "node-1");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-2".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        assert!(matches!(
            prepare_force_repin(&commands, "db-0"),
            Err(ForceRepinError::PrimaryStillAlive(_))
        ));
    }

    #[test]
    fn prepare_drain_repin_succeeds_against_a_still_alive_primary() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        // node-3 needs a real advertised replicate_addr, same as the
        // existing force-repin happy-path test, so a fresh standby can be
        // picked for the promoted primary's spec.
        register_node_with_replicate_addr(
            &commands,
            "node-3",
            "10.0.0.3",
            "10.0.0.3",
            4.0,
            8 * 1024 * 1024 * 1024,
        );
        apply_service_with_template(&commands, "db", 1, stateful_template());
        record_placement(&commands, "db-0", "node-1");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-2".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        let prep = prepare_drain_repin(&commands, "db-0").unwrap();
        assert_eq!(prep.old_node_id, "node-1");
        assert_eq!(prep.standby_node_id, "node-2");
    }

    #[test]
    fn prepare_drain_repin_still_refuses_a_non_stateful_name() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service(&commands, "web", 1);
        record_placement(&commands, "web-0", "node-1");

        assert_eq!(
            prepare_drain_repin(&commands, "web-0"),
            Err(ForceRepinError::NotStateful("web-0".to_string()))
        );
    }

    #[test]
    fn prepare_force_repin_happy_path_picks_a_fresh_standby_and_a_free_address() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        // node-3 needs a real advertised replicate_addr: PrepareForceRepin's
        // fresh-standby pick deliberately reads registry.replicate_addr(),
        // not resolve() (Task 8b's distinction), which is None unless a node
        // has explicitly registered one.
        register_node_with_replicate_addr(
            &commands,
            "node-3",
            "10.0.0.3",
            "10.0.0.3",
            4.0,
            8 * 1024 * 1024 * 1024,
        );
        apply_service_with_template(&commands, "db", 1, stateful_template());
        // db-0's primary ("node-unreachable") is never registered, so
        // registry.resolve() fails for it exactly like a genuinely Dead
        // node -- the same trick this file's existing pinning tests use.
        record_placement(&commands, "db-0", "node-unreachable");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-2".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        let prep = prepare_force_repin(&commands, "db-0").unwrap();
        assert_eq!(prep.old_node_id, "node-unreachable");
        assert_eq!(prep.standby_node_id, "node-2");
        assert_eq!(prep.standby_addr, "10.0.0.2");
        assert_eq!(prep.fresh_standby_node_id, "node-3");
        assert_eq!(prep.fresh_standby_addr, "10.0.0.3");
        assert_eq!(prep.template, stateful_template());
    }

    #[test]
    fn prepare_force_repin_with_no_alive_node_left_for_a_fresh_standby_is_rejected() {
        let commands = spawn(
            Registry::new(test_cluster_cidr()),
            Placements::new(),
            Services::new(test_service_cidr()),
            UsedAddresses::new(),
            Standbys::new(),
            PendingFences::new(),
            Cordoned::new(),
            fresh_state_dir(),
        )
        .1;
        register_node(&commands, "node-2", "10.0.0.2", 4.0, 8 * 1024 * 1024 * 1024);
        apply_service_with_template(&commands, "db", 1, stateful_template());
        record_placement(&commands, "db-0", "node-unreachable");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::RecordStandby(
                "db-0".to_string(),
                "node-2".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(
            prepare_force_repin(&commands, "db-0"),
            Err(ForceRepinError::NoFreshStandby)
        );
    }
}
