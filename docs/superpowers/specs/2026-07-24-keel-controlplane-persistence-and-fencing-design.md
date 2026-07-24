# keel-controlplane: State Persistence and Proactive Fencing

Status: Approved (design), pending implementation
Date: 2026-07-24

## Motivation

This is Phase 4 of the 2026-07-24 audit remediation pass (see the published
audit artifact for the full findings list; Phases 1-3, spec validation,
network hardening, crash-safety ordering, are already merged). It's the one
finding explicitly called out as needing its own design pass rather than a
one-line patch, because it's architectural: `keel-controlplane` keeps its
entire scheduling state in memory, and its fencing mechanism only reaches a
fenced node when that node's own next heartbeat happens to arrive.

Concretely, today:

- `Registry` (nodes), `Placements` (jail_name -> node_id), `Services`
  (service definitions), `UsedAddresses` (jail_name -> (node_id, address)),
  `Standbys` (replica_name -> standby node_id), and `PendingFences`
  (replica_name -> node_id owed a forced delete) are all plain in-memory
  structs, wiped on every restart.
- `Services` alone has an external workaround: `keelctl/rc.d/keel_seed_services`
  re-applies every local Service YAML file at boot, which works because
  `Services::apply` is idempotent. Nothing reseeds the other four.
- A restart with `Placements`/`UsedAddresses` gone makes `ReconcileServices`
  see every existing replica as "missing" (since presence is judged purely by
  `Placements`), scheduling a second, permanently duplicate jail wherever the
  fresh pick lands on a different node than before, routine in any
  multi-node cluster, not a rare race.
- A restart during an open `PendingFences` entry silently drops it forever:
  the old, presumed-dead primary is never told to stop, and if it was only
  partitioned from the control plane (not actually dead), it and the
  promoted standby run split-brain indefinitely.
- Separately (not a restart issue): a force-repin only ever pushes the old
  primary's forced delete from inside that *specific node's own* next
  successful heartbeat handler. A node that's alive but not currently
  heartbeating to the control plane (an asymmetric network blip, a firewall
  rule, anything short of a hard crash) never receives it until it manages to
  heartbeat again, which may be never, in exactly the partition scenario
  fencing exists to handle.

This document designs the fix for both: real persistence for the four
never-reseeded collections (`Registry` is deliberately excluded, see Goals),
and a proactive fencing push that doesn't wait for the fenced node to call in.

## Goals

- `Placements`, `UsedAddresses`, `Standbys`, `PendingFences`, and `Services`
  all survive a `keel-controlplane` restart with no external script involved.
- The reconcile-after-restart behavior for `Placements`/`UsedAddresses` no
  longer treats every existing replica as newly missing.
- A fence recorded by `handle_force_repin` attempts to reach the old primary
  immediately, not only via that node's next heartbeat.
- Every change lands with tests proving the specific failure mode it fixes,
  matching the project's existing verification standard.

## Non-goals

- **`Registry` persistence.** Nodes already re-register periodically
  (`keel-agentd`'s `registration.rs` retries continuously), and
  `Registry::register` is itself idempotent for a known id, so a restarted
  control plane recovers every live node's entry within one registration
  cycle with no special handling. Persisting it would add a stale-data risk
  (a node the registry "remembers" but that's actually gone) for no real
  benefit.
- **True STONITH / power fencing.** The immediate-push fencing fix closes the
  common case (asymmetric network issues, a node that hasn't heartbeated
  recently for unrelated reasons). It cannot, and does not try to, guarantee
  fencing across a genuine bidirectional network partition between the old
  primary and the control plane, nothing software-only can solve that
  without an out-of-band power-control mechanism, which is out of scope for
  this project's current size.
- **Removing `keel_seed_services`.** Once `Services` is truly persisted, the
  rc.d script becomes redundant, but it's already idempotent and harmless to
  leave running. Removing it is an independent operational cleanup, not
  bundled into this change.
- **A write-ahead log or any transactional multi-collection consistency.**
  Each collection is persisted independently, matching `keel-agentd`'s own
  `store.rs` precedent. This document does not attempt to make, e.g., a
  `RecordPlacement` and its paired `RecordReplicaAddress` atomic with each
  other across a crash between the two writes, that narrow window already
  exists today in-memory (two separate `Command`s, two separate mutations)
  and is unchanged by adding persistence to each independently.

## Architecture: persistence mechanism

A new `keel-controlplane/src/store.rs`, mirroring `keel-agentd/src/store.rs`'s
write-tmp-then-rename atomicity but generic, since these five collections are
simple enough not to need per-record files:

```rust
pub fn load_or_default<T: Default + serde::de::DeserializeOwned>(path: &Path) -> T {
    match fs::read_to_string(path) {
        Ok(content) => serde_yaml::from_str(&content).unwrap_or_else(|e| {
            panic!("failed to parse state file {}: {e}", path.display())
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => T::default(),
        Err(e) => panic!("failed to read state file {}: {e}", path.display()),
    }
}

pub fn save<T: serde::Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("yaml.tmp");
    let content = serde_yaml::to_string(value).expect("state serialization should not fail");
    fs::write(&tmp_path, content)?;
    fs::rename(&tmp_path, path)
}
```

Both take a full file path (symmetric with each other), so every call site
looks the same shape whether loading or saving: `store::save(&state_dir.join("placements.yaml"), &placements)`.

(A malformed state file panics at startup rather than silently discarding
data, the same choice `keel-agentd`'s `Reconciler::new` makes implicitly by
propagating `StoreError` up through `?`. An operator hand-editing or losing a
state file is rare enough, and the consequence of silently ignoring it
severe enough (reverting to an empty collection and duplicating every
replica) that failing loudly at startup is the safer default.)

`Placements`, `UsedAddresses`, `Standbys`, and `PendingFences` each derive
`Serialize`/`Deserialize` on their existing private `HashMap` fields (no
observable API change, these derives are additive) and are loaded/saved
directly through the two functions above with a fixed filename each:
`placements.yaml`, `used_addresses.yaml`, `standbys.yaml`,
`pending_fences.yaml`.

`Services` is a special case: it holds `service_cidr` (from the
`--service-cidr` CLI flag, not persisted data) alongside `by_name`.
Persisting `service_cidr` would let a changed CLI flag get silently
overridden by stale on-disk data after a restart, exactly the kind of
footgun this design should avoid. Instead:

```rust
#[derive(Serialize, Deserialize, Default)]
struct ServicesState {
    by_name: HashMap<String, ServiceRecord>,
}

impl Services {
    pub fn load(state_dir: &Path, service_cidr: Ipv4Net) -> Self {
        let state: ServicesState = store::load_or_default(&state_dir.join("services.yaml"));
        Self { service_cidr, by_name: state.by_name }
    }

    fn persist(&self, state_dir: &Path) {
        let state = ServicesState { by_name: self.by_name.clone() };
        if let Err(e) = store::save(&state_dir.join("services.yaml"), &state) {
            eprintln!("keel-controlplane: failed to persist services.yaml: {e}");
        }
    }
}
```

`ServiceRecord` already derives the necessary traits transitively through
`keel_spec::JailTemplate`, which is already `Serialize`/`Deserialize` (used
for wire transport), no new derives needed there.

A persist failure is logged (`eprintln!`), not propagated as a command
error: the in-memory mutation has already happened and the command's actual
outcome (e.g. "service applied") is correct regardless of whether the disk
write succeeded. This matches how `keel-agentd`'s reconciler already treats
persistence failures in comparable positions (e.g. `reconcile_certs` logging
and backing off rather than failing the whole reconcile pass).

## Worker wiring

`worker::spawn`'s signature is unchanged in shape, it still takes the five
already-constructed collections, plus one new parameter, `state_dir:
PathBuf`, threaded into `handle_command` the same way `pool`/`state_dir`
already flow through `keel-agentd`'s `worker::spawn`/`handle_command`.

`main.rs` does the loading, mirroring `Reconciler::new`:

```rust
let placements: Placements = keel_controlplane::store::load_or_default(&state_dir.join("placements.yaml"));
let used_addresses: UsedAddresses = keel_controlplane::store::load_or_default(&state_dir.join("used_addresses.yaml"));
let standbys: Standbys = keel_controlplane::store::load_or_default(&state_dir.join("standbys.yaml"));
let pending_fences: PendingFences = keel_controlplane::store::load_or_default(&state_dir.join("pending_fences.yaml"));
let services = Services::load(&state_dir, service_cidr);
```

Nine `Command` branches in `handle_command` gain a persist call immediately
after their existing in-memory mutation:

| Command | Persists |
|---|---|
| `RecordPlacement` | `placements.yaml` |
| `RemovePlacement` | `placements.yaml` |
| `RecordReplicaAddress` | `used_addresses.yaml` |
| `ReleaseReplicaAddress` | `used_addresses.yaml` |
| `RecordStandby` | `standbys.yaml` |
| `RecordPendingFence` | `pending_fences.yaml` |
| `RemovePendingFence` | `pending_fences.yaml` |
| `ApplyService` (on success) | `services.yaml` |
| `DeleteService` (on success) | `services.yaml` |

Every other command (`Register`, `Heartbeat`, `List`, `Resolve`,
`ResolveOrSchedule`, `ResolvePlacement`, `OwnerOf`, `ReconcileServices`,
`DiscoverService`, `ListServices`, `ListServiceProxyEntries`,
`PendingFencesForNode`, `PrepareForceRepin`) is read-only against these five
collections and is untouched.

## Startup, CLI, and rc.d

`keel-controlplane/src/main.rs` gains a `--state-dir` flag, defaulting to
`/var/db/keel-controlplane`, parsed the same way `keel-agentd`'s existing
`--state-dir`/`--pool` flags are.

`keel-controlplane/rc.d/keel_controlplane` gains a
`keel_controlplane_state_dir` rc.conf variable, appended to `command_args`
the same way every other optional flag in that script already is (only
passed when the operator has set it, defaulting in the binary itself
otherwise).

`keelctl/rc.d/keel_seed_services` is left exactly as-is (see Non-goals).

## Fencing: immediate push

`Registry` gains one new read-only method:

```rust
/// The node's last-registered address, with no aliveness check --
/// deliberately bypassing `DEAD_THRESHOLD`. Used only by the immediate
/// force-repin fencing push: the entire point is attempting to reach a node
/// the heartbeat-derived state currently calls "dead," in case it's actually
/// alive and only failing to heartbeat to the control plane specifically.
pub fn last_known_addr(&self, node_id: &str) -> Option<String> {
    self.nodes.get(node_id).map(|r| r.addr.clone())
}
```

`Registry` (and therefore this method) only lives inside the worker thread,
never in `http.rs`, so `handle_force_repin` cannot call it directly. Rather
than adding a whole new `Command` round-trip just to fetch one address,
`ForceRepinPrep` gains one new field, computed in the same
`Command::PrepareForceRepin` handler that already builds the rest of the
struct (right where `registry` is already in scope):

```rust
pub struct ForceRepinPrep {
    pub old_node_id: String,
    pub old_node_last_known_addr: Option<String>, // new
    pub standby_node_id: String,
    // ...unchanged fields...
}
```

```rust
// inside the existing Command::PrepareForceRepin handler in worker.rs,
// after old_node_id is resolved and the PrimaryStillAlive check has passed:
let old_node_last_known_addr = registry.last_known_addr(&old_node_id);
```

In `handle_force_repin` (`http.rs`), immediately after
`send_record_pending_fence(name, &prep.old_node_id, commands)` succeeds:

```rust
if let Some(old_addr) = &prep.old_node_last_known_addr {
    match forward(old_addr, "DELETE", &format!("/jails/{name}"), &[], client_config) {
        Ok((status, _)) if (200..300).contains(&status) || status == 404 => {
            send_remove_pending_fence(name, commands);
        }
        _ => {
            // Left in place for check_and_execute_fencing to retry the next
            // time (if ever) this node's own heartbeat reaches the control
            // plane, unchanged from today's only mechanism.
        }
    }
}
```

This does not change `check_and_execute_fencing`'s existing heartbeat-gated
retry at all, it remains exactly as the fallback for when the immediate
attempt above fails (the node truly is unreachable right now). The two
mechanisms are independent and both end in the same
`send_remove_pending_fence` call, so there's no risk of double-clearing or
conflicting state.

## Testing

- `store.rs`: round-trip tests matching `keel-agentd/src/store.rs`'s own
  style, save then load returns the same value, a missing file loads as
  `Default`, a directory that doesn't exist yet is created on save.
- Each of `Placements`/`UsedAddresses`/`Standbys`/`PendingFences`: a
  serialize/deserialize round-trip test (proving the new derives produce
  the expected YAML shape and read it back correctly).
- `Services::load`/`persist`: a round-trip test proving `service_cidr` comes
  from the passed-in argument, not from disk, even when a *different*
  `service_cidr` was in effect when the file was last written.
- `worker.rs`: for each of the 9 mutating commands, a test that sends the
  command against a `spawn()` pointed at a real temp `state_dir`, then
  constructs a **second, independent** `spawn()` over that same
  `state_dir` (simulating a restart) and confirms the mutation is visible
  in the fresh instance, mirroring
  `resume_replication_loops_starts_a_loop_for_a_record_persisted_before_a_restart`'s
  existing restart-simulation pattern in `keel-agentd`.
- One test proving the specific bug this phase exists to fix: apply a
  service, record a placement and a replica address, restart (fresh
  `spawn()` over the same `state_dir`), and confirm `ReconcileServices`
  computes zero actions, i.e. the previously-placed replica is not seen
  as missing and does not get a duplicate scheduled.
- Fencing, two tiers (a real constraint surfaced while reviewing this spec:
  every existing `PrimaryStillAlive`-adjacent test in this codebase stands in
  for "the old primary is Dead" by using a node id that was *never
  registered at all* -- `registry.resolve()` fails for it exactly like a
  genuinely Dead node, with no need to wait out the real 15-second
  `DEAD_THRESHOLD`. That trick doesn't work for testing the immediate push
  specifically, since an unregistered node has no address on file for
  `last_known_addr` to return -- there'd be nothing to push to):
  - A fast, direct `Registry`-level unit test (no `worker`/`Command` channel
    involved, matching `registry.rs`'s own existing style of advancing an
    arithmetic `Instant` rather than sleeping): register a node, call
    `resolve()` with a `now` past `DEAD_THRESHOLD` to confirm it reports
    Dead, then call `last_known_addr()` with that same registered id and
    confirm it still returns the address regardless.
  - One true end-to-end integration test proving `handle_force_repin`
    actually performs the immediate push through the full pipeline (real
    `worker::spawn`, a real fake node HTTP server, a real `forward()` call):
    register the old primary with a real reachable fake address, then
    `std::thread::sleep` past the real 15-second `DEAD_THRESHOLD` (unavoidable
    here -- `Command::PrepareForceRepin`'s `now` comes from a live
    `Instant::now()` inside `handle_command`, with no test-only override, and
    adding one would be a larger, separate change touching several other
    commands' shapes for a single test's benefit) before triggering the
    force-repin and confirming the `DELETE` arrives at the fake server with
    no heartbeat ever sent from it.

## Verification plan

- Full workspace test suite green (`cargo test --workspace`), as with every
  prior phase.
- Manually confirm on the real FreeBSD VM (matching this project's existing
  methodology of verifying real-system behavior, not just fakes): apply a
  service with a stateful replica, restart `keel-controlplane`, confirm
  `GET /services/<name>` still reports the existing replica healthy with no
  duplicate jail appearing on any node.
