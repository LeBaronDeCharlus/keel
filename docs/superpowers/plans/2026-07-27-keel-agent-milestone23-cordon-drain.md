# Milestone 23: Cordon and Drain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `keelctl cordon <node>` / `uncordon <node>` (mark a node schedulable or not, with no disruption to what's already running) and `keelctl drain <node>` (actively empty a schedulable node of everything it hosts, so it becomes safe to take down), per `docs/superpowers/specs/2026-07-27-keel-agent-milestone23-cordon-drain-design.md`.

**Architecture:** A new `Cordoned` set, persisted the same way `Standbys`/`PendingFences` already are, threaded through `worker::spawn` alongside them. One shared `is_schedulable` helper replaces three independently-duplicated `NodeState::Alive`-only filters. `drain` handles three kinds of placed work with three different mechanisms: a stateless `Service` replica already self-heals once its placement is removed (the existing `ReconcileServices` reconcile pass does the rest); a stateful replica reuses Milestone 19's `PrepareForceRepin` machinery through a new, narrower bypass of its `PrimaryStillAlive` guard; a plain `kind: Jail` gets genuinely new orchestration (fetch its spec from the old node, place it on a new schedulable node, delete the old one) since no relocation mechanism for it exists today. `drain` refuses upfront if the target node has a live Ingress (already-written contributed section, folded into the design doc) or is already `Dead` (use `force-repin` instead).

**Tech Stack:** No new crates, no new dependencies. All changes are within `keel-controlplane`, `keelctl`, and `keel-dashboard`, using this workspace's existing `thiserror`/`serde_yaml`/`rustls` stack.

## Global Constraints

- Match this project's established per-collection shape exactly: a small owned module (`keel-controlplane/src/cordoned.rs`), a plain `HashSet<String>` wrapped in a `#[derive(Debug, Default, Serialize, Deserialize)]` struct, persisted via `crate::store::save`/`crate::store::load_or_default` — the same shape as `keel-controlplane/src/pending_fences.rs`.
- Every error enum uses `thiserror::Error`, one variant per real failure mode, matching `ForceRepinError`/`ScheduleOrResolveError`/`PlacementError`.
- `Cordoned` is deliberately **not** a variant of `NodeState` (`keel-controlplane/src/wire.rs:41-43`) — that enum is purely derived from heartbeat recency on every `Registry::list` call (`keel-controlplane/src/registry.rs:155-166`) and must stay that way. Cordoned status is operator-set and must survive both a missed heartbeat and a control-plane restart.
- Adding a new `Cordoned` parameter to `worker::spawn` and `handle_command` touches every one of that file's ~20 existing `spawn(...)` test call sites (`keel-controlplane/src/worker.rs`, search `Registry::new(test_cluster_cidr())`) — purely mechanical (insert `Cordoned::new()` in the same position as the other five collections), called out once here rather than enumerated per call site.
- Run `cargo test --workspace` and `cargo clippy --workspace --all-targets -- -D warnings` after every task; both must be clean before committing, matching every prior milestone's bar. (Bumble's parallel CI-hygiene work is bringing the workspace to a clean `fmt`/`clippy` baseline concurrently — rebase onto that once it lands rather than fighting pre-existing warnings unrelated to this milestone.)
- FreeBSD-only real-hardware verification (Task 10) follows this project's standing pattern: gated behind real 3-node VM access, run last, never assumed to pass without being actually run.

---

### Task 1: `keel-controlplane` — `Cordoned` state and persistence

**Files:**
- Create: `keel-controlplane/src/cordoned.rs`
- Modify: `keel-controlplane/src/lib.rs` (add `mod cordoned; pub use cordoned::Cordoned;`, matching how `Standbys`/`PendingFences` are exported)
- Modify: `keel-controlplane/src/worker.rs` (new `Command::Cordon`/`Command::Uncordon` variants, `spawn`/`handle_command` gain a `cordoned: Cordoned` parameter, new `persist_cordoned` function mirroring `persist_standbys`/`persist_pending_fences` at `worker.rs:191-210`)
- Modify: `keel-controlplane/src/main.rs` (load `cordoned.yaml` at startup alongside the other four persisted collections, `main.rs:101-118`, pass into `worker::spawn`)

**Interfaces:**
- Produces: `keel_controlplane::Cordoned::new() -> Self`; `Cordoned::cordon(&mut self, node_id: String)`; `Cordoned::uncordon(&mut self, node_id: &str)`; `Cordoned::is_cordoned(&self, node_id: &str) -> bool`; `Command::Cordon(String, Sender<Result<(), UnknownNode>>)`; `Command::Uncordon(String, Sender<Result<(), UnknownNode>>)`.
- Consumes: `keel_controlplane::registry::UnknownNode` (already exists, `registry.rs:28-30`) as the "no such node" error shape, matching every other node-id-taking path.

- [x] **Step 1: Write the failing tests in `keel-controlplane/src/cordoned.rs`**

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cordoned {
    ids: HashSet<String>,
}

impl Cordoned {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cordon(&mut self, node_id: String) {
        self.ids.insert(node_id);
    }

    pub fn uncordon(&mut self, node_id: &str) {
        self.ids.remove(node_id);
    }

    pub fn is_cordoned(&self, node_id: &str) -> bool {
        self.ids.contains(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_node_is_not_cordoned() {
        assert!(!Cordoned::new().is_cordoned("node-1"));
    }

    #[test]
    fn cordon_marks_a_node_cordoned() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        assert!(c.is_cordoned("node-1"));
        assert!(!c.is_cordoned("node-2"));
    }

    #[test]
    fn cordoning_twice_is_idempotent() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        c.cordon("node-1".to_string());
        assert!(c.is_cordoned("node-1"));
    }

    #[test]
    fn uncordon_clears_it() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        c.uncordon("node-1");
        assert!(!c.is_cordoned("node-1"));
    }

    #[test]
    fn uncordoning_an_uncordoned_node_is_a_harmless_no_op() {
        let mut c = Cordoned::new();
        c.uncordon("node-1");
        assert!(!c.is_cordoned("node-1"));
    }

    #[test]
    fn cordoned_round_trips_through_yaml() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        let path = std::env::temp_dir().join(format!("keel-controlplane-cordoned-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &c).unwrap();
        let loaded: Cordoned = crate::store::load_or_default(&path);
        assert!(loaded.is_cordoned("node-1"));
        assert!(!loaded.is_cordoned("node-2"));
    }
}
```

Run `cargo test -p keel-controlplane cordoned::` — all six pass immediately against this implementation (this type is simple enough that test-first and implementation land together, matching how `PendingFences` itself was written).

- [x] **Step 2: Wire `Command::Cordon`/`Command::Uncordon` into `worker.rs`**

Add to the `Command` enum (alongside `RecordStandby`/`RemoveStandby`, `worker.rs:130-131`):

```rust
Cordon(String, Sender<Result<(), UnknownNode>>),
Uncordon(String, Sender<Result<(), UnknownNode>>),
```

Add to `spawn`'s signature and its `mpsc::Receiver` loop body (`worker.rs:156-165`), and to `handle_command`'s signature (`worker.rs:223-230`), a new `mut cordoned: Cordoned` parameter, positioned after `pending_fences` in both. Add the persistence helper (mirroring `persist_pending_fences`, `worker.rs:209-211`):

```rust
fn persist_cordoned(cordoned: &Cordoned, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("cordoned.yaml"), cordoned) {
        eprintln!("keel-controlplane: failed to persist cordoned state: {e}");
    }
}
```

Add the two match arms in `handle_command` (mirroring `Command::RecordStandby`'s validate-then-mutate-then-persist shape):

```rust
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
```

`registry.pod_cidr(&node_id).is_some()` is the existing, cheapest "is this a known node id at all" check already used elsewhere in this file (no aliveness implied — cordoning a currently-`Dead`-but-previously-registered node is legitimate and must succeed, matching `last_known_addr`'s same "no aliveness check on purpose" precedent at `registry.rs:145-152`).

- [x] **Step 3: Update every `spawn(...)` test call site**

Mechanical sweep across `keel-controlplane/src/worker.rs`'s ~20 test functions: each `spawn(Registry::new(test_cluster_cidr()), Placements::new(), Services::new(test_service_cidr()), UsedAddresses::new(), Standbys::new(), PendingFences::new(), state_dir)` call gains one more argument, `Cordoned::new()`, in the same position as the other five. Also update the two production call sites: `main.rs`'s `worker::spawn(...)` call (load `cordoned.yaml` first, matching the existing four-collection load block at `main.rs:101-114`) and any other test harness in `http.rs`'s test module that constructs a worker directly (`grep -n "worker::spawn" keel-controlplane/src/http.rs` to enumerate).

- [x] **Step 4: Run full verification**

```bash
cargo test -p keel-controlplane
cargo clippy -p keel-controlplane --all-targets -- -D warnings
```

---

### Task 2: `keel-controlplane` — one shared `is_schedulable` helper

**Files:**
- Modify: `keel-controlplane/src/worker.rs` (new private helper function; replace the three duplicated filters at `worker.rs:250`, `319`, `571`)

**Interfaces:**
- Produces: `fn is_schedulable(status: &wire::NodeStatus, cordoned: &Cordoned) -> bool` (private to `worker.rs`, no new public surface).
- Consumes: `NodeState::Alive` comparison (unchanged), `Cordoned::is_cordoned`.

- [x] **Step 1: Write the failing test**

```rust
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
```

(`test_node_status` is a small new test helper constructing a minimal `NodeStatus` with the given `id`/`status` and zeroed resource fields — follow the existing `NodeStatus { .. }` literal pattern already used in this file's other tests, e.g. around `worker.rs:712`.)

- [x] **Step 2: Implement and wire in at all three call sites**

```rust
fn is_schedulable(status: &wire::NodeStatus, cordoned: &Cordoned) -> bool {
    status.status == NodeState::Alive && !cordoned.is_cordoned(&status.id)
}
```

Replace `.filter(|status| status.status == NodeState::Alive)` at `worker.rs:250` (inside `Command::ResolveOrSchedule`) with `.filter(|status| is_schedulable(status, cordoned))`. Replace the identical filter at `worker.rs:319` (`Command::ReconcileServices`'s `alive_nodes` build) the same way. Replace the third, inside the per-replica standby-selection block at `worker.rs:571`, identically. All three sites already have `cordoned` in scope once Task 1's `handle_command` signature change lands.

No other change: `scheduler::pick_node`'s bin-packing and `services::pick_node_for_service`'s same-service spreading (`services.rs:199-207`) are completely untouched — only the candidate set feeding them changes.

- [x] **Step 3: Regression + verification**

Existing tests exercising `ResolveOrSchedule`/`ReconcileServices` against an all-`Alive`, never-cordoned cluster must still pass unchanged — this proves the change is additive on the common case. Then:

```bash
cargo test -p keel-controlplane
cargo clippy -p keel-controlplane --all-targets -- -D warnings
```

---

### Task 3: `keel-controlplane` — `POST /nodes/<id>/cordon` and `/uncordon` HTTP routes

**Files:**
- Modify: `keel-controlplane/src/http.rs` (two new route-table entries alongside `("POST", ["replicas", name, "force-repin"])` at `http.rs:181`; two new handler functions)

**Interfaces:**
- Produces: `fn handle_cordon(node_id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>)`; `fn handle_uncordon(node_id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>)`.
- Consumes: `Command::Cordon`/`Command::Uncordon` (Task 1).

- [x] **Step 1: Write the failing integration tests**

Following this file's existing `send_request(&cp_addr, ...)` test harness pattern (e.g. `http.rs:1391-1412`):

```rust
#[test]
fn cordon_an_unknown_node_returns_404() {
    let cp_addr = start_test_control_plane();
    let (status, _) = send_request(&cp_addr, "POST", "/nodes/ghost/cordon", "");
    assert_eq!(status, 404);
}

#[test]
fn cordon_then_list_shows_the_node_excluded_from_a_fresh_schedule() {
    let cp_addr = start_test_control_plane();
    register_test_node(&cp_addr, "node-1");
    let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
    assert_eq!(status, 200);
    // A fresh PUT /jails/<name> with only node-1 registered now has no
    // schedulable candidate at all.
    let (status, _) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
    assert_eq!(status, 503);
}

#[test]
fn uncordon_restores_schedulability() {
    let cp_addr = start_test_control_plane();
    register_test_node(&cp_addr, "node-1");
    send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
    send_request(&cp_addr, "POST", "/nodes/node-1/uncordon", "");
    let (status, _) = send_request(&cp_addr, "PUT", "/jails/web-1", "apiVersion: keel/v1\n");
    assert_eq!(status, 200);
}

#[test]
fn cordoning_an_already_cordoned_node_is_a_no_op_success() {
    let cp_addr = start_test_control_plane();
    register_test_node(&cp_addr, "node-1");
    send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
    let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/cordon", "");
    assert_eq!(status, 200);
}
```

(`register_test_node` is a small helper if one doesn't already exist — check for an existing equivalent in this file's test module first, e.g. around the node-registration tests near `http.rs:1645`, before adding a new one.)

- [x] **Step 2: Add the route entries and handlers**

```rust
("POST", ["nodes", id, "cordon"]) => handle_cordon(id, commands),
("POST", ["nodes", id, "uncordon"]) => handle_uncordon(id, commands),
```

```rust
fn handle_cordon(node_id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::Cordon(node_id.to_string(), reply_tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(404, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_uncordon(node_id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::Uncordon(node_id.to_string(), reply_tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e)) => error_response(404, e.to_string()),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}
```

- [x] **Step 3: Verification**

```bash
cargo test -p keel-controlplane
cargo clippy -p keel-controlplane --all-targets -- -D warnings
```

---

### Task 4: `keelctl` — `cordon`/`uncordon` subcommands

**Files:**
- Modify: `keelctl/src/main.rs` (dispatch table at `main.rs:49-62`; two new `run_*` functions alongside `run_force_repin`, `main.rs:190-194`)

**Interfaces:**
- Produces: `keelctl cordon <node>`, `keelctl uncordon <node>` CLI subcommands.
- Consumes: nothing new in `keel-agentd`/`ErrorBody` — these always go to the control plane directly (a node-targeting operation, never proxied through a single node's own socket the way `apply`/`get`/`delete` can be), so both require `--control-plane-addr`; reuse whatever existing error keelctl surfaces today when a `Target::Socket` is used for a control-plane-only command (check `jails_path`'s existing handling of a bare-socket target attempting a control-plane-only path, e.g. how `force-repin` already behaves against a plain socket target, and match it — likely a plain HTTP call against the socket that 404s, which is an acceptable existing precedent to reuse rather than invent new plumbing for).

- [ ] **Step 1: Write the failing CLI-level tests**

Following the existing pattern at `keelctl/src/main.rs:373-412` (spinning up a `start_test_agent`/mock control-plane listener and calling the `run_*` function directly rather than shelling out):

```rust
#[test]
fn run_cordon_posts_to_the_nodes_cordon_route() {
    let cp_addr = start_test_control_plane(&[("POST", "/nodes/node-1/cordon", 200, "")]);
    let target = Target::ControlPlane { addr: cp_addr, node: None, /* tls fields */ };
    let result = run_cordon(&target, &["node-1".to_string()]);
    assert!(result.is_ok());
}

#[test]
fn run_cordon_with_no_node_argument_is_a_usage_error() {
    let target = Target::ControlPlane { addr: "127.0.0.1:0".to_string(), node: None, /* tls fields */ };
    let result = run_cordon(&target, &[]);
    assert!(result.is_err());
}
```

- [ ] **Step 2: Implement**

```rust
Some((cmd, rest)) if cmd == "cordon" => run_cordon(&target, rest),
Some((cmd, rest)) if cmd == "uncordon" => run_uncordon(&target, rest),
```

```rust
fn run_cordon(target: &Target, args: &[String]) -> Result<String, String> {
    let node = args.first().ok_or("cordon requires a node id")?;
    let path = format!("/nodes/{node}/cordon");
    success_body(dispatch(target, "POST", &path, "")).map(|_| String::new())
}

fn run_uncordon(target: &Target, args: &[String]) -> Result<String, String> {
    let node = args.first().ok_or("uncordon requires a node id")?;
    let path = format!("/nodes/{node}/uncordon");
    success_body(dispatch(target, "POST", &path, "")).map(|_| String::new())
}
```

Update the usage string at `main.rs:59-61` to include `cordon NODE|uncordon NODE`.

- [ ] **Step 3: Verification**

```bash
cargo test -p keelctl
cargo clippy -p keelctl --all-targets -- -D warnings
```

---

### Task 5: `keel-controlplane` — drain-mode standby promotion (bypass `PrimaryStillAlive`)

**Files:**
- Modify: `keel-controlplane/src/worker.rs` (extract `Command::PrepareForceRepin`'s closure body, `worker.rs:532-608`, into a shared private function; add `Command::PrepareDrainRepin`)

**Interfaces:**
- Produces: `fn prepare_repin(replica_name: &str, allow_alive_primary: bool, registry: &Registry, placements: &Placements, standbys: &Standbys, services: &Services, used_addresses: &UsedAddresses) -> Result<ForceRepinPrep, ForceRepinError>` (private helper); `Command::PrepareDrainRepin(String, Sender<Result<ForceRepinPrep, ForceRepinError>>)`.
- Consumes: everything `PrepareForceRepin` already consumes, unchanged.

This is the one place this milestone reuses Milestone 19's machinery with a real, narrow modification rather than wholesale as-is: `PrepareForceRepin`'s guard at `worker.rs:542-544` (`if registry.resolve(&old_node_id, now).is_ok() { return Err(PrimaryStillAlive) }`) exists specifically to stop promoting a standby out from under a still-live, still-independently-running primary — the split-brain guard. `drain` is calling this *because* the primary is still `Alive`, so it needs a path that skips exactly this one check while leaving every other guard (`NotPlaced`, `NotStateful`, `StandbyUnresolvable`, `NoFreshStandby`, `NoFreeAddress`) fully intact. The split-brain invariant this guard protects still holds under `drain`, because `drain` fences the old primary's jail directly and synchronously as part of the same operation (Task 6) — it is not left running independently the way a merely-`Dead`-per-heartbeat node's jail might still secretly be.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn prepare_drain_repin_succeeds_against_a_still_alive_primary() {
    let commands = spawn(/* registry with node-1 (primary, Alive) and node-2 (standby) registered, a stateful replica placed on node-1 with node-2 as standby and a completed replication */);
    let (tx, rx) = mpsc::channel();
    commands.send(Command::PrepareDrainRepin("db-0".to_string(), tx)).unwrap();
    assert!(rx.recv().unwrap().is_ok());
}

#[test]
fn prepare_force_repin_still_refuses_against_a_still_alive_primary() {
    // Unchanged regression: the ordinary (non-drain) path keeps refusing.
    let commands = spawn(/* same setup */);
    let (tx, rx) = mpsc::channel();
    commands.send(Command::PrepareForceRepin("db-0".to_string(), tx)).unwrap();
    assert!(matches!(rx.recv().unwrap(), Err(ForceRepinError::PrimaryStillAlive(_))));
}

#[test]
fn prepare_drain_repin_still_refuses_a_non_stateful_name() {
    let commands = spawn(/* node-1 hosting a plain Jail "web-1", no standby */);
    let (tx, rx) = mpsc::channel();
    commands.send(Command::PrepareDrainRepin("web-1".to_string(), tx)).unwrap();
    assert!(matches!(rx.recv().unwrap(), Err(ForceRepinError::NotStateful(_))));
}

#[test]
fn prepare_drain_repin_still_refuses_when_the_standby_has_no_completed_replication() {
    // ForceRepinPrep's construction itself doesn't check last_snapshot --
    // that check lives in http.rs's handle_force_repin (http.rs:269-281),
    // checked identically for both PrepareForceRepin and PrepareDrainRepin
    // since it's a property of the *standby*, unrelated to whether the
    // primary happens to be Alive or Dead. Covered by Task 6's HTTP-level
    // test instead of here.
}
```

- [ ] **Step 2: Refactor `PrepareForceRepin`'s closure into `prepare_repin`, add `PrepareDrainRepin`**

Extract the existing closure body (`worker.rs:533-608`) into a standalone function taking one new `allow_alive_primary: bool` parameter, changing only the guard:

```rust
if !allow_alive_primary && registry.resolve(&old_node_id, now).is_ok() {
    return Err(ForceRepinError::PrimaryStillAlive(old_node_id));
}
```

`Command::PrepareForceRepin`'s match arm becomes `prepare_repin(&replica_name, false, ...)`; add:

```rust
Command::PrepareDrainRepin(replica_name, reply) => {
    let _ = reply.send(prepare_repin(&replica_name, true, &registry, placements, standbys, services, used_addresses));
}
```

No change to `ForceRepinPrep`'s fields or to anything downstream in `http.rs`'s `handle_force_repin` — Task 6 adds a parallel, `drain`-specific caller of the same downstream steps (readiness check, forward the promotion, fence the old primary), not a modification of `handle_force_repin` itself.

- [ ] **Step 3: Verification**

```bash
cargo test -p keel-controlplane
cargo clippy -p keel-controlplane --all-targets -- -D warnings
```

---

### Task 6: `keel-controlplane` — `drain` orchestration

**Files:**
- Modify: `keel-controlplane/src/worker.rs` (new `Command::NodePlacements` read-only query, listing every jail/replica name currently placed on a node, split by kind)
- Modify: `keel-controlplane/src/http.rs` (new `handle_drain` function, the bulk of this task)

**Interfaces:**
- Produces: `Command::NodePlacements(String, Sender<Vec<(String, PlacementKind)>>)` where `enum PlacementKind { PlainJail, StatelessService, StatefulService }` (derived from `services::owner_of` + `record.template.volumes.is_empty()`, both already available via `services::owner_of` and `Services::get`); `fn handle_drain(node_id: &str, commands: &Sender<Command>, client_config: &Arc<rustls::ClientConfig>) -> (u16, Vec<u8>)`.
- Consumes: `Command::List` (Dead check, Ingress check), `Command::Cordon` (Task 1, implicit re-cordon), `Command::RemovePlacement` (existing), `Command::PrepareDrainRepin` (Task 5), `is_schedulable`/scheduling machinery (Task 2), `handle_scheduled_read`'s underlying `forward` primitive (existing, `http.rs:220-233`).

- [ ] **Step 1: Write the failing integration tests**

```rust
#[test]
fn drain_a_dead_node_is_refused() {
    let cp_addr = start_test_control_plane();
    register_test_node(&cp_addr, "node-1");
    // advance past DEAD_THRESHOLD with no heartbeat, or use a short test threshold if one exists
    let (status, body) = send_request(&cp_addr, "POST", "/nodes/node-1/drain", "");
    assert_eq!(status, 409);
    assert!(String::from_utf8_lossy(&body).contains("force-repin"));
}

#[test]
fn drain_refuses_a_node_with_a_live_ingress() {
    let cp_addr = start_test_control_plane();
    register_test_node(&cp_addr, "node-1");
    send_heartbeat_with_ingress(&cp_addr, "node-1", "example.com");
    let (status, body) = send_request(&cp_addr, "POST", "/nodes/node-1/drain", "");
    assert_eq!(status, 409);
    assert!(String::from_utf8_lossy(&body).contains("example.com"));
}

#[test]
fn drain_reschedules_a_stateless_service_replica_elsewhere() {
    let cp_addr = start_test_control_plane();
    // node-1 and node-2 both registered and heartbeating; web-0 placed on node-1
    let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/drain", "");
    assert_eq!(status, 200);
    // trigger one more heartbeat/reconcile pass, then confirm web-0 (or its replacement index) now resolves to node-2
}

#[test]
fn drain_migrates_a_plain_jail_to_a_schedulable_node() {
    let cp_addr = start_test_control_plane();
    // node-1 (source, has plain Jail "solo-1") and node-2 (target) both alive
    let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/drain", "");
    assert_eq!(status, 200);
    let (_, body) = send_request(&cp_addr, "GET", "/jails/solo-1", "");
    // confirm it now resolves against node-2's fake agent, not node-1's
}

#[test]
fn drain_promotes_a_stateful_replicas_standby_and_fences_the_old_primary() {
    let cp_addr = start_test_control_plane();
    // node-1 primary (db-0), node-2 standby with a completed replication, node-3 available as a fresh standby
    let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/drain", "");
    assert_eq!(status, 200);
    // confirm Placements["db-0"] == node-2, Standbys["db-0"] == node-3, and node-1's fake agent shows db-0 deleted
}

#[test]
fn drain_leaves_an_empty_node_as_a_trivial_success() {
    let cp_addr = start_test_control_plane();
    register_test_node(&cp_addr, "node-1"); // nothing placed on it, no ingresses
    let (status, _) = send_request(&cp_addr, "POST", "/nodes/node-1/drain", "");
    assert_eq!(status, 200);
}

#[test]
fn drain_leaves_a_failed_migration_in_place_rather_than_deleting_the_original() {
    // Target node unreachable/rejects the PUT for the plain-Jail case:
    // original placement and jail on the source node must be untouched,
    // and the response must name the placement that failed to move.
}
```

- [ ] **Step 2: Add `Command::NodePlacements`**

```rust
Command::NodePlacements(node_id, reply) => {
    let result = placements
        .iter()
        .filter(|(_, placed_node)| *placed_node == node_id)
        .map(|(name, _)| {
            let kind = match services::owner_of(name, placements, services) {
                Some(Owner::Service(service_name)) => {
                    let stateful = services.get(&service_name).map(|r| !r.template.volumes.is_empty()).unwrap_or(false);
                    if stateful { PlacementKind::StatefulService } else { PlacementKind::StatelessService }
                }
                _ => PlacementKind::PlainJail,
            };
            (name.to_string(), kind)
        })
        .collect();
    let _ = reply.send(result);
}
```

- [ ] **Step 3: Implement `handle_drain`**

```rust
fn handle_drain(node_id: &str, commands: &Sender<Command>, client_config: &Arc<rustls::ClientConfig>) -> (u16, Vec<u8>) {
    let statuses = {
        let (tx, rx) = mpsc::channel();
        if commands.send(Command::List(tx)).is_err() {
            return error_response(500, "control plane worker is not running".to_string());
        }
        rx.recv().unwrap_or_default()
    };
    let Some(node_status) = statuses.iter().find(|s| s.id == node_id) else {
        return error_response(404, format!("unknown node '{node_id}'"));
    };
    if node_status.status != NodeState::Alive {
        return error_response(409, format!("node '{node_id}' is not Alive; use force-repin per-replica instead of drain"));
    }
    if !node_status.ingresses.is_empty() {
        let hosts: Vec<&str> = node_status.ingresses.iter().map(|i| i.host.as_str()).collect();
        return error_response(409, format!(
            "node '{node_id}' has {} live Ingress host(s) that would be stranded by drain: {}. Move them first: keelctl apply -f <ingress>.yaml --node <other-node>, then delete the old one, then retry drain.",
            hosts.len(), hosts.join(", ")
        ));
    }

    // Implicit cordon so nothing new lands mid-drain.
    let (tx, rx) = mpsc::channel();
    if commands.send(Command::Cordon(node_id.to_string(), tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let _ = rx.recv();

    let placed = {
        let (tx, rx) = mpsc::channel();
        if commands.send(Command::NodePlacements(node_id.to_string(), tx)).is_err() {
            return error_response(500, "control plane worker is not running".to_string());
        }
        rx.recv().unwrap_or_default()
    };

    let mut failed = Vec::new();
    for (name, kind) in placed {
        let outcome = match kind {
            PlacementKind::StatefulService => drain_stateful_replica(&name, node_status, commands, client_config),
            PlacementKind::StatelessService => drain_stateless_replica(&name, node_status, commands, client_config),
            PlacementKind::PlainJail => drain_plain_jail(&name, node_status, commands, client_config),
        };
        if let Err(reason) = outcome {
            failed.push(format!("{name}: {reason}"));
        }
    }

    if failed.is_empty() {
        (200, Vec::new())
    } else {
        error_response(500, format!("drain of '{node_id}' partially failed: {}", failed.join("; ")))
    }
}
```

Three helpers, one per case from the design doc's Architecture section:

```rust
fn drain_stateless_replica(name: &str, node_status: &NodeStatus, commands: &Sender<Command>, client_config: &Arc<rustls::ClientConfig>) -> Result<(), String> {
    // Case 1: remove the placement (the next ReconcileServices pass, on
    // any subsequent heartbeat, sees it missing and schedules a
    // replacement on a schedulable node via is_schedulable/pick_node_for_service),
    // then delete the jail that's still physically running on this
    // still-Alive node -- unlike the Dead-node case, it won't tear itself
    // down on its own.
    let (tx, rx) = mpsc::channel();
    commands.send(Command::RemovePlacement(name.to_string(), tx)).map_err(|_| "worker not running".to_string())?;
    let _ = rx.recv();
    match forward(&node_status.addr, "DELETE", &format!("/jails/{name}"), &[], client_config) {
        Ok((status, _)) if (200..300).contains(&status) || status == 404 => Ok(()),
        Ok((status, body)) => Err(format!("delete on old node returned {status}: {}", String::from_utf8_lossy(&body))),
        Err(e) => Err(format!("failed to reach old node: {e}")),
    }
}

fn drain_stateful_replica(name: &str, node_status: &NodeStatus, commands: &Sender<Command>, client_config: &Arc<rustls::ClientConfig>) -> Result<(), String> {
    // Case 2: Task 5's PrepareDrainRepin, then the same promotion +
    // synchronous fence http.rs's handle_force_repin already does at
    // http.rs:269-312 -- reused as a shared function rather than
    // duplicated here (refactor handle_force_repin's body past its
    // PrepareForceRepin call into `execute_repin(prep, name, commands, client_config)`,
    // called by both handle_force_repin and this helper).
    let (tx, rx) = mpsc::channel();
    commands.send(Command::PrepareDrainRepin(name.to_string(), tx)).map_err(|_| "worker not running".to_string())?;
    match rx.recv() {
        Ok(Ok(prep)) => execute_repin(prep, name, commands, client_config).map_err(|(_, body)| String::from_utf8_lossy(&body).to_string()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("worker did not respond".to_string()),
    }
}

fn drain_plain_jail(name: &str, node_status: &NodeStatus, commands: &Sender<Command>, client_config: &Arc<rustls::ClientConfig>) -> Result<(), String> {
    // Case 3: the genuinely new mechanism -- fetch the spec from the old
    // node, place it on a fresh schedulable node, delete the old one only
    // once the new placement's PUT has actually succeeded.
    let spec_body = match forward(&node_status.addr, "GET", &format!("/jails/{name}"), &[], client_config) {
        Ok((status, body)) if (200..300).contains(&status) => body,
        Ok((status, body)) => return Err(format!("failed to fetch spec from old node: status {status}, {}", String::from_utf8_lossy(&body))),
        Err(e) => return Err(format!("failed to reach old node: {e}")),
    };

    let (tx, rx) = mpsc::channel();
    commands.send(Command::ResolveOrSchedule(name.to_string(), tx)).map_err(|_| "worker not running".to_string())?;
    // ResolveOrSchedule finds no existing placement only once RemovePlacement
    // below runs first -- so this helper removes the placement, then calls
    // ResolveOrSchedule to pick a fresh schedulable node (is_schedulable
    // already excludes this draining node), mirroring handle_scheduled_apply's
    // own shape (http.rs:176-212) rather than inventing a new scheduling path.
    let _ = rx.recv();

    let (rp_tx, rp_rx) = mpsc::channel();
    commands.send(Command::RemovePlacement(name.to_string(), rp_tx)).map_err(|_| "worker not running".to_string())?;
    let _ = rp_rx.recv();

    let (sched_tx, sched_rx) = mpsc::channel();
    commands.send(Command::ResolveOrSchedule(name.to_string(), sched_tx)).map_err(|_| "worker not running".to_string())?;
    let (new_node_id, new_addr) = match sched_rx.recv() {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return Err(e.to_string()),
        Err(_) => return Err("worker did not respond".to_string()),
    };

    match forward(&new_addr, "PUT", &format!("/jails/{name}"), &spec_body, client_config) {
        Ok((status, _)) if (200..300).contains(&status) => {
            send_record_placement(name, &new_node_id, commands);
        }
        Ok((status, body)) => return Err(format!("PUT to new node '{new_node_id}' returned {status}: {}", String::from_utf8_lossy(&body))),
        Err(e) => return Err(format!("failed to reach new node '{new_node_id}': {e}")),
    }

    match forward(&node_status.addr, "DELETE", &format!("/jails/{name}"), &[], client_config) {
        Ok((status, _)) if (200..300).contains(&status) || status == 404 => Ok(()),
        Ok((status, body)) => Err(format!("new placement succeeded but delete on old node returned {status}: {}", String::from_utf8_lossy(&body))),
        Err(e) => Err(format!("new placement succeeded but failed to reach old node to clean up: {e}")),
    }
}
```

Note the ordering in `drain_plain_jail`: `RemovePlacement` happens *before* `ResolveOrSchedule`, and the old node's `DELETE` happens *only after* the new node's `PUT` succeeds — this is what Error Handling's partial-failure requirement in the design doc actually depends on: a failed `PUT` leaves both the placement removed and the old jail still running, which is a real bookkeeping gap this task must not silently accept. Add a follow-up fix within this same task: on a `PUT` failure, restore the original placement via `RecordPlacement(name, node_status.id, ...)` before returning `Err`, so a failed migration attempt is not left with a torn-down `Placements` entry pointing nowhere.

- [ ] **Step 4: Add the route**

```rust
("POST", ["nodes", id, "drain"]) => handle_drain(id, commands, client_config),
```

- [ ] **Step 5: Verification**

```bash
cargo test -p keel-controlplane
cargo clippy -p keel-controlplane --all-targets -- -D warnings
```

---

### Task 7: `keelctl` — `drain` subcommand

**Files:**
- Modify: `keelctl/src/main.rs` (dispatch table; new `run_drain` alongside `run_cordon`/`run_uncordon`)

**Interfaces:**
- Produces: `keelctl drain <node>`.

- [ ] **Step 1: Write the failing test** (same shape as Task 4's `run_cordon` test, targeting `/nodes/<node>/drain`)

- [ ] **Step 2: Implement**

```rust
Some((cmd, rest)) if cmd == "drain" => run_drain(&target, rest),
```

```rust
fn run_drain(target: &Target, args: &[String]) -> Result<String, String> {
    let node = args.first().ok_or("drain requires a node id")?;
    let path = format!("/nodes/{node}/drain");
    success_body(dispatch(target, "POST", &path, "")).map(|_| String::new())
}
```

Update the usage string to include `drain NODE`.

- [ ] **Step 3: Verification**

```bash
cargo test -p keelctl
cargo clippy -p keelctl --all-targets -- -D warnings
```

---

### Task 8: `keel-dashboard` — surface cordoned state

**Files:**
- Modify: `keel-controlplane/src/wire.rs` (`NodeStatus` gains `#[serde(default)] pub cordoned: bool`, matching `ingresses`'s own rolling-deploy-compatibility comment at `wire.rs:56-65`)
- Modify: `keel-controlplane/src/worker.rs` (`Command::List`'s handler populates the new field from `cordoned`)
- Modify: `keel-dashboard/src/html.rs` (`render_nodes`, `html.rs:46-66`, gains a `Cordoned` column)

**Interfaces:**
- Produces: `NodeStatus.cordoned: bool`. No change needed to `keel-dashboard/src/snapshot.rs`'s `NodeSnapshot` — it wraps `NodeStatus` directly (`snapshot.rs:16-20`), so the new field flows through automatically once `NodeStatus` carries it.

- [ ] **Step 1: Write the failing tests**

```rust
// keel-controlplane/src/wire.rs
#[test]
fn node_status_without_a_cordoned_field_deserializes_with_default_false() {
    let yaml = "id: node-1\naddr: 192.168.64.4\npod_cidr: 10.0.4.0/24\nstatus: Alive\nlast_seen_secs: 3\ncapacity_cpu: 4\ncapacity_memory: 8589934592\ncommitted_cpu: 1.5\ncommitted_memory: 536870912\n";
    let parsed: NodeStatus = serde_yaml::from_str(yaml).unwrap();
    assert!(!parsed.cordoned);
}
```

```rust
// keel-controlplane/src/worker.rs
#[test]
fn list_reports_cordoned_true_for_a_cordoned_node() {
    let commands = spawn(/* ... */);
    // register node-1, cordon it
    let (tx, rx) = mpsc::channel();
    commands.send(Command::List(tx)).unwrap();
    let statuses = rx.recv().unwrap();
    assert!(statuses.iter().find(|s| s.id == "node-1").unwrap().cordoned);
}
```

- [ ] **Step 2: Implement**

`wire.rs`:

```rust
#[serde(default)]
pub cordoned: bool,
```

`worker.rs`'s `Command::List` handler:

```rust
Command::List(reply) => {
    let mut statuses = registry.list(Instant::now());
    for s in &mut statuses {
        s.cordoned = cordoned.is_cordoned(&s.id);
    }
    let _ = reply.send(statuses);
}
```

`html.rs`'s `render_nodes`:

```rust
rows.push_str(&format!(
    "<tr><td>{id}</td><td>{addr}</td><td>{status:?}</td><td>{cordoned}</td><td>{committed_cpu:.2}/{capacity_cpu:.2}</td>\
     <td>{committed_memory}/{capacity_memory}</td><td>{last_seen_secs}s</td>{stale}</tr>",
    id = escape_html(&s.id),
    addr = escape_html(&s.addr),
    status = s.status,
    cordoned = if s.status.cordoned { "cordoned" } else { "" },
    committed_cpu = s.committed_cpu,
    capacity_cpu = s.capacity_cpu,
    committed_memory = s.committed_memory,
    capacity_memory = s.capacity_memory,
    last_seen_secs = s.last_seen_secs,
    stale = if node.data_stale { "<td>stale</td>" } else { "<td></td>" },
));
```

(and add the matching `<th>Cordoned</th>` to the header row.)

- [ ] **Step 3: Verification**

```bash
cargo test -p keel-controlplane -p keel-dashboard
cargo clippy -p keel-controlplane -p keel-dashboard --all-targets -- -D warnings
```

---

### Task 9: `keel-spec`/`README.md` — no changes expected, confirm

**Files:** none expected to change.

This milestone adds no new `kind`, no new `Spec` field, and no new validation rule — confirm this holds by re-reading `keel-spec/src/types.rs`/`validate.rs` after Tasks 1-8 land and checking nothing there needed touching. If something did (e.g. a spec-level `unschedulable` marker was accidentally introduced instead of the control-plane-only `Cordoned` set this plan specifies), that is a signal the implementation drifted from the design doc's explicit "operator-set, not spec-set" architecture decision — stop and reconcile against the design doc rather than proceeding.

---

### Task 10: Full workspace verification and real 3-node VM verification

- [ ] `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check` all clean.
- [ ] Update `README.md`'s roadmap/milestone list per this project's standing convention (every prior milestone gets an entry once implemented and verified — follow the exact style of the Milestone 19-22 entries).
- [ ] **Real 3-node VM verification** (per this project's standing discipline of never assuming FreeBSD-specific behavior holds until proven on real hardware):
  1. Stand up a 3-node cluster running a mix of a plain `kind: Jail`, a stateless `Service`, and a stateful `Service` with its standby on a second node.
  2. `keelctl cordon` one node hosting all three kinds; confirm nothing already running is disturbed, and a fresh `apply` of a new replica lands only on the other two nodes.
  3. `keelctl uncordon` it; confirm it becomes schedulable again.
  4. `keelctl drain` the same node (re-cordoning it): confirm the plain Jail reappears running on another node with an identical spec; the stateless replica is recreated elsewhere within one heartbeat interval; the stateful replica's standby is promoted with its data intact and the old primary's jail is confirmed gone (`keelctl get <old-node>/jails` or direct SSH check).
  5. Apply a real Ingress with a real Let's Encrypt-staging cert to a node, attempt `keelctl drain` against it, and confirm it refuses naming that host rather than silently succeeding and leaving a dangling nginx jail with an orphaned cert behind.
  6. Perform the documented remediation (apply the Ingress fresh to another node, delete the original) and confirm `drain` then succeeds.
  7. Confirm `keelctl drain` against an already-`Dead` node (kill its `keel-agentd`) is refused with a message pointing at `force-repin`.

Per Fizz's finding in the channel discussion (2026-07-27T15:38): this step cannot currently be executed from an agent shell (the FreeBSD test VM's UTM vmnet interface is unreachable from this session — confirmed live, `ssh root@192.168.64.2` returns "No route to host"). This step needs either a copy-paste runbook for Corentin to run directly, or a session with real VM/SSH access, matching Milestone 1's own precedent for this exact limitation. Do not mark this milestone verified without it actually being run.
