# keel-controlplane State Persistence and Proactive Fencing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `keel-controlplane` real, on-disk persistence for `Placements`, `UsedAddresses`, `Standbys`, `PendingFences`, and `Services` (not `Registry`, which self-heals via periodic node re-registration), and make its fencing mechanism attempt an immediate push to a fenced node instead of waiting only for that node's own next heartbeat.

**Architecture:** A new generic `store.rs` (write-tmp-then-rename, mirroring `keel-agentd/src/store.rs`) persists each collection to its own YAML file under a new `--state-dir`. Nine `Command` branches in `worker.rs` persist after their existing in-memory mutation. `Registry` gains a `last_known_addr` accessor and `ForceRepinPrep` carries it through to `http.rs`, which attempts a direct `DELETE` to the old primary right when a fence is recorded.

**Tech Stack:** Rust, serde/serde_yaml (already a dependency), std::sync::mpsc (existing worker command-channel pattern), std::fs (write-tmp-then-rename).

## Global Constraints

- Full spec: `docs/superpowers/specs/2026-07-24-keel-controlplane-persistence-and-fencing-design.md`. Every task below implements a specific section of it; re-read the relevant section if a step's rationale is unclear.
- `Registry` is explicitly NOT persisted (see spec's Non-goals), do not add it in any task.
- `keelctl/rc.d/keel_seed_services` is left untouched (see spec's Non-goals), do not remove or modify it.
- Every mutating command's persist call logs on failure (`eprintln!`) and does not change the command's own success/failure result, a disk write failure must never turn an otherwise-successful in-memory mutation into an error response.
- Run `cargo test --workspace` after every task in this plan (not just the touched crate), this codebase's convention, and this plan's changes touch `keel-agentd` and `keelctl` call sites too (Task 2), not only `keel-controlplane`.
- No em dashes or en dashes in any code comment, doc comment, or commit message you write, use commas, parentheses, or a plain hyphen instead (project-wide writing convention already followed throughout this codebase's own comments).

---

### Task 1: `store.rs` generic persistence helpers

**Files:**
- Create: `keel-controlplane/src/store.rs`
- Modify: `keel-controlplane/src/lib.rs:1-12` (add `pub mod store;`)

**Interfaces:**
- Produces: `pub fn load_or_default<T: Default + serde::de::DeserializeOwned>(path: &Path) -> T` and `pub fn save<T: serde::Serialize>(path: &Path, value: &T) -> std::io::Result<()>`, every later task calls these two functions and nothing else from this module.

- [ ] **Step 1: Write the failing tests**

Create `keel-controlplane/src/store.rs`:

```rust
use std::fs;
use std::io;
use std::path::Path;

pub fn load_or_default<T: Default + serde::de::DeserializeOwned>(path: &Path) -> T {
    match fs::read_to_string(path) {
        Ok(content) => serde_yaml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse state file {}: {e}", path.display())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct Scratch {
        names: Vec<String>,
        count: u32,
    }

    fn test_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-controlplane-store-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        dir.join(format!("{name}.yaml"))
    }

    #[test]
    fn save_then_load_or_default_roundtrips() {
        let path = test_path("save_then_load_or_default_roundtrips");
        let value = Scratch { names: vec!["a".to_string(), "b".to_string()], count: 2 };
        save(&path, &value).unwrap();
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, value);
    }

    #[test]
    fn load_or_default_on_a_missing_file_returns_default() {
        let path = test_path("load_or_default_on_a_missing_file_returns_default");
        let _ = fs::remove_file(&path);
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, Scratch::default());
    }

    #[test]
    fn save_creates_the_parent_directory_if_missing() {
        let dir = std::env::temp_dir().join(format!("keel-controlplane-store-test-missing-parent-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("scratch.yaml");
        let value = Scratch { names: vec![], count: 0 };
        save(&path, &value).unwrap();
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, value);
    }

    #[test]
    fn save_overwrites_a_previous_value_rather_than_merging() {
        let path = test_path("save_overwrites_a_previous_value_rather_than_merging");
        save(&path, &Scratch { names: vec!["old".to_string()], count: 1 }).unwrap();
        let new_value = Scratch { names: vec!["new".to_string()], count: 2 };
        save(&path, &new_value).unwrap();
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, new_value);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail (compile check first)**

Run: `cargo test -p keel-controlplane store:: 2>&1 | tail -30`
Expected: compiles and passes immediately since the implementation above is already written in the same step. This module has no separate red phase distinct from step 1 (the function bodies are trivial and written test-first as one unit, matching how `keel-agentd/src/store.rs` itself was originally built), instead, verify by temporarily commenting out the body of `save` (replace with `unimplemented!()`) and confirming `save_then_load_or_default_roundtrips` panics, then restore it. This is the fastest correct way to prove the test actually exercises the function.

- [ ] **Step 3: Wire the module into `lib.rs`**

In `keel-controlplane/src/lib.rs`, add one line after the existing `pub mod services;`:

```rust
pub mod services;
pub mod standbys;
pub mod store;
pub mod subnet;
```

(Keep the existing alphabetical-ish ordering: `store` goes between `services`/`standbys` and `subnet`.)

- [ ] **Step 4: Run the full test suite**

Run: `cargo test -p keel-controlplane 2>&1 | tail -30`
Expected: all existing tests still pass, plus the 4 new `store::tests::*` tests.

- [ ] **Step 5: Commit**

```bash
git add keel-controlplane/src/store.rs keel-controlplane/src/lib.rs
git commit -m "feat(keel-controlplane): add generic state persistence helpers"
```

---

### Task 2: Thread `state_dir` through the worker and persist `Placements`

This is the task that touches every `worker::spawn` call site in the workspace (there are 42: 36 in `keel-controlplane/src/worker.rs`'s own tests, 4 in `keel-controlplane/src/http.rs`'s tests, 1 in `keel-agentd/src/registration.rs`'s tests, 1 in `keelctl/tests/cli.rs`). Every one of them currently ends in the exact literal text `Standbys::new(), PendingFences::new())` (or, in the three files outside `worker.rs`, the multi-line block ending `PendingFences::new(),\n    );`), which is what makes a single mechanical fix possible instead of editing 42 call sites by hand.

**Files:**
- Modify: `keel-controlplane/src/placements.rs` (derive `Serialize`/`Deserialize`)
- Modify: `keel-controlplane/src/worker.rs` (add `state_dir` param to `spawn`/`handle_command`, persist `Placements` on `RecordPlacement`/`RemovePlacement`, update 36 test call sites)
- Modify: `keel-controlplane/src/http.rs` (update 4 test call sites)
- Modify: `keel-controlplane/src/main.rs` (pass a `state_dir` to `worker::spawn`, gated behind a placeholder for now, a hardcoded temp path; Task 7 replaces this with the real `--state-dir` CLI flag)
- Modify: `keel-agentd/src/registration.rs` (update 1 test call site)
- Modify: `keelctl/tests/cli.rs` (update 1 test call site)

**Interfaces:**
- Consumes: `store::load_or_default`/`store::save` from Task 1.
- Produces: `worker::spawn(registry, placements, services, used_addresses, standbys, pending_fences, state_dir: PathBuf)`, every later task's `spawn()` call sites already exist from this task; Tasks 3-6 only add more persist calls inside `handle_command`, they don't touch `spawn`'s signature again.

- [ ] **Step 1: Derive `Serialize`/`Deserialize` on `Placements`**

In `keel-controlplane/src/placements.rs`, change:

```rust
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Placements {
    by_jail: HashMap<String, String>,
}
```

to:

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Placements {
    by_jail: HashMap<String, String>,
}
```

- [ ] **Step 2: Write the failing round-trip test for `Placements`**

Add to `keel-controlplane/src/placements.rs`'s existing `mod tests`:

```rust
    #[test]
    fn placements_round_trips_through_yaml() {
        let mut placements = Placements::new();
        placements.set("web-0".to_string(), "node-1".to_string());
        placements.set("web-1".to_string(), "node-2".to_string());
        let path = std::env::temp_dir().join(format!("keel-controlplane-placements-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &placements).unwrap();
        let loaded: Placements = crate::store::load_or_default(&path);
        assert_eq!(loaded.get("web-0"), Some("node-1"));
        assert_eq!(loaded.get("web-1"), Some("node-2"));
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p keel-controlplane placements_round_trips_through_yaml 2>&1 | tail -20`
Expected: compiles and passes already (the derive from Step 1 is enough), there is no meaningful red phase for a derive-only change. Confirm instead by temporarily removing `Serialize, Deserialize` from the derive list and re-running: expect a compile error (`Placements` doesn't implement `Serialize`), then restore the derive.

- [ ] **Step 4: Thread `state_dir` through `worker::spawn`/`handle_command`**

In `keel-controlplane/src/worker.rs`, add near the top (after the existing `use` block):

```rust
use std::path::{Path, PathBuf};
```

Change:

```rust
pub fn spawn(
    mut registry: Registry,
    mut placements: Placements,
    mut services: Services,
    mut used_addresses: UsedAddresses,
    mut standbys: Standbys,
    mut pending_fences: PendingFences,
) -> (JoinHandle<()>, Sender<Command>) {
    let (tx, rx) = mpsc::channel::<Command>();
    let handle = thread::spawn(move || {
        for command in rx {
            handle_command(&mut registry, &mut placements, &mut services, &mut used_addresses, &mut standbys, &mut pending_fences, command);
        }
    });
    (handle, tx)
}

fn handle_command(
    registry: &mut Registry,
    placements: &mut Placements,
    services: &mut Services,
    used_addresses: &mut UsedAddresses,
    standbys: &mut Standbys,
    pending_fences: &mut PendingFences,
    command: Command,
) {
```

to:

```rust
pub fn spawn(
    mut registry: Registry,
    mut placements: Placements,
    mut services: Services,
    mut used_addresses: UsedAddresses,
    mut standbys: Standbys,
    mut pending_fences: PendingFences,
    state_dir: PathBuf,
) -> (JoinHandle<()>, Sender<Command>) {
    let (tx, rx) = mpsc::channel::<Command>();
    let handle = thread::spawn(move || {
        for command in rx {
            handle_command(&mut registry, &mut placements, &mut services, &mut used_addresses, &mut standbys, &mut pending_fences, &state_dir, command);
        }
    });
    (handle, tx)
}

fn persist_placements(placements: &Placements, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("placements.yaml"), placements) {
        eprintln!("keel-controlplane: failed to persist placements.yaml: {e}");
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_command(
    registry: &mut Registry,
    placements: &mut Placements,
    services: &mut Services,
    used_addresses: &mut UsedAddresses,
    standbys: &mut Standbys,
    pending_fences: &mut PendingFences,
    state_dir: &Path,
    command: Command,
) {
```

- [ ] **Step 5: Persist on `RecordPlacement`/`RemovePlacement`**

Change:

```rust
        Command::RecordPlacement(jail_name, node_id, reply) => {
            placements.set(jail_name, node_id);
            let _ = reply.send(());
        }
        Command::RemovePlacement(jail_name, reply) => {
            placements.remove(&jail_name);
            let _ = reply.send(());
        }
```

to:

```rust
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
```

- [ ] **Step 6: Add a unique-state-dir test helper and fix all 36 call sites in `worker.rs`**

In `keel-controlplane/src/worker.rs`'s `mod tests`, add near the top (right after the existing `fn test_service_cidr()`):

```rust
    fn fresh_state_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("keel-controlplane-worker-test-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }
```

Then run this exact find-and-replace across the whole file (every one of the 36 occurrences is character-for-character identical, confirmed via `grep -c "spawn(Registry::new(test_cluster_cidr())" keel-controlplane/src/worker.rs` returning 36):

Find: `UsedAddresses::new(), Standbys::new(), PendingFences::new())`
Replace: `UsedAddresses::new(), Standbys::new(), PendingFences::new(), fresh_state_dir())`

- [ ] **Step 7: Fix the 4 call sites in `http.rs`**

In `keel-controlplane/src/http.rs`'s `mod tests`, add the same helper (private copy, test modules are per-file in Rust, this one isn't reachable from `worker.rs`'s):

```rust
    fn fresh_state_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("keel-controlplane-http-test-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }
```

Then find-and-replace all 4 occurrences of this exact multi-line block:

Find:
```
            crate::pending_fences::PendingFences::new(),
        );
```

Replace:
```
            crate::pending_fences::PendingFences::new(),
            fresh_state_dir(),
        );
```

- [ ] **Step 8: Fix the 1 call site in `keel-agentd/src/registration.rs`**

In `keel-agentd/src/registration.rs`'s `mod tests`, add (this file's tests already use `keel_controlplane::worker::spawn`, per its own `use keel_controlplane::worker;` import):

```rust
    fn fresh_controlplane_state_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("keel-agentd-registration-test-controlplane-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }
```

Then change:

```rust
        let (_worker_handle, commands) = worker::spawn(
            Registry::new("10.0.0.0/16".parse().unwrap()),
            Placements::new(),
            keel_controlplane::Services::new("10.0.250.0/24".parse().unwrap()),
            keel_controlplane::addresses::UsedAddresses::new(),
            keel_controlplane::Standbys::new(),
            keel_controlplane::PendingFences::new(),
        );
```

to:

```rust
        let (_worker_handle, commands) = worker::spawn(
            Registry::new("10.0.0.0/16".parse().unwrap()),
            Placements::new(),
            keel_controlplane::Services::new("10.0.250.0/24".parse().unwrap()),
            keel_controlplane::addresses::UsedAddresses::new(),
            keel_controlplane::Standbys::new(),
            keel_controlplane::PendingFences::new(),
            fresh_controlplane_state_dir(),
        );
```

- [ ] **Step 9: Fix the 1 call site in `keelctl/tests/cli.rs`**

Add near the top of `keelctl/tests/cli.rs` (this is an integration test file, not a `mod tests` inside `src/`, so the helper goes at file scope):

```rust
fn fresh_controlplane_state_dir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("keelctl-test-controlplane-{}-{id}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
```

Then change:

```rust
    let (_worker_handle, commands) = keel_controlplane::worker::spawn(
        keel_controlplane::Registry::new("10.0.0.0/16".parse().unwrap()),
        keel_controlplane::Placements::new(),
        keel_controlplane::Services::new("10.0.250.0/24".parse().unwrap()),
        keel_controlplane::addresses::UsedAddresses::new(),
        keel_controlplane::Standbys::new(),
        keel_controlplane::PendingFences::new(),
    );
```

to:

```rust
    let (_worker_handle, commands) = keel_controlplane::worker::spawn(
        keel_controlplane::Registry::new("10.0.0.0/16".parse().unwrap()),
        keel_controlplane::Placements::new(),
        keel_controlplane::Services::new("10.0.250.0/24".parse().unwrap()),
        keel_controlplane::addresses::UsedAddresses::new(),
        keel_controlplane::Standbys::new(),
        keel_controlplane::PendingFences::new(),
        fresh_controlplane_state_dir(),
    );
```

- [ ] **Step 10: Fix the one production call site in `main.rs` (temporary, Task 7 replaces this)**

In `keel-controlplane/src/main.rs`, change:

```rust
    let (_worker_handle, commands) = worker::spawn(
        Registry::new(cluster_cidr),
        Placements::new(),
        keel_controlplane::Services::new(service_cidr),
        keel_controlplane::addresses::UsedAddresses::new(),
        keel_controlplane::Standbys::new(),
        keel_controlplane::PendingFences::new(),
    );
```

to:

```rust
    let state_dir = std::path::PathBuf::from("/var/db/keel-controlplane");
    let (_worker_handle, commands) = worker::spawn(
        Registry::new(cluster_cidr),
        Placements::new(),
        keel_controlplane::Services::new(service_cidr),
        keel_controlplane::addresses::UsedAddresses::new(),
        keel_controlplane::Standbys::new(),
        keel_controlplane::PendingFences::new(),
        state_dir,
    );
```

- [ ] **Step 11: Run the full workspace test suite**

Run: `cargo test --workspace 2>&1 | tail -60`
Expected: everything compiles and every test passes, including the new `placements_round_trips_through_yaml`.

- [ ] **Step 12: Commit**

```bash
git add keel-controlplane/src/placements.rs keel-controlplane/src/worker.rs keel-controlplane/src/http.rs keel-controlplane/src/main.rs keel-agentd/src/registration.rs keelctl/tests/cli.rs
git commit -m "feat(keel-controlplane): thread state_dir through the worker, persist Placements"
```

---

### Task 3: Persist `UsedAddresses`

**Files:**
- Modify: `keel-controlplane/src/addresses.rs` (derive `Serialize`/`Deserialize`)
- Modify: `keel-controlplane/src/worker.rs` (persist on `RecordReplicaAddress`/`ReleaseReplicaAddress`)

**Interfaces:**
- Consumes: `store::save`/`store::load_or_default` (Task 1), `state_dir: &Path` already threaded into `handle_command` (Task 2).

- [ ] **Step 1: Derive `Serialize`/`Deserialize` on `UsedAddresses`**

In `keel-controlplane/src/addresses.rs`, change:

```rust
use ipnet::Ipv4Net;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

/// Which addresses are currently assigned to a replica, per node -- lives
/// next to `Placements`/`Services`: no persistence, forgotten on restart,
/// populated when a replica is scheduled and freed when it's torn down.
#[derive(Debug, Default, Clone)]
pub struct UsedAddresses {
    used_by_node: HashMap<String, HashSet<Ipv4Addr>>,
    by_jail: HashMap<String, (String, Ipv4Addr)>,
}
```

to:

```rust
use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

/// Which addresses are currently assigned to a replica, per node -- lives
/// next to `Placements`/`Services`, persisted the same way (see
/// `keel-controlplane/src/store.rs`), populated when a replica is scheduled
/// and freed when it's torn down.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UsedAddresses {
    used_by_node: HashMap<String, HashSet<Ipv4Addr>>,
    by_jail: HashMap<String, (String, Ipv4Addr)>,
}
```

- [ ] **Step 2: Write the failing round-trip test**

Add to `keel-controlplane/src/addresses.rs`'s existing `mod tests`:

```rust
    #[test]
    fn used_addresses_round_trips_through_yaml() {
        let mut used = UsedAddresses::new();
        used.record("web-0".to_string(), "node-1".to_string(), addr("10.0.60.2"));
        let path = std::env::temp_dir().join(format!("keel-controlplane-used-addresses-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &used).unwrap();
        let loaded: UsedAddresses = crate::store::load_or_default(&path);
        assert_eq!(loaded.address_of("web-0"), Some(addr("10.0.60.2")));
        assert!(loaded.is_used("node-1", addr("10.0.60.2")));
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p keel-controlplane used_addresses_round_trips_through_yaml 2>&1 | tail -20`
Expected: fails to compile (`UsedAddresses` doesn't implement `Serialize`) if Step 1 hasn't landed yet in your working tree; since Step 1 and Step 2 are both above, verify red by temporarily removing `Serialize, Deserialize` from the derive list, confirming the compile error, then restoring it.

- [ ] **Step 4: Persist on `RecordReplicaAddress`/`ReleaseReplicaAddress`**

In `keel-controlplane/src/worker.rs`, add next to `persist_placements`:

```rust
fn persist_used_addresses(used_addresses: &UsedAddresses, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("used_addresses.yaml"), used_addresses) {
        eprintln!("keel-controlplane: failed to persist used_addresses.yaml: {e}");
    }
}
```

Change:

```rust
        Command::RecordReplicaAddress(jail_name, node_id, address, reply) => {
            used_addresses.record(jail_name, node_id, address);
            let _ = reply.send(());
        }
        Command::ReleaseReplicaAddress(jail_name, reply) => {
            used_addresses.release(&jail_name);
            let _ = reply.send(());
        }
```

to:

```rust
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
```

- [ ] **Step 5: Run the full test suite**

Run: `cargo test -p keel-controlplane 2>&1 | tail -30`
Expected: all pass, including the new round-trip test.

- [ ] **Step 6: Commit**

```bash
git add keel-controlplane/src/addresses.rs keel-controlplane/src/worker.rs
git commit -m "feat(keel-controlplane): persist UsedAddresses"
```

---

### Task 4: Persist `Standbys`

**Files:**
- Modify: `keel-controlplane/src/standbys.rs` (derive `Serialize`/`Deserialize`)
- Modify: `keel-controlplane/src/worker.rs` (persist on `RecordStandby`)

- [ ] **Step 1: Derive `Serialize`/`Deserialize` on `Standbys`**

In `keel-controlplane/src/standbys.rs`, change:

```rust
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Standbys {
    by_replica: HashMap<String, String>,
}
```

to:

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Standbys {
    by_replica: HashMap<String, String>,
}
```

- [ ] **Step 2: Write the failing round-trip test**

Add to `keel-controlplane/src/standbys.rs`'s existing `mod tests`:

```rust
    #[test]
    fn standbys_round_trips_through_yaml() {
        let mut standbys = Standbys::new();
        standbys.set("db-0".to_string(), "node-2".to_string());
        let path = std::env::temp_dir().join(format!("keel-controlplane-standbys-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &standbys).unwrap();
        let loaded: Standbys = crate::store::load_or_default(&path);
        assert_eq!(loaded.get("db-0"), Some("node-2"));
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p keel-controlplane standbys_round_trips_through_yaml 2>&1 | tail -20`
Expected: same as Task 3's Step 3, verify red by temporarily removing the derive, confirm compile error, restore.

- [ ] **Step 4: Persist on `RecordStandby`**

In `keel-controlplane/src/worker.rs`, add next to `persist_used_addresses`:

```rust
fn persist_standbys(standbys: &Standbys, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("standbys.yaml"), standbys) {
        eprintln!("keel-controlplane: failed to persist standbys.yaml: {e}");
    }
}
```

Change:

```rust
        Command::RecordStandby(replica_name, node_id, reply) => {
            standbys.set(replica_name, node_id);
            let _ = reply.send(());
        }
```

to:

```rust
        Command::RecordStandby(replica_name, node_id, reply) => {
            standbys.set(replica_name, node_id);
            persist_standbys(standbys, state_dir);
            let _ = reply.send(());
        }
```

- [ ] **Step 5: Run the full test suite**

Run: `cargo test -p keel-controlplane 2>&1 | tail -30`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add keel-controlplane/src/standbys.rs keel-controlplane/src/worker.rs
git commit -m "feat(keel-controlplane): persist Standbys"
```

---

### Task 5: Persist `PendingFences`

**Files:**
- Modify: `keel-controlplane/src/pending_fences.rs` (derive `Serialize`/`Deserialize`)
- Modify: `keel-controlplane/src/worker.rs` (persist on `RecordPendingFence`/`RemovePendingFence`)

- [ ] **Step 1: Derive `Serialize`/`Deserialize` on `PendingFences`**

In `keel-controlplane/src/pending_fences.rs`, change:

```rust
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct PendingFences {
    by_replica: HashMap<String, String>, // replica_name -> node_id owed a forced delete
}
```

to:

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PendingFences {
    by_replica: HashMap<String, String>, // replica_name -> node_id owed a forced delete
}
```

- [ ] **Step 2: Write the failing round-trip test**

Add to `keel-controlplane/src/pending_fences.rs`'s existing `mod tests`:

```rust
    #[test]
    fn pending_fences_round_trips_through_yaml() {
        let mut fences = PendingFences::new();
        fences.set("db-0".to_string(), "node-1".to_string());
        let path = std::env::temp_dir().join(format!("keel-controlplane-pending-fences-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &fences).unwrap();
        let loaded: PendingFences = crate::store::load_or_default(&path);
        assert_eq!(loaded.for_node("node-1"), vec!["db-0".to_string()]);
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p keel-controlplane pending_fences_round_trips_through_yaml 2>&1 | tail -20`
Expected: same pattern as previous tasks, verify red via temporary derive removal, then restore.

- [ ] **Step 4: Persist on `RecordPendingFence`/`RemovePendingFence`**

In `keel-controlplane/src/worker.rs`, add next to `persist_standbys`:

```rust
fn persist_pending_fences(pending_fences: &PendingFences, state_dir: &Path) {
    if let Err(e) = crate::store::save(&state_dir.join("pending_fences.yaml"), pending_fences) {
        eprintln!("keel-controlplane: failed to persist pending_fences.yaml: {e}");
    }
}
```

Change:

```rust
        Command::RecordPendingFence(replica_name, node_id, reply) => {
            pending_fences.set(replica_name, node_id);
            let _ = reply.send(());
        }
        Command::PendingFencesForNode(node_id, reply) => {
            let _ = reply.send(pending_fences.for_node(&node_id));
        }
        Command::RemovePendingFence(replica_name, reply) => {
            pending_fences.remove(&replica_name);
            let _ = reply.send(());
        }
```

to:

```rust
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
```

- [ ] **Step 5: Run the full test suite**

Run: `cargo test -p keel-controlplane 2>&1 | tail -30`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add keel-controlplane/src/pending_fences.rs keel-controlplane/src/worker.rs
git commit -m "feat(keel-controlplane): persist PendingFences"
```

---

### Task 6: `Services` persistence

`Services` needs a slightly different treatment than the previous four: it holds `service_cidr` (from a CLI flag) alongside the actual data, and `service_cidr` must never be persisted (see spec's Architecture section for why).

**Files:**
- Modify: `keel-controlplane/src/services.rs` (add `ServicesState`, `Services::load`/`persist`)
- Modify: `keel-controlplane/src/worker.rs` (persist on `ApplyService`/`DeleteService` success)

**Interfaces:**
- Produces: `Services::load(state_dir: &Path, service_cidr: Ipv4Net) -> Services` and `Services::persist(&self, state_dir: &Path)`, Task 7 calls `Services::load` from `main.rs`.

- [ ] **Step 1: Write the failing test proving `service_cidr` comes from the argument, not disk**

Add to `keel-controlplane/src/services.rs`'s existing `mod tests`:

```rust
    #[test]
    fn load_uses_the_passed_in_service_cidr_not_whatever_was_persisted_last() {
        let dir = std::env::temp_dir().join(format!("keel-controlplane-services-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let mut original = Services::new("10.0.250.0/24".parse().unwrap());
        original.apply("web".to_string(), 1, template(), 8080).unwrap();
        original.persist(&dir);

        // A restart with a *different* --service-cidr flag: the loaded
        // Services must use the new cidr, not silently keep serving the old
        // one from disk.
        let reloaded = Services::load(&dir, "10.0.251.0/24".parse().unwrap());
        assert_eq!(reloaded.get("web").unwrap().desired_replicas, 1, "the persisted service itself must still be there");

        let mut fresh_apply = reloaded;
        // A fresh, never-before-seen service name must get a VIP inside the
        // *new* cidr, proving service_cidr truly came from the argument.
        fresh_apply.apply("api".to_string(), 1, template(), 8080).unwrap();
        let vip = fresh_apply.get("api").unwrap().vip;
        assert!(
            "10.0.251.0/24".parse::<Ipv4Net>().unwrap().contains(&vip),
            "expected the new service's VIP {vip} inside the newly-passed-in service_cidr, not the persisted one"
        );
    }

    #[test]
    fn load_on_a_missing_state_dir_returns_an_empty_services() {
        let dir = std::env::temp_dir().join(format!("keel-controlplane-services-test-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let services = Services::load(&dir, test_service_cidr());
        assert_eq!(services.list(), vec![]);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p keel-controlplane load_uses_the_passed_in_service_cidr load_on_a_missing_state_dir 2>&1 | tail -30`
Expected: FAIL with "no method named `load`/`persist` found for struct `Services`".

- [ ] **Step 3: Implement `ServicesState`/`Services::load`/`Services::persist`**

In `keel-controlplane/src/services.rs`, add near the top:

```rust
use serde::{Deserialize, Serialize};
```

Add right after the `Services` struct's existing `impl` block starts (before the existing `pub fn new`):

```rust
#[derive(Default, Serialize, Deserialize)]
struct ServicesState {
    by_name: HashMap<String, ServiceRecord>,
}
```

`ServiceRecord` needs `Serialize`/`Deserialize` too since `ServicesState` embeds it directly. Change:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceRecord {
```

to:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRecord {
```

Then add these two methods to `impl Services`, right after `pub fn new`:

```rust
    /// Loads persisted service definitions from `state_dir`, pairing them
    /// with `service_cidr` from the current process's own config -- never
    /// from disk, so a changed `--service-cidr` flag across a restart can
    /// never be silently overridden by stale persisted data.
    pub fn load(state_dir: &std::path::Path, service_cidr: Ipv4Net) -> Self {
        let state: ServicesState = crate::store::load_or_default(&state_dir.join("services.yaml"));
        Self { service_cidr, by_name: state.by_name }
    }

    pub fn persist(&self, state_dir: &std::path::Path) {
        let state = ServicesState { by_name: self.by_name.clone() };
        if let Err(e) = crate::store::save(&state_dir.join("services.yaml"), &state) {
            eprintln!("keel-controlplane: failed to persist services.yaml: {e}");
        }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p keel-controlplane load_uses_the_passed_in_service_cidr load_on_a_missing_state_dir 2>&1 | tail -30`
Expected: PASS.

- [ ] **Step 5: Persist on `ApplyService`/`DeleteService` success**

In `keel-controlplane/src/worker.rs`, change:

```rust
        Command::ApplyService(name, replicas, template, port, reply) => {
            let result = (|| {
                for i in 0..replicas {
                    let candidate = services::replica_name(&name, i);
                    if let Some(owner) = services::owner_of(&candidate, placements, services) {
                        let is_self = matches!(&owner, Owner::Service(other) if other == &name);
                        if !is_self {
                            return Err(services::ApplyServiceError::NameConflict { name: candidate, owner });
                        }
                    }
                }
                services.apply(name, replicas, template, port)
            })();
            let _ = reply.send(result);
        }
```

to:

```rust
        Command::ApplyService(name, replicas, template, port, reply) => {
            let result = (|| {
                for i in 0..replicas {
                    let candidate = services::replica_name(&name, i);
                    if let Some(owner) = services::owner_of(&candidate, placements, services) {
                        let is_self = matches!(&owner, Owner::Service(other) if other == &name);
                        if !is_self {
                            return Err(services::ApplyServiceError::NameConflict { name: candidate, owner });
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
```

And change:

```rust
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
                        Some(ReplicaAction::TearDown { replica_name: jail_name.to_string(), node_id: node_id.to_string(), node_addr })
                    })
                    .collect();
                services.remove(&name);
                Ok(actions)
            };
            let _ = reply.send(result);
        }
```

to:

```rust
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
                        Some(ReplicaAction::TearDown { replica_name: jail_name.to_string(), node_id: node_id.to_string(), node_addr })
                    })
                    .collect();
                services.remove(&name);
                services.persist(state_dir);
                Ok(actions)
            };
            let _ = reply.send(result);
        }
```

- [ ] **Step 6: Run the full test suite**

Run: `cargo test --workspace 2>&1 | tail -60`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add keel-controlplane/src/services.rs keel-controlplane/src/worker.rs
git commit -m "feat(keel-controlplane): persist Services with cidr from config, not disk"
```

---

### Task 7: Wire real startup loading, `--state-dir` CLI flag, and rc.d

**Files:**
- Modify: `keel-controlplane/src/main.rs` (add `--state-dir` flag, load all 5 collections at startup)
- Modify: `keel-controlplane/rc.d/keel_controlplane` (add `keel_controlplane_state_dir` variable)

**Interfaces:**
- Consumes: `store::load_or_default` (Task 1), `Services::load` (Task 6), `worker::spawn`'s `state_dir` parameter (Task 2).

- [ ] **Step 1: Write the failing test for the new CLI flag**

Add to `keel-controlplane/src/main.rs`'s existing `mod tests`:

```rust
    #[test]
    fn parses_the_state_dir_flag() {
        let config = parse_args_from(args(&[
            "--cluster-cidr", "10.0.0.0/16",
            "--service-cidr", "10.0.250.0/24",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/controlplane.crt",
            "--tls-key-file", "/etc/keel/controlplane.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
            "--state-dir", "/custom/state/dir",
        ]));
        assert_eq!(config.state_dir, PathBuf::from("/custom/state/dir"));
    }

    #[test]
    fn state_dir_defaults_when_not_given() {
        let config = parse_args_from(args(&[
            "--cluster-cidr", "10.0.0.0/16",
            "--service-cidr", "10.0.250.0/24",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/controlplane.crt",
            "--tls-key-file", "/etc/keel/controlplane.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
        ]));
        assert_eq!(config.state_dir, PathBuf::from("/var/db/keel-controlplane"));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p keel-controlplane --bin keel-controlplane parses_the_state_dir_flag state_dir_defaults_when_not_given 2>&1 | tail -30`
Expected: FAIL with "no field `state_dir` on type `Config`".

- [ ] **Step 3: Add the flag to `Config`/`parse_args_from`**

In `keel-controlplane/src/main.rs`, change:

```rust
struct Config {
    addr: String,
    cluster_cidr: Option<Ipv4Net>,
    service_cidr: Option<Ipv4Net>,
    tls_ca_file: Option<PathBuf>,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    tls_crl_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:7620".to_string(),
            cluster_cidr: None,
            service_cidr: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_crl_file: None,
        }
    }
}
```

to:

```rust
struct Config {
    addr: String,
    cluster_cidr: Option<Ipv4Net>,
    service_cidr: Option<Ipv4Net>,
    tls_ca_file: Option<PathBuf>,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    tls_crl_file: Option<PathBuf>,
    state_dir: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:7620".to_string(),
            cluster_cidr: None,
            service_cidr: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_crl_file: None,
            state_dir: PathBuf::from("/var/db/keel-controlplane"),
        }
    }
}
```

And add one match arm in `parse_args_from`, right after the existing `"--tls-crl-file"` arm:

```rust
            "--tls-crl-file" => config.tls_crl_file = Some(PathBuf::from(value)),
            "--state-dir" => config.state_dir = PathBuf::from(value),
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p keel-controlplane --bin keel-controlplane parses_the_state_dir_flag state_dir_defaults_when_not_given 2>&1 | tail -30`
Expected: PASS.

- [ ] **Step 5: Load all five collections at startup in `main()`**

Change:

```rust
    eprintln!("keel-controlplane: starting (addr={})", config.addr);

    let (_worker_handle, commands) = worker::spawn(
        Registry::new(cluster_cidr),
        Placements::new(),
        keel_controlplane::Services::new(service_cidr),
        keel_controlplane::addresses::UsedAddresses::new(),
        keel_controlplane::Standbys::new(),
        keel_controlplane::PendingFences::new(),
    );
```

to:

```rust
    eprintln!(
        "keel-controlplane: starting (addr={}, state_dir={})",
        config.addr,
        config.state_dir.display()
    );

    let placements: Placements =
        keel_controlplane::store::load_or_default(&config.state_dir.join("placements.yaml"));
    let used_addresses: keel_controlplane::addresses::UsedAddresses =
        keel_controlplane::store::load_or_default(&config.state_dir.join("used_addresses.yaml"));
    let standbys: keel_controlplane::Standbys =
        keel_controlplane::store::load_or_default(&config.state_dir.join("standbys.yaml"));
    let pending_fences: keel_controlplane::PendingFences =
        keel_controlplane::store::load_or_default(&config.state_dir.join("pending_fences.yaml"));
    let services = keel_controlplane::Services::load(&config.state_dir, service_cidr);

    let (_worker_handle, commands) = worker::spawn(
        Registry::new(cluster_cidr),
        placements,
        services,
        used_addresses,
        standbys,
        pending_fences,
        config.state_dir.clone(),
    );
```

(Remove the placeholder line `let state_dir = std::path::PathBuf::from("/var/db/keel-controlplane");` that Task 2 Step 10 added, it's superseded by `config.state_dir` now.)

- [ ] **Step 6: Run the full test suite**

Run: `cargo test --workspace 2>&1 | tail -60`
Expected: all pass.

- [ ] **Step 7: Add the rc.d variable**

In `keel-controlplane/rc.d/keel_controlplane`, change:

```sh
: ${keel_controlplane_tls_crl_file:=""}

pidfile="/var/run/${name}.pid"
command="/usr/sbin/daemon"
command_args="-r -P ${pidfile} -S -T ${name} -- \
  ${keel_controlplane_bin} --addr ${keel_controlplane_addr} \
  --cluster-cidr ${keel_controlplane_cluster_cidr} \
  --service-cidr ${keel_controlplane_service_cidr} \
  --tls-ca-file ${keel_controlplane_tls_ca_file} \
  --tls-cert-file ${keel_controlplane_tls_cert_file} \
  --tls-key-file ${keel_controlplane_tls_key_file} \
  --tls-crl-file ${keel_controlplane_tls_crl_file}"
```

to:

```sh
: ${keel_controlplane_tls_crl_file:=""}
: ${keel_controlplane_state_dir:=""}

pidfile="/var/run/${name}.pid"
command="/usr/sbin/daemon"

# state_dir is optional (the binary defaults to /var/db/keel-controlplane on
# its own), so it's only passed when the operator has actually set it -
# named controlplane_extra_flags, not the generic "flags", since that name
# collides with rc.subr's own reserved ${name}_flags convention (see
# keel-dashboard's rc.d script for where this bug class was first found).
controlplane_extra_flags=""
[ -n "$keel_controlplane_state_dir" ] && controlplane_extra_flags="--state-dir $keel_controlplane_state_dir"

command_args="-r -P ${pidfile} -S -T ${name} -- \
  ${keel_controlplane_bin} --addr ${keel_controlplane_addr} \
  --cluster-cidr ${keel_controlplane_cluster_cidr} \
  --service-cidr ${keel_controlplane_service_cidr} \
  --tls-ca-file ${keel_controlplane_tls_ca_file} \
  --tls-cert-file ${keel_controlplane_tls_cert_file} \
  --tls-key-file ${keel_controlplane_tls_key_file} \
  --tls-crl-file ${keel_controlplane_tls_crl_file} \
  ${controlplane_extra_flags}"
```

- [ ] **Step 8: Commit**

```bash
git add keel-controlplane/src/main.rs keel-controlplane/rc.d/keel_controlplane
git commit -m "feat(keel-controlplane): add --state-dir flag, load persisted state at startup"
```

---

### Task 8: Integration test proving restart no longer duplicates a placed replica

This is the test proving the actual bug (not just that persistence code exists in isolation).

**Files:**
- Modify: `keel-controlplane/src/worker.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-6 (`Services`/`Placements`/`UsedAddresses` persistence, `worker::spawn`'s `state_dir` parameter).

- [ ] **Step 1: Write the failing test**

Add to `keel-controlplane/src/worker.rs`'s `mod tests`, near the other `reconcile_services_*` tests:

```rust
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
        let placements: Placements = crate::store::load_or_default(&state_dir.join("placements.yaml"));
        let used_addresses: UsedAddresses = crate::store::load_or_default(&state_dir.join("used_addresses.yaml"));
        let services = Services::load(&state_dir, test_service_cidr());
        let restarted_commands = spawn(
            Registry::new(test_cluster_cidr()),
            placements,
            services,
            used_addresses,
            Standbys::new(),
            PendingFences::new(),
            state_dir,
        )
        .1;
        register_node(&restarted_commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);
        heartbeat_with_jails(&restarted_commands, "node-1", vec![running("web-0")]);

        assert_eq!(
            reconcile(&restarted_commands),
            vec![],
            "the already-placed replica must not be seen as missing and rescheduled after a restart"
        );
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p keel-controlplane reconcile_services_after_a_simulated_restart_does_not_duplicate_an_already_placed_replica 2>&1 | tail -30`
Expected: if Tasks 1-7 are already applied in your working tree, this passes immediately (there's no separate implementation step left to do, persistence already exists). Verify it actually exercises the fix by temporarily reverting to `Placements::new()`/`UsedAddresses::new()`/`Services::new(test_service_cidr())` instead of the three `load`/`load_or_default` calls in the "restart" block above, re-running, and confirming it now fails with a non-empty `Schedule` action in the result (proving a duplicate would have been scheduled without persistence), then restore the load calls.

- [ ] **Step 3: Write one consolidated test covering the 6 mutating commands not already exercised above**

The test above exercises `RecordPlacement`, `RecordReplicaAddress`, and `ApplyService`. The remaining 6 mutating commands (`RemovePlacement`, `ReleaseReplicaAddress`, `RecordStandby`, `RecordPendingFence`, `RemovePendingFence`, `DeleteService`) each need their own restart-survival proof per the design spec's Testing section, one consolidated test covers all 6 rather than 6 near-identical test functions. Add to `keel-controlplane/src/worker.rs`'s `mod tests`, right after the test from Step 1:

```rust
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
                state_dir.clone(),
            )
            .1;
            register_node(&commands, "node-1", "10.0.0.1", 4.0, 8 * 1024 * 1024 * 1024);

            let (tx, rx) = mpsc::channel();
            commands.send(Command::RecordStandby("db-0".to_string(), "node-2".to_string(), tx)).unwrap();
            rx.recv().unwrap();

            let (tx, rx) = mpsc::channel();
            commands.send(Command::RecordPendingFence("db-1".to_string(), "node-3".to_string(), tx)).unwrap();
            rx.recv().unwrap();

            // A second, separately-recorded fence, so its *absence* after
            // restart is provably due to RemovePendingFence, not just never
            // having been recorded in the first place.
            let (tx, rx) = mpsc::channel();
            commands.send(Command::RecordPendingFence("db-2".to_string(), "node-4".to_string(), tx)).unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands.send(Command::RemovePendingFence("db-2".to_string(), tx)).unwrap();
            rx.recv().unwrap();

            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordReplicaAddress("db-3".to_string(), "node-1".to_string(), "10.0.60.5".parse().unwrap(), tx))
                .unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands
                .send(Command::RecordReplicaAddress("db-4".to_string(), "node-1".to_string(), "10.0.60.6".parse().unwrap(), tx))
                .unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands.send(Command::ReleaseReplicaAddress("db-4".to_string(), tx)).unwrap();
            rx.recv().unwrap();

            let (tx, rx) = mpsc::channel();
            commands.send(Command::RecordPlacement("db-5".to_string(), "node-1".to_string(), tx)).unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands.send(Command::RecordPlacement("db-6".to_string(), "node-1".to_string(), tx)).unwrap();
            rx.recv().unwrap();
            let (tx, rx) = mpsc::channel();
            commands.send(Command::RemovePlacement("db-6".to_string(), tx)).unwrap();
            rx.recv().unwrap();

            apply_service(&commands, "web", 1);
            apply_service(&commands, "api", 1);
            let (tx, rx) = mpsc::channel();
            commands.send(Command::DeleteService("api".to_string(), tx)).unwrap();
            rx.recv().unwrap().unwrap();
        }

        let standbys: Standbys = crate::store::load_or_default(&state_dir.join("standbys.yaml"));
        assert_eq!(standbys.get("db-0"), Some("node-2"), "RecordStandby must survive a restart");

        let pending_fences: PendingFences = crate::store::load_or_default(&state_dir.join("pending_fences.yaml"));
        assert_eq!(pending_fences.for_node("node-3"), vec!["db-1".to_string()], "RecordPendingFence must survive a restart");
        assert_eq!(pending_fences.for_node("node-4"), Vec::<String>::new(), "RemovePendingFence must survive a restart");

        let used_addresses: UsedAddresses = crate::store::load_or_default(&state_dir.join("used_addresses.yaml"));
        assert_eq!(used_addresses.address_of("db-3"), Some("10.0.60.5".parse().unwrap()), "RecordReplicaAddress must survive a restart");
        assert_eq!(used_addresses.address_of("db-4"), None, "ReleaseReplicaAddress must survive a restart");

        let placements: Placements = crate::store::load_or_default(&state_dir.join("placements.yaml"));
        assert_eq!(placements.get("db-5"), Some("node-1"), "RecordPlacement must survive a restart");
        assert_eq!(placements.get("db-6"), None, "RemovePlacement must survive a restart");

        let services = Services::load(&state_dir, test_service_cidr());
        assert!(services.get("web").is_some(), "ApplyService must survive a restart");
        assert!(services.get("api").is_none(), "DeleteService must survive a restart");
    }
```

- [ ] **Step 4: Run both tests to verify they pass**

Run: `cargo test -p keel-controlplane reconcile_services_after_a_simulated_restart_does_not_duplicate_an_already_placed_replica every_remaining_mutating_command_persists_and_survives_a_simulated_restart 2>&1 | tail -40`
Expected: both PASS.

- [ ] **Step 5: Run the full test suite**

Run: `cargo test --workspace 2>&1 | tail -60`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add keel-controlplane/src/worker.rs
git commit -m "test(keel-controlplane): prove every mutating command survives a restart"
```

---

### Task 9: `Registry::last_known_addr`

**Files:**
- Modify: `keel-controlplane/src/registry.rs`

**Interfaces:**
- Produces: `Registry::last_known_addr(&self, node_id: &str) -> Option<String>`, Task 10 calls this from inside `Command::PrepareForceRepin`'s handler.

- [ ] **Step 1: Write the failing tests**

Add to `keel-controlplane/src/registry.rs`'s existing `mod tests`:

```rust
    #[test]
    fn last_known_addr_returns_the_registered_address_regardless_of_aliveness() {
        let mut registry = Registry::new(test_cluster_cidr());
        let t0 = Instant::now();
        registry.register("node-1".to_string(), "10.0.0.1".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, t0).unwrap();

        // Confirm it reports Dead at this point (the whole reason
        // last_known_addr needs to bypass this check)...
        let past_threshold = t0 + DEAD_THRESHOLD;
        assert!(matches!(registry.resolve("node-1", past_threshold), Err(ResolveError::Dead { .. })));

        // ...yet the address is still returned.
        assert_eq!(registry.last_known_addr("node-1"), Some("10.0.0.1".to_string()));
    }

    #[test]
    fn last_known_addr_on_an_unknown_node_is_none() {
        let registry = Registry::new(test_cluster_cidr());
        assert_eq!(registry.last_known_addr("missing"), None);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p keel-controlplane last_known_addr 2>&1 | tail -20`
Expected: FAIL with "no method named `last_known_addr` found for struct `Registry`".

- [ ] **Step 3: Implement `last_known_addr`**

In `keel-controlplane/src/registry.rs`, add right after the existing `pub fn replicate_addr`:

```rust
    /// The node's last-registered address, with no aliveness check --
    /// deliberately bypassing `DEAD_THRESHOLD`. Used only by the immediate
    /// force-repin fencing push: the entire point is attempting to reach a
    /// node the heartbeat-derived state currently calls "dead," in case
    /// it's actually alive and only failing to heartbeat to the control
    /// plane specifically.
    pub fn last_known_addr(&self, node_id: &str) -> Option<String> {
        self.nodes.get(node_id).map(|r| r.addr.clone())
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p keel-controlplane last_known_addr 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Run the full test suite**

Run: `cargo test -p keel-controlplane 2>&1 | tail -30`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add keel-controlplane/src/registry.rs
git commit -m "feat(keel-controlplane): add Registry::last_known_addr for immediate fencing push"
```

---

### Task 10: Immediate fencing push

**Files:**
- Modify: `keel-controlplane/src/worker.rs` (`ForceRepinPrep` gains a field, computed inside `Command::PrepareForceRepin`)
- Modify: `keel-controlplane/src/http.rs` (`handle_force_repin` attempts the immediate push)

**Interfaces:**
- Consumes: `Registry::last_known_addr` (Task 9).
- Produces: `ForceRepinPrep.old_node_last_known_addr: Option<String>`, read by `http.rs`'s `handle_force_repin`.

- [ ] **Step 1: Skip a separate `worker.rs` unit test for this field, by design**

A dedicated `worker.rs`-level test for `ForceRepinPrep.old_node_last_known_addr`
would need `old-primary` to be both genuinely registered (so it has a real
address on file) and `resolve()`-Dead, which, like the `http.rs` test in
Step 4 below, has no way to happen without a real sleep past the 15-second
`DEAD_THRESHOLD` (see this task's design-spec section for why). Running
that same real-time wait twice, once here and once in Step 4, would double
this plan's slowest test for no extra coverage: the `http.rs` end-to-end
test in Step 4 already exercises `old_node_last_known_addr` as the specific
mechanism behind the observable behavior it proves (the `DELETE` arriving).
No test file changes in this step, proceed directly to Step 2.

- [ ] **Step 2: Add the field and compute it in `Command::PrepareForceRepin`**

In `keel-controlplane/src/worker.rs`, change:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct ForceRepinPrep {
    pub old_node_id: String,
    pub standby_node_id: String,
    pub standby_addr: String,
    pub template: keel_spec::JailTemplate,
    pub fresh_standby_node_id: String,
    pub fresh_standby_addr: String,
    pub address: std::net::Ipv4Addr,
    pub prefix_len: u8,
}
```

to:

```rust
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
}
```

Then, inside the `Command::PrepareForceRepin` handler, change:

```rust
                let old_node_id = placements.get(&replica_name).map(|s| s.to_string()).ok_or_else(|| ForceRepinError::NotPlaced(replica_name.clone()))?;
                // Checked before the primary-aliveness check below: a
                // non-stateful replica (no recorded standby at all) must
                // report NotStateful regardless of whether its sole node
                // happens to be Alive or Dead, rather than reporting
                // PrimaryStillAlive first and never surfacing NotStateful for
                // an Alive-but-stateless replica.
                let standby_node_id = standbys.get(&replica_name).map(|s| s.to_string()).ok_or_else(|| ForceRepinError::NotStateful(replica_name.clone()))?;
                if registry.resolve(&old_node_id, now).is_ok() {
                    return Err(ForceRepinError::PrimaryStillAlive(old_node_id));
                }
```

to:

```rust
                let old_node_id = placements.get(&replica_name).map(|s| s.to_string()).ok_or_else(|| ForceRepinError::NotPlaced(replica_name.clone()))?;
                // Checked before the primary-aliveness check below: a
                // non-stateful replica (no recorded standby at all) must
                // report NotStateful regardless of whether its sole node
                // happens to be Alive or Dead, rather than reporting
                // PrimaryStillAlive first and never surfacing NotStateful for
                // an Alive-but-stateless replica.
                let standby_node_id = standbys.get(&replica_name).map(|s| s.to_string()).ok_or_else(|| ForceRepinError::NotStateful(replica_name.clone()))?;
                if registry.resolve(&old_node_id, now).is_ok() {
                    return Err(ForceRepinError::PrimaryStillAlive(old_node_id));
                }
                // No aliveness check here on purpose (see last_known_addr's
                // own doc comment): the whole point of the immediate fencing
                // push is attempting to reach a node the check just above
                // called Dead, in case it's actually alive and only failing
                // to heartbeat to the control plane specifically.
                let old_node_last_known_addr = registry.last_known_addr(&old_node_id);
```

And change the `Ok(ForceRepinPrep { ... })` construction near the end of the same handler:

```rust
                Ok(ForceRepinPrep {
                    old_node_id,
                    standby_node_id,
                    standby_addr,
                    template,
                    fresh_standby_node_id,
                    fresh_standby_addr,
                    address,
                    prefix_len: pod_cidr.prefix_len(),
                })
```

to:

```rust
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
                })
```

- [ ] **Step 3: Fix the existing `ForceRepinPrep` test assertions in `worker.rs`**

Search for `ForceRepinPrep {` in `keel-controlplane/src/worker.rs`'s test module (the existing happy-path tests construct or destructure this struct directly). Run:

```bash
grep -n "ForceRepinPrep" keel-controlplane/src/worker.rs
```

For each test that pattern-matches on `ForceRepinPrep { .. }` with named fields (not `..`), add `old_node_last_known_addr: _,` (or an explicit expected value, if the test already asserts on it) to the pattern. For each test that constructs one directly, add `old_node_last_known_addr: None,` (or the specific expected address) to the literal. There is no fixed set of exact strings to replace here since the surrounding assertions vary per test, read each match, add the one missing field, and move on.

Run `cargo build -p keel-controlplane --tests 2>&1 | tail -40` after each edit; the compiler names the exact line of every place still missing the field.

- [ ] **Step 4: Write the failing end-to-end test (`http.rs`)**

First, add this new fake-server helper to `keel-controlplane/src/http.rs`'s
`mod tests`, right after the existing `start_fake_remote_tls_agentd` (it's
the same TLS handshake and request-draining shape, but records every
`DELETE /jails/<name>` path it receives instead of ignoring the request):

```rust
    fn start_fake_remote_tls_agentd_recording_deletes(status: u16) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server_config = Arc::new(
            tls::load_server_config(&fixture("fixture-node.crt"), &fixture("fixture-node.key"), &fixture("ca.crt"), &fixture("crl.pem"))
                .unwrap(),
        );
        let deletes = Arc::new(Mutex::new(Vec::new()));
        let thread_deletes = Arc::clone(&deletes);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&server_config)) else { continue };
                let mut tls_stream = rustls::StreamOwned::new(conn, stream);
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match tls_stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&chunk[..n]),
                    }
                }
                let request_text = String::from_utf8_lossy(&request);
                if let Some(first_line) = request_text.lines().next() {
                    let mut parts = first_line.split_whitespace();
                    if parts.next() == Some("DELETE") {
                        if let Some(replica_name) = parts.next().and_then(|path| path.strip_prefix("/jails/")) {
                            thread_deletes.lock().unwrap().push(replica_name.to_string());
                        }
                    }
                }
                let response = format!("HTTP/1.1 {status} OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                let _ = tls_stream.write_all(response.as_bytes());
                let _ = tls_stream.flush();
            }
        });
        (addr, deletes)
    }
```

`Mutex` needs importing in this file's test module if not already in scope
,  check with `grep -n "^use std::sync" keel-controlplane/src/http.rs`; add
`use std::sync::Mutex;` to the existing `use std::sync::{...}` line if it's
missing.

Then add the test itself, right after the existing
`force_repin_happy_path_updates_placements_standbys_and_pending_fences`
(this is that same test's setup, with the old primary swapped for a real
registered-and-then-aged-past-`DEAD_THRESHOLD` node instead of the
never-registered `"node-unreachable"` stand-in it uses, since a
never-registered node has no address on file for `last_known_addr` to find):

```rust
    #[test]
    fn force_repin_immediately_pushes_a_delete_to_the_old_primary_without_waiting_for_its_heartbeat() {
        let (cp_addr, commands) = start_test_server_with_commands();

        // Registered now, then aged past DEAD_THRESHOLD below -- unlike the
        // other force-repin tests' "node-unreachable" (never registered at
        // all, which resolve()-fails identically but leaves no address on
        // file), this one needs a real address for last_known_addr to find.
        let (old_primary_addr, old_primary_deletes) = start_fake_remote_tls_agentd_recording_deletes(200);
        register_node(&cp_addr, "old-primary", &old_primary_addr);

        std::thread::sleep(std::time::Duration::from_secs(16));

        // Registered after the sleep above, so they're freshly Alive (not
        // also aged past DEAD_THRESHOLD) when force-repin runs.
        let node_b = start_fake_remote_tls_agentd(200, "running: true\n");
        register_node(&cp_addr, "node-b", &node_b);
        send_request(
            &cp_addr,
            "POST",
            "/nodes/register",
            "id: node-c\naddr: 127.0.0.1:1\nreplicate_addr: 127.0.0.1:2\ncapacity_cpu: 4.0\ncapacity_memory: 8589934592\n",
        );

        apply_service_with_template_via_http(&cp_addr, "db", 1);
        record_placement(&commands, "db-0", "old-primary");
        let (tx, rx) = mpsc::channel();
        commands.send(Command::RecordStandby("db-0".to_string(), "node-b".to_string(), tx)).unwrap();
        rx.recv().unwrap();

        let (status, body) = send_request(&cp_addr, "POST", "/replicas/db-0/force-repin", "");
        assert_eq!(status, 200, "got: {body}");

        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(
            old_primary_deletes.lock().unwrap().contains(&"db-0".to_string()),
            "expected the immediate push to have sent a DELETE for db-0 to the old primary, with no heartbeat from it ever received"
        );
    }
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `cargo test -p keel-controlplane force_repin_immediately_pushes_a_delete_to_the_old_primary 2>&1 | tail -40`
Expected: FAIL (either a compile error from an unimplemented helper, or, once those are filled in per Step 4's instructions, a real assertion failure showing an empty delete log), confirming the immediate push doesn't exist yet.

- [ ] **Step 6: Implement the immediate push in `handle_force_repin`**

In `keel-controlplane/src/http.rs`, change:

```rust
    match forward(&prep.standby_addr, "PUT", &format!("/jails/{name}"), body.as_bytes(), client_config) {
        Ok((status, resp_body)) if (200..300).contains(&status) => {
            send_record_placement(name, &prep.standby_node_id, commands);
            send_record_replica_address(name, &prep.standby_node_id, prep.address, commands);
            send_record_standby(name, &prep.fresh_standby_node_id, commands);
            send_record_pending_fence(name, &prep.old_node_id, commands);
            (200, resp_body)
        }
        Ok((status, resp_body)) => error_response(status, String::from_utf8_lossy(&resp_body).to_string()),
        Err(e) => error_response(500, format!("failed to reach node '{}' at {}: {e}", prep.standby_node_id, prep.standby_addr)),
    }
```

to:

```rust
    match forward(&prep.standby_addr, "PUT", &format!("/jails/{name}"), body.as_bytes(), client_config) {
        Ok((status, resp_body)) if (200..300).contains(&status) => {
            send_record_placement(name, &prep.standby_node_id, commands);
            send_record_replica_address(name, &prep.standby_node_id, prep.address, commands);
            send_record_standby(name, &prep.fresh_standby_node_id, commands);
            send_record_pending_fence(name, &prep.old_node_id, commands);
            // Attempt to reach the old primary immediately, rather than
            // waiting only for its own next heartbeat
            // (check_and_execute_fencing, unchanged, still retries this on
            // that node's next heartbeat if this attempt fails or no
            // last-known address is on file at all).
            if let Some(old_addr) = &prep.old_node_last_known_addr {
                match forward(old_addr, "DELETE", &format!("/jails/{name}"), &[], client_config) {
                    Ok((del_status, _)) if (200..300).contains(&del_status) || del_status == 404 => {
                        send_remove_pending_fence(name, commands);
                    }
                    _ => {}
                }
            }
            (200, resp_body)
        }
        Ok((status, resp_body)) => error_response(status, String::from_utf8_lossy(&resp_body).to_string()),
        Err(e) => error_response(500, format!("failed to reach node '{}' at {}: {e}", prep.standby_node_id, prep.standby_addr)),
    }
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cargo test -p keel-controlplane force_repin_immediately_pushes_a_delete_to_the_old_primary 2>&1 | tail -40`
Expected: PASS. This test takes about 16 real seconds, that's expected and matches the design spec's Testing section.

- [ ] **Step 8: Run the full workspace test suite**

Run: `cargo test --workspace 2>&1 | tail -60`
Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add keel-controlplane/src/worker.rs keel-controlplane/src/http.rs
git commit -m "feat(keel-controlplane): push a fenced node's forced delete immediately"
```

---

### Task 11: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Full workspace build and test**

```bash
cargo build --workspace 2>&1 | tail -40
cargo test --workspace 2>&1 | tail -100
```

Expected: clean build, every test passes (accept the ~16s real-time cost of the one fencing integration test from Task 10).

- [ ] **Step 2: Confirm no em dashes were introduced**

```bash
git diff main.rs --stat 2>/dev/null; git log --oneline a0bab59..HEAD -- keel-controlplane keel-agentd/src/registration.rs keelctl/tests/cli.rs | cat
git diff a0bab59..HEAD -- keel-controlplane keel-agentd/src/registration.rs keelctl/tests/cli.rs | grep -c "—\|–" || true
```

Expected: `0` (or the grep finds nothing, exiting non-zero, which is the passing case).

- [ ] **Step 3: Manual verification on the real FreeBSD VM (per the design spec's Verification plan)**

Apply a service with a stateful replica, restart `keel-controlplane`, confirm `GET /services/<name>` still reports the existing replica healthy with no duplicate jail appearing on any node. This step is manual (not automatable from this plan), perform it before considering this phase fully done, and note the result in the audit artifact/memory the same way prior phases were tracked.
