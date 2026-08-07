# Milestone 24: Cluster Backup and Restore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give an operator one command to take a point-in-time, cluster-wide backup (control-plane state, every node's agent state, every node's volume data) and one command to restore a previously captured backup onto the same cluster.

**Architecture:** The control plane generates one timestamp-based backup ID per `backup create` call and fans it out to every currently-Alive node over the same direct per-node mTLS HTTP push `handle_force_repin` already uses (`forward()` in `keel-controlplane/src/http.rs`), reusing the existing single-worker-thread-owns-all-mutable-state design (`Sender<Command>` + reply channel) for every piece of file I/O so nothing races the worker's own persistence. Each node backs up its own agent state and streams a full `zfs send` of every volume dataset to a local file (reusing `ZfsManager::send_snapshot`/`receive_snapshot`, which are already generic over `Write`/`Read`, not TCP-specific). Restore is the mirror image: tear down live jails via the existing `Reconciler::delete`, wipe and replace the state directory tree, and `zfs receive` each volume dataset back from its saved file. Both the control plane and every restored node's `keel-agentd` require an operator-triggered restart afterward to load the restored files into memory, exactly like the control plane's existing "read state only at startup" behavior.

**Tech Stack:** Rust, std-only synchronous I/O (no async runtime, no `reqwest`/`axum` anywhere in this workspace), `serde_yaml` for all wire and on-disk formats, hand-rolled HTTP/1.1 parsing (`httparse`) over raw `rustls` TCP streams, `mpsc::Sender<Command>` + reply-channel pattern for every piece of state owned by a single worker thread.

## Global Constraints

- No new runtime dependencies: this workspace has zero `reqwest`, `axum`, `walkdir`, `fs_extra`, `chrono`, or `tokio`-driven HTTP paths anywhere, by deliberate existing convention. Every new piece of code in this plan uses only crates already declared in the touched crate's `Cargo.toml` plus already-existing intra-workspace `keel-*` path dependencies (`keel-spec` is already a dependency of both `keel-controlplane` and `keel-agentd`; `keel-controlplane` is already a dev-dependency of `keelctl`).
- All wire and on-disk formats are YAML via `serde_yaml`, never JSON.
- Every mutation of a component's on-disk `state_dir` happens on that component's single worker thread, reached only via its `Sender<Command>` + reply channel — never a raw filesystem write from an HTTP handler thread. This is a preexisting invariant (see `keel-controlplane/src/worker.rs`'s `persist_*` helpers and `keel-agentd/src/worker.rs`'s `handle_command`), not new to this milestone.
- `keel-controlplane`/`keel-agentd` only ever read their persisted `state_dir` once, inside their own startup path (`main.rs` / `Reconciler::new`) — there is no hot-reload mechanism anywhere in this codebase, and this milestone does not introduce one. Restore therefore requires an operator-triggered process restart on both sides to take effect.
- Restore is destructive by design (overwrites live state and volume data) and must never run without an explicit `--yes` flag from the operator on the CLI side, and must 404 on the control plane before touching any node if the requested backup ID is unknown.
- Test doubles: `keel_zfs::FakeZfsManager`, `keel_jail::FakeJailRuntime`/`FakeMountManager`, `keel_net::FakeNetManager`, `keel_ingress::FakeAcmeClient`/`FakeDnsProvider`, `keel_agentd::nginx::FakeNginxController` are the only fakes to construct a `Reconciler` in tests — never touch real ZFS/jail/network state in unit tests.

---

## Task 1: `keel-spec` shared directory copy/wipe helpers

Both `keel-controlplane` and `keel-agentd` need to (a) copy a component's `state_dir` tree into a `backups/<id>/...` destination without recursing into that same `state_dir`'s own `backups/` subdirectory, and (b) wipe a live `state_dir`'s current contents (again skipping `backups/`) before copying a backup's saved tree back onto it during restore. This task adds those two functions once, in the one crate both already depend on.

**Files:**
- Create: `keel-spec/src/fs_copy.rs`
- Modify: `keel-spec/src/lib.rs`

**Interfaces:**
- Produces: `keel_spec::fs_copy::copy_dir_recursive(src: &Path, dst: &Path, skip_names: &[&str]) -> std::io::Result<()>`, `keel_spec::fs_copy::wipe_dir_contents(dir: &Path, skip_names: &[&str]) -> std::io::Result<()>` — both used by Task 2 and Task 4.

- [ ] **Step 1: Write the failing tests**

Create `keel-spec/src/fs_copy.rs`:

```rust
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;

/// Recursively copies every entry under `src` into `dst` (creating `dst`
/// and any missing parent directories as needed), skipping any entry
/// directly under `src` whose name matches one in `skip_names`. Used by
/// backup to copy a component's `state_dir` into `backups/<id>/...`
/// without also copying that same `state_dir`'s own `backups/`
/// subdirectory into itself, and by restore to copy a backup's saved tree
/// back onto an already-wiped live `state_dir`.
pub fn copy_dir_recursive(src: &Path, dst: &Path, skip_names: &[&str]) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if skip_names.iter().any(|s| name == OsStr::new(s)) {
            continue;
        }
        let dst_path = dst.join(&name);
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &dst_path, skip_names)?;
        } else {
            fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}

/// Removes every entry directly under `dir`, except any name in
/// `skip_names`, recursively. Used by restore to clear a live `state_dir`
/// of its current contents (without touching its own `backups/`
/// subdirectory) before copying a backup's saved tree over it, so a
/// record that existed live but not in the backup doesn't survive the
/// restore.
pub fn wipe_dir_contents(dir: &Path, skip_names: &[&str]) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if skip_names.iter().any(|s| name == OsStr::new(s)) {
            continue;
        }
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn fresh_dir(name: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "keel-spec-fs-copy-test-{name}-{}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn copy_dir_recursive_copies_nested_files_and_subdirs() {
        let src = fresh_dir("copy_src");
        let dst = fresh_dir("copy_dst");
        fs::create_dir_all(src.join("sub")).unwrap();
        fs::write(src.join("top.yaml"), "top").unwrap();
        fs::write(src.join("sub").join("nested.yaml"), "nested").unwrap();

        copy_dir_recursive(&src, &dst, &[]).unwrap();

        assert_eq!(fs::read_to_string(dst.join("top.yaml")).unwrap(), "top");
        assert_eq!(
            fs::read_to_string(dst.join("sub").join("nested.yaml")).unwrap(),
            "nested"
        );
    }

    #[test]
    fn copy_dir_recursive_skips_named_top_level_entries() {
        let src = fresh_dir("copy_skip_src");
        let dst = fresh_dir("copy_skip_dst");
        fs::create_dir_all(src.join("backups")).unwrap();
        fs::write(src.join("backups").join("old.yaml"), "old").unwrap();
        fs::write(src.join("placements.yaml"), "keep").unwrap();

        copy_dir_recursive(&src, &dst, &["backups"]).unwrap();

        assert!(!dst.join("backups").exists());
        assert_eq!(
            fs::read_to_string(dst.join("placements.yaml")).unwrap(),
            "keep"
        );
    }

    #[test]
    fn wipe_dir_contents_removes_files_and_subdirs_but_skips_named_entries() {
        let dir = fresh_dir("wipe");
        fs::create_dir_all(dir.join("backups")).unwrap();
        fs::write(dir.join("backups").join("keep.yaml"), "keep").unwrap();
        fs::create_dir_all(dir.join("replica-targets")).unwrap();
        fs::write(dir.join("replica-targets").join("r.yaml"), "r").unwrap();
        fs::write(dir.join("placements.yaml"), "gone").unwrap();

        wipe_dir_contents(&dir, &["backups"]).unwrap();

        assert!(dir.join("backups").join("keep.yaml").exists());
        assert!(!dir.join("replica-targets").exists());
        assert!(!dir.join("placements.yaml").exists());
    }
}
```

- [ ] **Step 2: Wire the module in and run to verify it compiles and the tests pass**

Add to `keel-spec/src/lib.rs` (alongside the other `pub mod` lines):

```rust
pub mod fs_copy;
```

Run: `cargo test -p keel-spec fs_copy`
Expected: 3 tests pass (`copy_dir_recursive_copies_nested_files_and_subdirs`, `copy_dir_recursive_skips_named_top_level_entries`, `wipe_dir_contents_removes_files_and_subdirs_but_skips_named_entries`).

- [ ] **Step 3: Commit**

```bash
git add keel-spec/src/fs_copy.rs keel-spec/src/lib.rs
git commit -m "feat(keel-spec): add shared directory copy/wipe helpers for backup/restore"
```

---

## Task 2: `keel-controlplane` backup module — control-plane state, ID generation, manifest storage

Adds the control plane's own state-dir backup/restore, a dependency-free backup-ID generator (no calendar-formatting crate exists in this workspace, so this uses Howard Hinnant's `civil_from_days` algorithm), and manifest read/write helpers — plus the `Command` variants that let `http.rs` (Task 3) reach all of it through the existing single-worker-thread pattern.

**Files:**
- Create: `keel-controlplane/src/backup.rs`
- Modify: `keel-controlplane/src/lib.rs`, `keel-controlplane/src/wire.rs`, `keel-controlplane/src/worker.rs`

**Interfaces:**
- Consumes: `keel_spec::fs_copy::{copy_dir_recursive, wipe_dir_contents}` (Task 1), `crate::store::save` (existing, `keel-controlplane/src/store.rs:5-22`).
- Produces: `crate::backup::BackupError`, `crate::backup::generate_backup_id() -> String`, `crate::backup::backup_control_plane_state(state_dir: &Path, backup_id: &str) -> Result<(), BackupError>`, `crate::backup::restore_control_plane_state(state_dir: &Path, backup_id: &str) -> Result<(), BackupError>`, `crate::backup::list_manifests(state_dir: &Path) -> Vec<wire::BackupManifest>`, `crate::backup::get_manifest(state_dir: &Path, backup_id: &str) -> Option<wire::BackupManifest>`, `crate::wire::BackupComponentResult { success: bool, error: Option<String> }`, `crate::wire::BackupManifest { id: String, controlplane: BackupComponentResult, nodes: HashMap<String, BackupComponentResult> }`, and new `Command::BackupControlPlaneState`/`Command::RestoreControlPlaneState`/`Command::SaveBackupManifest`/`Command::ListBackupManifests`/`Command::GetBackupManifest` variants — all consumed by Task 3's HTTP handlers.

- [ ] **Step 1: Write the failing tests for ID generation**

Create `keel-controlplane/src/backup.rs`:

```rust
use std::io;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackupError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

const SKIP_TOP_LEVEL: &[&str] = &["backups"];

/// Converts a Unix timestamp (seconds since 1970-01-01T00:00:00Z) into the
/// backup ID format `YYYY-MM-DDTHH-MM-SSZ`, using Howard Hinnant's
/// `civil_from_days` algorithm
/// (http://howardhinnant.github.io/date_algorithms.html) so this crate
/// doesn't need a calendar-formatting dependency this project has never
/// otherwise needed.
pub fn format_backup_id(unix_secs: u64) -> String {
    let days = (unix_secs / 86400) as i64;
    let secs_of_day = unix_secs % 86400;
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}-{minute:02}-{second:02}Z")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// One timestamp-based ID per `backup create` call, shared by the control
/// plane and every node it fans out to (see the design doc's "Backup ID"
/// section).
pub fn generate_backup_id() -> String {
    let unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after 1970")
        .as_secs();
    format_backup_id(unix_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_backup_id_at_unix_epoch() {
        assert_eq!(format_backup_id(0), "1970-01-01T00-00-00Z");
    }

    #[test]
    fn format_backup_id_matches_the_designs_example() {
        assert_eq!(format_backup_id(1786019696), "2026-08-06T12-34-56Z");
    }

    #[test]
    fn format_backup_id_handles_a_leap_day() {
        assert_eq!(format_backup_id(951782400), "2000-02-29T00-00-00Z");
    }

    #[test]
    fn format_backup_id_handles_end_of_century() {
        assert_eq!(format_backup_id(4102444799), "2099-12-31T23-59-59Z");
    }
}
```

- [ ] **Step 2: Run to verify the ID-formatting tests pass**

Run: `cargo test -p keel-controlplane backup::tests::format_backup_id`
Expected: 4 tests pass.

- [ ] **Step 3: Write the failing tests for control-plane state backup/restore and manifest storage**

Append to `keel-controlplane/src/backup.rs` (inside the same `#[cfg(test)] mod tests` block, after the `format_backup_id_*` tests):

```rust
    fn fresh_state_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "keel-controlplane-backup-test-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn backup_then_restore_control_plane_state_round_trips_and_drops_stale_files() {
        let state_dir = fresh_state_dir("round_trip");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::write(state_dir.join("placements.yaml"), "web-0: node-1\n").unwrap();

        backup_control_plane_state(&state_dir, "2026-08-06T12-34-56Z").unwrap();
        assert_eq!(
            std::fs::read_to_string(
                state_dir
                    .join("backups")
                    .join("2026-08-06T12-34-56Z")
                    .join("controlplane")
                    .join("placements.yaml")
            )
            .unwrap(),
            "web-0: node-1\n"
        );

        // Mutate live state after the backup, including adding a file the
        // backup never saw.
        std::fs::write(state_dir.join("placements.yaml"), "web-0: node-2\n").unwrap();
        std::fs::write(state_dir.join("standbys.yaml"), "web-0: node-3\n").unwrap();

        restore_control_plane_state(&state_dir, "2026-08-06T12-34-56Z").unwrap();

        assert_eq!(
            std::fs::read_to_string(state_dir.join("placements.yaml")).unwrap(),
            "web-0: node-1\n"
        );
        assert!(
            !state_dir.join("standbys.yaml").exists(),
            "a file created after the backup was taken must not survive restore"
        );
    }

    #[test]
    fn list_manifests_is_empty_with_no_backups_dir() {
        let state_dir = fresh_state_dir("list_empty");
        assert_eq!(list_manifests(&state_dir), Vec::new());
    }

    #[test]
    fn get_manifest_on_an_unknown_id_is_none() {
        let state_dir = fresh_state_dir("get_unknown");
        assert_eq!(get_manifest(&state_dir, "missing"), None);
    }

    #[test]
    fn list_manifests_returns_every_saved_manifest_sorted_by_id() {
        let state_dir = fresh_state_dir("list_sorted");
        let manifest_a = crate::wire::BackupManifest {
            id: "2026-08-06T10-00-00Z".to_string(),
            controlplane: crate::wire::BackupComponentResult {
                success: true,
                error: None,
            },
            nodes: std::collections::HashMap::new(),
        };
        let manifest_b = crate::wire::BackupManifest {
            id: "2026-08-06T09-00-00Z".to_string(),
            controlplane: crate::wire::BackupComponentResult {
                success: true,
                error: None,
            },
            nodes: std::collections::HashMap::new(),
        };
        crate::store::save(
            &state_dir
                .join("backups")
                .join(&manifest_a.id)
                .join("manifest.yaml"),
            &manifest_a,
        )
        .unwrap();
        crate::store::save(
            &state_dir
                .join("backups")
                .join(&manifest_b.id)
                .join("manifest.yaml"),
            &manifest_b,
        )
        .unwrap();

        let manifests = list_manifests(&state_dir);
        assert_eq!(
            manifests.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
            vec![manifest_b.id.clone(), manifest_a.id.clone()],
            "expected manifests sorted by id"
        );
        assert_eq!(get_manifest(&state_dir, &manifest_a.id), Some(manifest_a));
    }
```

- [ ] **Step 4: Run to verify these new tests fail to compile (the functions/wire types don't exist yet)**

Run: `cargo test -p keel-controlplane backup::tests`
Expected: compile error — `backup_control_plane_state`, `restore_control_plane_state`, `list_manifests`, `get_manifest`, `crate::wire::BackupManifest`, `crate::wire::BackupComponentResult` are not found.

- [ ] **Step 5: Add the wire types**

Add to `keel-controlplane/src/wire.rs` (near the other request/response structs, e.g. after `NodeStatus`):

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupComponentResult {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub id: String,
    pub controlplane: BackupComponentResult,
    pub nodes: std::collections::HashMap<String, BackupComponentResult>,
}
```

- [ ] **Step 6: Implement `backup_control_plane_state`/`restore_control_plane_state`/`list_manifests`/`get_manifest`**

Add to `keel-controlplane/src/backup.rs` (above the `#[cfg(test)]` block):

```rust
pub fn backup_control_plane_state(state_dir: &Path, backup_id: &str) -> Result<(), BackupError> {
    let dest = state_dir.join("backups").join(backup_id).join("controlplane");
    keel_spec::fs_copy::copy_dir_recursive(state_dir, &dest, SKIP_TOP_LEVEL)?;
    Ok(())
}

pub fn restore_control_plane_state(state_dir: &Path, backup_id: &str) -> Result<(), BackupError> {
    let src = state_dir.join("backups").join(backup_id).join("controlplane");
    keel_spec::fs_copy::wipe_dir_contents(state_dir, SKIP_TOP_LEVEL)?;
    keel_spec::fs_copy::copy_dir_recursive(&src, state_dir, &[])?;
    Ok(())
}

pub fn list_manifests(state_dir: &Path) -> Vec<crate::wire::BackupManifest> {
    let backups_dir = state_dir.join("backups");
    let mut manifests = Vec::new();
    let Ok(entries) = std::fs::read_dir(&backups_dir) else {
        return manifests;
    };
    for entry in entries.flatten() {
        let manifest_path = entry.path().join("manifest.yaml");
        if let Ok(content) = std::fs::read_to_string(&manifest_path) {
            if let Ok(manifest) = serde_yaml::from_str(&content) {
                manifests.push(manifest);
            }
        }
    }
    manifests.sort_by(|a: &crate::wire::BackupManifest, b: &crate::wire::BackupManifest| {
        a.id.cmp(&b.id)
    });
    manifests
}

pub fn get_manifest(state_dir: &Path, backup_id: &str) -> Option<crate::wire::BackupManifest> {
    let manifest_path = state_dir.join("backups").join(backup_id).join("manifest.yaml");
    let content = std::fs::read_to_string(manifest_path).ok()?;
    serde_yaml::from_str(&content).ok()
}
```

- [ ] **Step 7: Run to verify all of `backup.rs`'s tests pass**

Run: `cargo test -p keel-controlplane backup::tests`
Expected: all tests pass (4 ID-formatting tests + 4 new tests from Step 3).

- [ ] **Step 8: Wire the module into `lib.rs`**

Add to `keel-controlplane/src/lib.rs` (alongside the other `pub mod` lines, e.g. after `pub mod addresses;`):

```rust
pub mod backup;
```

- [ ] **Step 9: Add the `Command` variants and worker wiring, with a test proving the round trip through the command channel**

**Note:** this crate now also has a `Cordoned` state (from the since-landed Milestone 23 cordon/drain work) threaded through `worker::spawn`/`handle_command` as an extra parameter positioned right after `pending_fences`/`PendingFences`. None of this task's new `Command` variants need it — just be aware `spawn(...)` now takes one more positional argument than older code examples show, and `Command`'s last variants are now `PrepareDrainRepin`/`Cordon`/`Uncordon`, not `PrepareForceRepin`.

Add to `keel-controlplane/src/worker.rs`'s `pub enum Command { ... }` (after the last variant, `Uncordon`):

```rust
    BackupControlPlaneState(String, Sender<Result<(), crate::backup::BackupError>>),
    RestoreControlPlaneState(String, Sender<Result<(), crate::backup::BackupError>>),
    SaveBackupManifest(wire::BackupManifest, Sender<Result<(), crate::backup::BackupError>>),
    ListBackupManifests(Sender<Vec<wire::BackupManifest>>),
    GetBackupManifest(String, Sender<Option<wire::BackupManifest>>),
```

Add to `handle_command`'s `match command { ... }` (after the last arm, `Command::Uncordon`):

```rust
        Command::BackupControlPlaneState(id, reply) => {
            let _ = reply.send(crate::backup::backup_control_plane_state(state_dir, &id));
        }
        Command::RestoreControlPlaneState(id, reply) => {
            let _ = reply.send(crate::backup::restore_control_plane_state(state_dir, &id));
        }
        Command::SaveBackupManifest(manifest, reply) => {
            let path = state_dir
                .join("backups")
                .join(&manifest.id)
                .join("manifest.yaml");
            let result = crate::store::save(&path, &manifest).map_err(crate::backup::BackupError::Io);
            let _ = reply.send(result);
        }
        Command::ListBackupManifests(reply) => {
            let _ = reply.send(crate::backup::list_manifests(state_dir));
        }
        Command::GetBackupManifest(id, reply) => {
            let _ = reply.send(crate::backup::get_manifest(state_dir, &id));
        }
```

Add to `worker.rs`'s `#[cfg(test)] mod tests` block (using the existing `spawn`/`fresh_state_dir`/`test_cluster_cidr`/`test_service_cidr` helpers already in that module):

```rust
    #[test]
    fn backup_control_plane_state_command_writes_the_state_dir_into_the_backup_tree() {
        let state_dir = fresh_state_dir();
        std::fs::create_dir_all(&state_dir).unwrap();
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
            .send(Command::BackupControlPlaneState(
                "backup-1".to_string(),
                tx,
            ))
            .unwrap();
        rx.recv().unwrap().unwrap();

        assert!(state_dir
            .join("backups")
            .join("backup-1")
            .join("controlplane")
            .join("placements.yaml")
            .exists());
    }

    #[test]
    fn save_then_list_then_get_backup_manifest_commands_round_trip() {
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
        let manifest = wire::BackupManifest {
            id: "backup-1".to_string(),
            controlplane: wire::BackupComponentResult {
                success: true,
                error: None,
            },
            nodes: std::collections::HashMap::new(),
        };

        let (save_tx, save_rx) = mpsc::channel();
        commands
            .send(Command::SaveBackupManifest(manifest.clone(), save_tx))
            .unwrap();
        save_rx.recv().unwrap().unwrap();

        let (list_tx, list_rx) = mpsc::channel();
        commands.send(Command::ListBackupManifests(list_tx)).unwrap();
        assert_eq!(list_rx.recv().unwrap(), vec![manifest.clone()]);

        let (get_tx, get_rx) = mpsc::channel();
        commands
            .send(Command::GetBackupManifest("backup-1".to_string(), get_tx))
            .unwrap();
        assert_eq!(get_rx.recv().unwrap(), Some(manifest));

        let (get_missing_tx, get_missing_rx) = mpsc::channel();
        commands
            .send(Command::GetBackupManifest(
                "missing".to_string(),
                get_missing_tx,
            ))
            .unwrap();
        assert_eq!(get_missing_rx.recv().unwrap(), None);
    }
```

- [ ] **Step 10: Run the full `keel-controlplane` test suite**

Run: `cargo test -p keel-controlplane`
Expected: all tests pass, including the two new worker-level tests and every existing test (no regressions).

- [ ] **Step 11: Commit**

```bash
git add keel-controlplane/src/backup.rs keel-controlplane/src/lib.rs keel-controlplane/src/wire.rs keel-controlplane/src/worker.rs
git commit -m "feat(keel-controlplane): add backup module, manifest wire types, and Command wiring for cluster backup/restore"
```

---

## Task 3: `keel-controlplane` HTTP surface — `POST /backup`, `GET /backup`, `POST /restore/<id>`

Adds the three routes that trigger a cluster-wide backup, list known backups, and trigger a cluster-wide restore, fanning out to every currently-Alive node using the same `forward()` helper `handle_force_repin` already uses.

**Files:**
- Modify: `keel-controlplane/src/http.rs`

**Interfaces:**
- Consumes: `Command::List` (existing, returns `Vec<wire::NodeStatus>`), `Command::BackupControlPlaneState`/`RestoreControlPlaneState`/`SaveBackupManifest`/`ListBackupManifests`/`GetBackupManifest` (Task 2), `crate::backup::generate_backup_id()` (Task 2), the existing private `fn forward(addr: &str, method: &str, path: &str, body: &[u8], client_config: &Arc<rustls::ClientConfig>) -> Result<(u16, Vec<u8>), String>` (`http.rs:957-963`).
- Produces: `POST /backup`, `GET /backup`, `POST /restore/<id>` routes, consumed by Task 6's `keelctl` CLI.

- [ ] **Step 1: Write the failing tests**

Add to `keel-controlplane/src/http.rs`'s `#[cfg(test)] mod tests` block (using the existing `start_test_server_with_commands`, `register_node`, `send_request`, `start_fake_remote_tls_agentd` helpers already in that module):

```rust
    #[test]
    fn backup_create_fans_out_to_every_alive_node_and_the_result_is_listable() {
        let (cp_addr, _commands) = start_test_server_with_commands();
        let node_b = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-b", &node_b);

        let (status, body) = send_request(&cp_addr, "POST", "/backup", "");
        assert_eq!(status, 200, "got: {body}");
        assert!(body.contains("success: true"), "got: {body}");
        assert!(body.contains("node-b"), "got: {body}");

        let (status, list_body) = send_request(&cp_addr, "GET", "/backup", "");
        assert_eq!(status, 200);
        assert!(
            list_body.contains("id:"),
            "expected the just-created backup to appear in the list, got: {list_body}"
        );
    }

    #[test]
    fn restore_on_an_unknown_backup_id_returns_404() {
        let (cp_addr, _commands) = start_test_server_with_commands();

        let (status, body) = send_request(&cp_addr, "POST", "/restore/unknown-id", "");
        assert_eq!(status, 404, "got: {body}");
    }

    #[test]
    fn backup_then_restore_round_trips_through_the_control_plane_and_every_alive_node() {
        let (cp_addr, _commands) = start_test_server_with_commands();
        let node_b = start_fake_remote_tls_agentd(200, "");
        register_node(&cp_addr, "node-b", &node_b);

        let (status, create_body) = send_request(&cp_addr, "POST", "/backup", "");
        assert_eq!(status, 200, "got: {create_body}");
        let id = create_body
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .expect("expected an 'id: <value>' line in the manifest")
            .trim()
            .to_string();

        let (status, restore_body) =
            send_request(&cp_addr, "POST", &format!("/restore/{id}"), "");
        assert_eq!(status, 200, "got: {restore_body}");
        assert!(
            restore_body.contains("success: true"),
            "got: {restore_body}"
        );
    }
```

- [ ] **Step 2: Run to verify these tests fail**

Run: `cargo test -p keel-controlplane http::tests::backup_create_fans_out http::tests::restore_on_an_unknown_backup_id http::tests::backup_then_restore_round_trips`
Expected: failures — `/backup` and `/restore/*` currently 404 (no route matches).

- [ ] **Step 3: Add the three handler functions**

Add to `keel-controlplane/src/http.rs` (near `handle_force_repin`):

```rust
fn handle_backup_create(
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let id = crate::backup::generate_backup_id();

    let (cp_tx, cp_rx) = mpsc::channel();
    if commands
        .send(Command::BackupControlPlaneState(id.clone(), cp_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let controlplane = match cp_rx.recv() {
        Ok(Ok(())) => wire::BackupComponentResult {
            success: true,
            error: None,
        },
        Ok(Err(e)) => wire::BackupComponentResult {
            success: false,
            error: Some(e.to_string()),
        },
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    };

    let (list_tx, list_rx) = mpsc::channel();
    if commands.send(Command::List(list_tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let nodes = match list_rx.recv() {
        Ok(nodes) => nodes,
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    };

    let mut node_results = std::collections::HashMap::new();
    for node in nodes {
        let result = if node.status != wire::NodeState::Alive {
            wire::BackupComponentResult {
                success: false,
                error: Some("node was not Alive when backup create ran".to_string()),
            }
        } else {
            match forward(&node.addr, "POST", &format!("/backup/{id}"), &[], client_config) {
                Ok((status, _)) if (200..300).contains(&status) => wire::BackupComponentResult {
                    success: true,
                    error: None,
                },
                Ok((status, body)) => wire::BackupComponentResult {
                    success: false,
                    error: Some(format!(
                        "status {status}: {}",
                        String::from_utf8_lossy(&body)
                    )),
                },
                Err(e) => wire::BackupComponentResult {
                    success: false,
                    error: Some(e),
                },
            }
        };
        node_results.insert(node.id, result);
    }

    let manifest = wire::BackupManifest {
        id,
        controlplane,
        nodes: node_results,
    };
    let (save_tx, save_rx) = mpsc::channel();
    if commands
        .send(Command::SaveBackupManifest(manifest.clone(), save_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match save_rx.recv() {
        Ok(Ok(())) => yaml_response(200, &manifest),
        Ok(Err(e)) => error_response(
            500,
            format!("backup completed but failed to write manifest: {e}"),
        ),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_backup_list(commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (tx, rx) = mpsc::channel();
    if commands.send(Command::ListBackupManifests(tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match rx.recv() {
        Ok(manifests) => yaml_response(200, &manifests),
        Err(_) => error_response(500, "control plane worker did not respond".to_string()),
    }
}

fn handle_restore(
    id: &str,
    commands: &Sender<Command>,
    client_config: &Arc<rustls::ClientConfig>,
) -> (u16, Vec<u8>) {
    let (get_tx, get_rx) = mpsc::channel();
    if commands
        .send(Command::GetBackupManifest(id.to_string(), get_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    match get_rx.recv() {
        Ok(Some(_)) => {}
        Ok(None) => return error_response(404, format!("no backup '{id}' found")),
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    }

    let (list_tx, list_rx) = mpsc::channel();
    if commands.send(Command::List(list_tx)).is_err() {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let nodes = match list_rx.recv() {
        Ok(nodes) => nodes,
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    };

    let mut node_results = std::collections::HashMap::new();
    for node in nodes {
        let result = if node.status != wire::NodeState::Alive {
            wire::BackupComponentResult {
                success: false,
                error: Some("node was not reachable during restore".to_string()),
            }
        } else {
            match forward(&node.addr, "POST", &format!("/restore/{id}"), &[], client_config) {
                Ok((status, _)) if (200..300).contains(&status) => wire::BackupComponentResult {
                    success: true,
                    error: None,
                },
                Ok((status, body)) => wire::BackupComponentResult {
                    success: false,
                    error: Some(format!(
                        "status {status}: {}",
                        String::from_utf8_lossy(&body)
                    )),
                },
                Err(e) => wire::BackupComponentResult {
                    success: false,
                    error: Some(e),
                },
            }
        };
        node_results.insert(node.id, result);
    }

    let (cp_tx, cp_rx) = mpsc::channel();
    if commands
        .send(Command::RestoreControlPlaneState(id.to_string(), cp_tx))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
    let controlplane = match cp_rx.recv() {
        Ok(Ok(())) => wire::BackupComponentResult {
            success: true,
            error: None,
        },
        Ok(Err(e)) => wire::BackupComponentResult {
            success: false,
            error: Some(e.to_string()),
        },
        Err(_) => return error_response(500, "control plane worker did not respond".to_string()),
    };

    let manifest = wire::BackupManifest {
        id: id.to_string(),
        controlplane,
        nodes: node_results,
    };
    yaml_response(200, &manifest)
}
```

- [ ] **Step 4: Add the three routes**

Add to `keel-controlplane/src/http.rs`'s `route()` function (after the `("POST", ["replicas", name, "force-repin"])` arm):

```rust
        ("POST", ["backup"]) => handle_backup_create(commands, client_config),
        ("GET", ["backup"]) => handle_backup_list(commands),
        ("POST", ["restore", id]) => handle_restore(id, commands, client_config),
```

- [ ] **Step 5: Run to verify the tests pass**

Run: `cargo test -p keel-controlplane http::tests::backup_create_fans_out http::tests::restore_on_an_unknown_backup_id http::tests::backup_then_restore_round_trips`
Expected: all 3 pass.

- [ ] **Step 6: Run the full `keel-controlplane` test suite**

Run: `cargo test -p keel-controlplane`
Expected: all tests pass, no regressions in existing routes.

- [ ] **Step 7: Commit**

```bash
git add keel-controlplane/src/http.rs
git commit -m "feat(keel-controlplane): add POST /backup, GET /backup, POST /restore/<id> with per-node fanout"
```

---

## Task 4: `keel-agentd` Reconciler accessors + agent-side backup/restore core

Adds two small accessor methods `Reconciler` needs to expose (`pool()`/`state_dir()` are currently private fields with no getters, unlike `list_volumes()`/`delete()` which are already `pub`), then the module that backs up a node's agent state (`JailRecord`s, `replica-targets/`, `ingress/`) and every volume dataset (via `zfs send` to a local file), and restores them (tearing down live jails first, wiping stale state, `zfs receive`-ing volume data back).

**Files:**
- Modify: `keel-agentd/src/reconciler.rs`, `keel-agentd/src/lib.rs`
- Create: `keel-agentd/src/backup.rs`

**Interfaces:**
- Consumes: `keel_spec::fs_copy::{copy_dir_recursive, wipe_dir_contents}` (Task 1), `Reconciler::{list_volumes, delete, list, get, apply}` (existing), `record::volume_dataset_path` (existing, `keel-agentd/src/record.rs:44-46`), `ZfsManager::{snapshot, send_snapshot, receive_snapshot, dataset_exists, destroy_dataset}` (existing, `keel-zfs/src/lib.rs`).
- Produces: `Reconciler::pool(&self) -> &str`, `Reconciler::state_dir(&self) -> &Path`, `keel_agentd::backup::BackupError`, `keel_agentd::wire::BackupResult { volumes: Vec<String>, failed_volumes: Vec<String> }`, `keel_agentd::backup::backup_agent_state(reconciler, zfs, pool, backup_id) -> Result<wire::BackupResult, BackupError>`, `keel_agentd::backup::restore_agent_state(reconciler, zfs, pool, backup_id) -> Result<(), BackupError>` — both consumed by Task 5's `Command` wiring.

- [ ] **Step 1: Add the Reconciler accessors**

Add to `keel-agentd/src/reconciler.rs`'s `impl<J, Z, N, M> Reconciler<J, Z, N, M>` block (right after `pub fn new(...)`'s closing brace):

```rust
    pub fn pool(&self) -> &str {
        &self.pool
    }

    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }
```

- [ ] **Step 2: Write the failing tests for `backup_agent_state`/`restore_agent_state`**

Create `keel-agentd/src/backup.rs`:

```rust
use crate::reconciler::Reconciler;
use crate::record;
use keel_jail::{JailRuntime, MountManager};
use keel_net::NetManager;
use keel_zfs::ZfsManager;
use std::fs::File;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackupError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Reconcile(#[from] crate::reconciler::ReconcileError),
    #[error(transparent)]
    Zfs(#[from] keel_zfs::ZfsError),
    #[error("no backup '{0}' found")]
    UnknownBackup(String),
}

const SKIP_TOP_LEVEL: &[&str] = &["backups"];

#[cfg(test)]
mod tests {
    use super::*;
    use keel_jail::{FakeJailRuntime, FakeMountManager};
    use keel_net::FakeNetManager;
    use keel_spec::{Metadata, NetworkSpec, ResourcesSpec, RestartPolicy, Spec, VolumeMount};
    use keel_zfs::FakeZfsManager;
    use std::path::PathBuf;

    fn test_state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-agentd-backup-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
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
                    mount_path: "/data".to_string(),
                    size: "1G".to_string(),
                }],
                replicate_to: None,
                generation: 0,
            },
        }
    }

    fn test_reconciler(
        name: &str,
    ) -> (
        Reconciler<FakeJailRuntime, FakeZfsManager, FakeNetManager, FakeMountManager>,
        FakeZfsManager,
    ) {
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
        (reconciler, zfs)
    }

    #[test]
    fn backup_agent_state_copies_records_and_sends_every_volume_dataset_to_a_file() {
        let (mut reconciler, zfs) = test_reconciler("backup_copies");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());

        let result = backup_agent_state(&reconciler, &zfs, "zroot", "backup-1").unwrap();
        assert_eq!(result.volumes, vec!["web-1-data".to_string()]);
        assert_eq!(result.failed_volumes, Vec::<String>::new());

        let state_dir = reconciler.state_dir();
        assert!(state_dir
            .join("backups/backup-1/agent/web-1.yaml")
            .exists());
        assert!(state_dir
            .join("backups/backup-1/zfs/web-1-data.zfs")
            .exists());
    }

    #[test]
    fn backup_agent_state_continues_past_one_failed_volume_and_still_backs_up_the_rest() {
        // Reproduces the design doc's Error Handling requirement: "other
        // datasets... still complete independently" -- a broken volume
        // dataset must not abort the whole node's backup.
        let (mut reconciler, zfs) = test_reconciler("backup_partial_failure");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        reconciler.apply(stateful_spec("web-2")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        // Destroy web-2's live volume dataset behind the reconciler's back
        // so its snapshot attempt fails with NotFound, while web-1's
        // dataset is untouched and still backable.
        zfs.destroy_dataset("zroot/keel/volumes/web-2-data").unwrap();

        let result = backup_agent_state(&reconciler, &zfs, "zroot", "backup-1").unwrap();
        assert_eq!(result.volumes, vec!["web-1-data".to_string()]);
        assert_eq!(result.failed_volumes, vec!["web-2-data".to_string()]);
        assert!(reconciler
            .state_dir()
            .join("backups/backup-1/zfs/web-1-data.zfs")
            .exists());
    }

    #[test]
    fn restore_agent_state_tears_down_a_jail_created_after_the_backup_and_restores_volume_data() {
        let (mut reconciler, zfs) = test_reconciler("restore_round_trip");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        backup_agent_state(&reconciler, &zfs, "zroot", "backup-1").unwrap();

        // Simulate drift after the backup: a second jail applied later.
        reconciler.apply(stateful_spec("web-2")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        assert!(reconciler
            .get("web-2", std::time::Instant::now())
            .is_some());

        restore_agent_state(&mut reconciler, &zfs, "zroot", "backup-1").unwrap();

        let state_dir = reconciler.state_dir().to_path_buf();
        assert!(
            !state_dir.join("web-2.yaml").exists(),
            "a jail record created after the backup must not survive restore"
        );
        assert!(state_dir.join("web-1.yaml").exists());
        assert!(zfs.dataset_exists("zroot/keel/volumes/web-1-data").unwrap());
    }

    #[test]
    fn restore_agent_state_on_an_unknown_backup_id_fails_without_tearing_anything_down() {
        let (mut reconciler, zfs) = test_reconciler("restore_unknown");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());

        let result = restore_agent_state(&mut reconciler, &zfs, "zroot", "missing-backup");
        assert!(matches!(result, Err(BackupError::UnknownBackup(_))));
        assert!(reconciler
            .get("web-1", std::time::Instant::now())
            .is_some());
    }
}
```

- [ ] **Step 3: Run to verify these tests fail to compile**

Run: `cargo test -p keel-agentd backup::tests`
Expected: compile error — `backup_agent_state`/`restore_agent_state` are not found.

- [ ] **Step 4: Add the `BackupResult` wire type, then implement `backup_agent_state`/`restore_agent_state`**

`backup_agent_state` must not let one bad volume dataset abort the whole node's backup (design doc Error Handling: "other datasets... still complete independently"), so it returns a wire-serializable result distinguishing which volumes succeeded from which failed, rather than a plain `Vec<String>` that a `?` on the first error would short-circuit. This same struct is reused directly as the HTTP response body in Task 5 (`crate::wire::VolumeStatus` is already returned the same way by `Reconciler::list_volumes`, so this isn't a new pattern in this crate).

Add to `keel-agentd/src/wire.rs` (near `VolumeStatus`):

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupResult {
    pub volumes: Vec<String>,
    pub failed_volumes: Vec<String>,
}
```

Add to `keel-agentd/src/backup.rs`, above the `#[cfg(test)]` block:

```rust
pub fn backup_agent_state<J: JailRuntime, Z: ZfsManager, N: NetManager, M: MountManager>(
    reconciler: &Reconciler<J, Z, N, M>,
    zfs: &Z,
    pool: &str,
    backup_id: &str,
) -> Result<crate::wire::BackupResult, BackupError> {
    let state_dir = reconciler.state_dir();
    let dest = state_dir.join("backups").join(backup_id).join("agent");
    keel_spec::fs_copy::copy_dir_recursive(state_dir, &dest, SKIP_TOP_LEVEL)?;

    let volumes = reconciler.list_volumes()?;
    let zfs_dir = state_dir.join("backups").join(backup_id).join("zfs");
    std::fs::create_dir_all(&zfs_dir)?;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    for volume in volumes {
        match backup_one_volume(zfs, pool, backup_id, &zfs_dir, &volume.name) {
            Ok(()) => succeeded.push(volume.name),
            Err(e) => {
                eprintln!(
                    "keel-agentd: failed to back up volume '{}': {e}",
                    volume.name
                );
                failed.push(volume.name);
            }
        }
    }
    Ok(crate::wire::BackupResult {
        volumes: succeeded,
        failed_volumes: failed,
    })
}

fn backup_one_volume<Z: ZfsManager>(
    zfs: &Z,
    pool: &str,
    backup_id: &str,
    zfs_dir: &std::path::Path,
    volume_name: &str,
) -> Result<(), BackupError> {
    let dataset = record::volume_dataset_path(pool, volume_name);
    zfs.snapshot(&dataset, backup_id)?;
    let mut file = File::create(zfs_dir.join(format!("{volume_name}.zfs")))?;
    zfs.send_snapshot(&dataset, backup_id, None, &mut file)?;
    Ok(())
}

pub fn restore_agent_state<J: JailRuntime, Z: ZfsManager, N: NetManager, M: MountManager>(
    reconciler: &mut Reconciler<J, Z, N, M>,
    zfs: &Z,
    pool: &str,
    backup_id: &str,
) -> Result<(), BackupError> {
    let state_dir = reconciler.state_dir().to_path_buf();
    let backup_root = state_dir.join("backups").join(backup_id);
    if !backup_root.join("agent").is_dir() {
        return Err(BackupError::UnknownBackup(backup_id.to_string()));
    }

    let names: Vec<String> = reconciler
        .list(std::time::Instant::now())
        .into_iter()
        .map(|status| status.record.spec.metadata.name.clone())
        .collect();
    for name in &names {
        reconciler.delete(name)?;
    }

    keel_spec::fs_copy::wipe_dir_contents(&state_dir, SKIP_TOP_LEVEL)?;
    keel_spec::fs_copy::copy_dir_recursive(&backup_root.join("agent"), &state_dir, &[])?;

    let zfs_dir = backup_root.join("zfs");
    if zfs_dir.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(&zfs_dir)?.collect::<Result<_, _>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            let Some(volume_name) = file_name.strip_suffix(".zfs") else {
                continue;
            };
            let dataset = record::volume_dataset_path(pool, volume_name);
            if zfs.dataset_exists(&dataset)? {
                zfs.destroy_dataset(&dataset)?;
            }
            let mut file = File::open(entry.path())?;
            zfs.receive_snapshot(&dataset, &mut file)?;
        }
    }
    Ok(())
}
```

- [ ] **Step 5: Run to verify the tests pass**

Run: `cargo test -p keel-agentd backup::tests`
Expected: all 4 tests pass.

- [ ] **Step 6: Wire the module into `lib.rs`**

Add to `keel-agentd/src/lib.rs` (alongside the other `pub mod` lines, e.g. after `pub mod backoff;`):

```rust
pub mod backup;
```

- [ ] **Step 7: Run the full `keel-agentd` test suite**

Run: `cargo test -p keel-agentd`
Expected: all tests pass, no regressions.

- [ ] **Step 8: Commit**

```bash
git add keel-agentd/src/reconciler.rs keel-agentd/src/lib.rs keel-agentd/src/backup.rs
git commit -m "feat(keel-agentd): add Reconciler pool/state_dir accessors and agent-side backup/restore"
```

---

## Task 5: `keel-agentd` HTTP surface — `POST /backup/<id>`, `POST /restore/<id>`

Wires Task 4's `backup_agent_state`/`restore_agent_state` through the reconciler worker's existing `Command` channel and exposes them over HTTP, matching the exact pattern `Command::DeleteVolume`/`handle_delete_volume` already use.

**Files:**
- Modify: `keel-agentd/src/worker.rs`, `keel-agentd/src/wire.rs`, `keel-agentd/src/http.rs`

**Interfaces:**
- Consumes: `keel_agentd::backup::{backup_agent_state, restore_agent_state, BackupError}`, `keel_agentd::wire::BackupResult` (Task 4).
- Produces: `POST /backup/<id>`, `POST /restore/<id>` routes on `keel-agentd`, consumed by Task 3's control-plane fanout (`forward(&node.addr, "POST", "/backup/{id}", ...)` / `"/restore/{id}"`) and, for direct single-node testing, by `keelctl` against a bare `--socket`.

- [ ] **Step 1: Write the failing tests**

Add to `keel-agentd/src/worker.rs`'s `#[cfg(test)] mod tests` block (using the existing `spawn_test_worker` helper):

```rust
    #[test]
    fn backup_command_backs_up_every_provisioned_volume() {
        let commands = spawn_test_worker("backup_command_backs_up_every_provisioned_volume");
        commands
            .send(Command::Apply(sample_spec("web-1"), mpsc::channel().0))
            .unwrap();
        // Wait for the immediate reconcile Apply triggers by round-tripping
        // a Get before issuing Backup.
        let (get_tx, get_rx) = mpsc::channel();
        commands.send(Command::Get(None, get_tx)).unwrap();
        get_rx.recv().unwrap();

        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::Backup("backup-1".to_string(), tx))
            .unwrap();
        let result = rx.recv().unwrap().unwrap();
        assert_eq!(result.volumes, Vec::<String>::new());
        assert_eq!(result.failed_volumes, Vec::<String>::new());
    }

    #[test]
    fn restore_command_on_an_unknown_backup_id_returns_an_error() {
        let commands =
            spawn_test_worker("restore_command_on_an_unknown_backup_id_returns_an_error");
        let (tx, rx) = mpsc::channel();
        commands
            .send(Command::Restore("missing".to_string(), tx))
            .unwrap();
        assert!(matches!(
            rx.recv().unwrap(),
            Err(crate::backup::BackupError::UnknownBackup(_))
        ));
    }
```

- [ ] **Step 2: Run to verify these fail to compile**

Run: `cargo test -p keel-agentd worker::tests::backup_command worker::tests::restore_command`
Expected: compile error — `Command::Backup`/`Command::Restore` don't exist.

- [ ] **Step 3: Add the `Command` variants and worker wiring**

Add to `keel-agentd/src/worker.rs`'s `pub enum Command { ... }` (after `DeleteIngress`):

```rust
    Backup(String, Sender<Result<crate::wire::BackupResult, crate::backup::BackupError>>),
    Restore(String, Sender<Result<(), crate::backup::BackupError>>),
```

Add to `handle_command`'s `match command { ... }` (after the `Command::DeleteIngress` arm):

```rust
        Command::Backup(id, reply) => {
            let result = crate::backup::backup_agent_state(reconciler, zfs, pool, &id);
            let _ = reply.send(result);
        }
        Command::Restore(id, reply) => {
            let result = crate::backup::restore_agent_state(reconciler, zfs, pool, &id);
            let _ = reply.send(result);
        }
```

- [ ] **Step 4: Run to verify the worker-level tests pass**

Run: `cargo test -p keel-agentd worker::tests::backup_command worker::tests::restore_command`
Expected: both pass.

- [ ] **Step 5: Write the failing HTTP-level test**

Add to `keel-agentd/src/http.rs`'s `#[cfg(test)] mod tests` block (using whatever existing `start_test_server`-equivalent helper that module already has, mirroring `get_volume_and_delete_volume_commands_round_trip`'s style but at the HTTP layer):

```rust
    #[test]
    fn backup_then_restore_over_http_round_trips() {
        let (socket, _handle) = start_test_server("backup_then_restore_over_http_round_trips");

        let (status, _) = send_request(&socket, "POST", "/backup/backup-1", "");
        assert_eq!(status, 200);

        let (status, _) = send_request(&socket, "POST", "/restore/backup-1", "");
        assert_eq!(status, 200);

        let (status, body) = send_request(&socket, "POST", "/restore/unknown-id", "");
        assert_eq!(status, 404, "got: {body}");
    }
```

(If this test module's existing server-startup helper has a different name/signature than `start_test_server`, use whatever the surrounding tests in this same file already call — match the exact existing pattern rather than introducing a new one.)

- [ ] **Step 6: Run to verify it fails**

Run: `cargo test -p keel-agentd http::tests::backup_then_restore_over_http_round_trips`
Expected: fails — `/backup/*` and `/restore/*` currently 404 (no route matches) for the first two assertions.

- [ ] **Step 7: Add the two handlers**

`keel_agentd::wire::BackupResult` was already added in Task 4, Step 4 (it's `backup_agent_state`'s own return type, reused directly as this route's response body — no separate wire-type addition needed here).

Add to `keel-agentd/src/http.rs` (near `handle_delete_volume`):

```rust
fn handle_backup(id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::Backup(id.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(result)) if result.failed_volumes.is_empty() => yaml_response(200, &result),
        // Some volumes were backed up successfully and remain on disk even
        // though this response is non-2xx: the failure here is reported so
        // the control plane's manifest doesn't silently look complete, not
        // to imply nothing was captured.
        Ok(Ok(result)) => error_response(
            500,
            format!(
                "backed up {} volume(s), failed on: {}",
                result.volumes.len(),
                result.failed_volumes.join(", ")
            ),
        ),
        Ok(Err(e)) => error_response(500, e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}

fn handle_restore(id: &str, commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands
        .send(Command::Restore(id.to_string(), reply_tx))
        .is_err()
    {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(())) => (200, Vec::new()),
        Ok(Err(e @ crate::backup::BackupError::UnknownBackup(_))) => {
            error_response(404, e.to_string())
        }
        Ok(Err(e)) => error_response(500, e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}
```

- [ ] **Step 8: Add the two routes**

Add to `keel-agentd/src/http.rs`'s `route()` function (after the `("DELETE", ["ingress", name])` arm):

```rust
        ("POST", ["backup", id]) => handle_backup(id, commands),
        ("POST", ["restore", id]) => handle_restore(id, commands),
```

- [ ] **Step 9: Run to verify the HTTP-level test passes**

Run: `cargo test -p keel-agentd http::tests::backup_then_restore_over_http_round_trips`
Expected: passes.

- [ ] **Step 10: Run the full `keel-agentd` test suite**

Run: `cargo test -p keel-agentd`
Expected: all tests pass, no regressions in existing routes.

- [ ] **Step 11: Commit**

```bash
git add keel-agentd/src/worker.rs keel-agentd/src/wire.rs keel-agentd/src/http.rs
git commit -m "feat(keel-agentd): add POST /backup/<id> and POST /restore/<id>"
```

---

## Task 6: `keelctl` CLI — `backup create`, `backup list`, `restore <id> --yes`

Adds the operator-facing commands. `backup`/`restore` always target the control plane's own `/backup`/`/restore/<id>` routes directly (never `--node`-scoped — a cluster backup/restore is never per-node from the CLI's perspective), matching how `jails_path` already leaves the path bare when `node: None`.

**Files:**
- Modify: `keelctl/src/main.rs`, `keelctl/tests/cli.rs`

**Interfaces:**
- Consumes: `dispatch`, `success_body` (existing, `keelctl/src/main.rs:129-169`), the control plane's `/backup`, `/backup` (GET), `/restore/<id>` routes (Task 3).
- Produces: `keelctl backup create`, `keelctl backup list`, `keelctl restore <id> --yes` subcommands.

- [ ] **Step 1: Write the failing integration tests**

Add to `keelctl/tests/cli.rs` (using the existing `start_test_agentd_tcp`, `start_test_control_plane_with_node`, `run_keelctl_scheduled` helpers already in this file):

```rust
#[test]
fn backup_create_list_and_restore_round_trip_through_the_control_plane() {
    let node_addr = start_test_agentd_tcp("backup_round_trip");
    let control_plane_addr = start_test_control_plane_with_node("node-1", &node_addr);

    let (ok, create_stdout, stderr) =
        run_keelctl_scheduled(&control_plane_addr, &["backup", "create"]);
    assert!(ok, "backup create failed: {stderr}");
    assert!(
        create_stdout.contains("success: true"),
        "expected a successful backup manifest, got: {create_stdout}"
    );
    let id = create_stdout
        .lines()
        .find_map(|line| line.strip_prefix("id: "))
        .expect("expected an 'id: <value>' line in the backup create output")
        .trim()
        .to_string();

    let (ok, list_stdout, stderr) =
        run_keelctl_scheduled(&control_plane_addr, &["backup", "list"]);
    assert!(ok, "backup list failed: {stderr}");
    assert!(
        list_stdout.contains(&id),
        "expected the created backup's id to show up in the list, got: {list_stdout}"
    );

    let (ok, restore_stdout, stderr) =
        run_keelctl_scheduled(&control_plane_addr, &["restore", &id, "--yes"]);
    assert!(ok, "restore failed: {stderr}");
    assert!(
        restore_stdout.contains("success: true"),
        "expected a successful restore manifest, got: {restore_stdout}"
    );
}

#[test]
fn restore_without_yes_is_rejected_without_contacting_the_control_plane() {
    // An unroutable address: if this reached dispatch() at all, the test
    // would hang or fail with a connection error instead of the
    // confirmation-required message below.
    let (ok, _stdout, stderr) = run_keelctl_scheduled("127.0.0.1:1", &["restore", "some-id"]);
    assert!(!ok, "expected restore without --yes to fail");
    assert!(
        stderr.contains("--yes"),
        "expected the confirmation-required message, got: {stderr}"
    );
}
```

- [ ] **Step 2: Run to verify these fail**

Run: `cargo test -p keelctl --test cli backup_create_list_and_restore restore_without_yes`
Expected: failures — `keelctl` doesn't yet recognize `backup`/`restore` as subcommands (usage error).

- [ ] **Step 3: Implement `run_backup`/`run_restore`**

Add to `keelctl/src/main.rs` (near `run_force_repin`):

```rust
fn run_backup(target: &Target, args: &[String]) -> Result<String, String> {
    match args.split_first() {
        Some((cmd, _)) if cmd == "create" => success_body(dispatch(target, "POST", "/backup", "")),
        Some((cmd, _)) if cmd == "list" => success_body(dispatch(target, "GET", "/backup", "")),
        _ => Err("backup requires a subcommand: create|list".to_string()),
    }
}

fn run_restore(target: &Target, args: &[String]) -> Result<String, String> {
    let id = args.first().ok_or("restore requires a backup id")?;
    if !args.iter().any(|a| a == "--yes") {
        return Err("restore is destructive; pass --yes to confirm".to_string());
    }
    success_body(dispatch(target, "POST", &format!("/restore/{id}"), ""))
}
```

- [ ] **Step 4: Wire the subcommands into `main()`**

**Note:** this crate now also has `cordon`/`uncordon`/`drain` subcommands (from the since-landed Milestone 23 cordon/drain work) already present in this match and in the usage string below — add this task's two new arms alongside them, keep every existing arm, and extend (don't replace) the usage string.

Modify `keelctl/src/main.rs`'s `match args.split_first() { ... }` in `fn main()`:

```rust
    let result = match args.split_first() {
        Some((cmd, rest)) if cmd == "apply" => run_apply(&target, rest),
        Some((cmd, rest)) if cmd == "get" => run_get(&target, rest),
        Some((cmd, rest)) if cmd == "delete" => run_delete(&target, rest),
        Some((cmd, rest)) if cmd == "delete-volume" => run_delete_volume(&target, rest),
        Some((cmd, rest)) if cmd == "force-repin" => run_force_repin(&target, rest),
        Some((cmd, rest)) if cmd == "cordon" => run_cordon(&target, rest),
        Some((cmd, rest)) if cmd == "uncordon" => run_uncordon(&target, rest),
        Some((cmd, rest)) if cmd == "drain" => run_drain(&target, rest),
        Some((cmd, rest)) if cmd == "backup" => run_backup(&target, rest),
        Some((cmd, rest)) if cmd == "restore" => run_restore(&target, rest),
        _ => {
            eprintln!(
                "usage: keelctl <apply -f FILE|get [name]|delete NAME|delete-volume NAME|force-repin NAME|cordon NODE|uncordon NODE|drain NODE|backup create|backup list|restore ID --yes> [--socket PATH|--control-plane-addr ADDR --node ID]"
            );
            return ExitCode::FAILURE;
        }
    };
```

- [ ] **Step 5: Run to verify the integration tests pass**

Run: `cargo test -p keelctl --test cli backup_create_list_and_restore restore_without_yes`
Expected: both pass.

- [ ] **Step 6: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass across every crate, no regressions.

- [ ] **Step 7: Run clippy and fmt (this project's existing CI hygiene gate)**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: no formatting diffs, no new clippy warnings introduced by this milestone's code (pre-existing warnings noted in memory — `~59 clippy warnings found workspace-wide` as of Milestone 22 — are not this task's responsibility to fix, but must not increase).

- [ ] **Step 8: Commit**

```bash
git add keelctl/src/main.rs keelctl/tests/cli.rs
git commit -m "feat(keelctl): add backup create/list and restore <id> --yes"
```

---

## Task 7: Real FreeBSD VM verification

Following this project's existing discipline (unit/integration tests against fakes first, then real verification), confirm the whole flow works against genuine ZFS/jail state before calling this milestone done.

**⚠️ Safety gate — do not skip:** Memory records that Milestone 21 put a *production* site (`lebaron.sh`) on a real FreeBSD VPS with a real Let's Encrypt cert, and Milestone 22's dashboard verification ran against that same production VPS. Restore is destructive (it tears down live jails and overwrites volume data). **Before running any step in this task, confirm with the user which host is safe to use** — do not run `restore` against `lebaron.sh` or any other production host without explicit, current confirmation, even if a past session's verification used it for a read-only feature. Prefer a separate disposable test VM/jail environment (the kind used for Milestones 17/19's storage/replication verification, prior to Milestone 21 turning a VPS into production) or a fresh throwaway VM.

**Files:** none (manual operational verification, no code changes).

- [ ] **Step 1: Identify a safe non-production host** and get explicit confirmation before proceeding (see safety gate above).

- [ ] **Step 2: Single-node verification**
  - Deploy the milestone's binaries (`keel-controlplane`, `keel-agentd`, `keelctl`) to the confirmed test host.
  - Apply a stateful jail spec with a volume, write identifiable data into the volume.
  - Run `keelctl backup create`; confirm the manifest reports success for the control plane and the node.
  - Destroy the volume's live data (e.g. overwrite a test file inside the mounted volume).
  - Run `keelctl restore <id> --yes`; restart `keel-agentd` and `keel-controlplane` per the manifest's restart reminder.
  - Confirm the restored volume's content is byte-identical to what was backed up.

- [ ] **Step 3: Partial-failure verification**
  - Start a cluster-wide backup, then kill one node's `keel-agentd` process mid-backup (e.g. `SIGKILL` right after issuing `keelctl backup create`).
  - Confirm the resulting manifest reports that node as failed rather than the backup looking fully successful.

- [ ] **Step 4: Restore-drift verification**
  - After taking a backup, apply an additional jail spec (creating a jail the backup never saw).
  - Run `restore <id> --yes` and confirm, after the required restarts, that the extra jail is torn down and only the backed-up jails exist.

- [ ] **Step 5: Unknown-ID verification**
  - Run `keelctl restore some-nonexistent-id --yes` against the control plane; confirm a 404 and that no node's state changed.

- [ ] **Step 6: Multi-node verification**
  - Repeat Steps 2 and 4 across at least two nodes registered to the same control plane, confirming the manifest correctly attributes per-node success/failure and that restore's teardown/restore sequence is independent per node.

- [ ] **Step 7: Update the design doc's Status line**

Change `docs/superpowers/specs/2026-08-06-keel-agent-milestone24-backup-restore-design.md`'s `Status: Draft` to `Status: Implemented` once every step above has passed on real hardware.

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/specs/2026-08-06-keel-agent-milestone24-backup-restore-design.md
git commit -m "docs: mark Milestone 24 (backup/restore) design as implemented"
```
