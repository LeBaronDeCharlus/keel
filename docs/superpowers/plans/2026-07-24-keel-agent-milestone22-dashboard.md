# Milestone 22: keel dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only, auto-refreshing web dashboard (`keel-dashboard`, a new binary crate) showing cluster-wide node/jail/service/volume/ingress state, plus the two small control-plane extensions it needs (ingress reporting via heartbeat, a volumes-list endpoint).

**Architecture:** `keel-dashboard` is both an mTLS client of `keel-controlplane` (same `ControlPlane` target shape as `keelctl`) and its own browser-facing HTTPS server (a hand-rolled `rustls` listener, HTTP Basic Auth in front of every route). A background poller thread refreshes an in-memory `Snapshot` behind `Arc<RwLock<Snapshot>>` every `--poll-interval-secs`; the browser-facing HTTP layer only ever reads that cache. Getting there requires two upstream extensions first: `Heartbeat`/`NodeStatus` gain an `ingresses` field (mirroring the existing `jails` field), and a new `GET /volumes` (agentd) / `GET /nodes/{id}/volumes` (control plane) list route is added, which in turn requires a brand-new `ZfsManager::list_child_datasets` method (the reconciler keeps no in-memory record of which volumes exist, so ZFS enumeration is the only source of truth).

**Tech Stack:** Rust 2021 workspace. New dependencies, confined to the new `keel-dashboard` crate: `serde_json` (only for the `/api/snapshot` JSON boundary between the browser and `keel-dashboard`; every other wire format in this project stays YAML) and `base64` (already present in `Cargo.lock` as a transitive dependency of `rustls`, so this only promotes it to a direct dependency rather than adding a new compiled crate). No web framework, no JS build step, no async runtime.

## Global Constraints

- Match this project's established per-subsystem shape exactly: a plain trait, a `Fake*` in-memory implementation usable from any OS, and a real implementation gated to production usage - the same split `keel-jail`/`keel-net`/`keel-zfs`/`keel-ingress` already use.
- Every error enum uses `thiserror::Error`, one variant per real failure mode, mirroring `keel_spec::SpecError` / `keel_jail::JailError` / `keel-agentd::reconciler::ReconcileError`. Simple client/CLI-style functions that only need a human-readable message stay `Result<T, String>`, exactly like `keelctl`/`keel-agentd::registration`.
- TLS-loading code is duplicated per-binary rather than shared through a library, exactly like `keelctl/src/tls.rs` already duplicates (a strict subset of) `keel-controlplane/src/tls.rs`. `keel-dashboard/src/tls.rs` follows the same convention.
- Add `"keel-dashboard"` to the root `Cargo.toml`'s `[workspace] members` list before the first `cargo build`/`cargo test` that touches the new crate (Task 8).
- Run `cargo test --workspace` and `cargo clippy --workspace --all-targets -- -D warnings` after every task; both must be clean before committing (this project's established bar).
- Read-only: no task in this plan adds any apply/delete/force-repin path to `keel-dashboard`. `keelctl` remains the only way to mutate cluster state.

---

### Task 1: `keel-zfs`: `ZfsManager::list_child_datasets`

The reconciler's `get_volume`/`delete_volume` deliberately never consult `self.records` (a volume can outlive every jail record that ever referenced it), so there is no in-memory list of volume names anywhere in `keel-agentd` today. `Command::ListVolumes` (Task 2) has no data source to enumerate from unless `ZfsManager` itself gains a listing method - it currently has none (only `dataset_exists`, `clone_from_base`, `create_volume`, `destroy_dataset`, `snapshot`, `destroy_snapshot`, `send_snapshot`, `receive_snapshot`).

**Files:**
- Modify: `keel-zfs/src/lib.rs` (trait method)
- Modify: `keel-zfs/src/fake.rs` (fake implementation + tests)
- Modify: `keel-zfs/src/cli.rs` (real implementation, no in-process unit tests - `CliZfsManager` shells out to the real `zfs` binary and has no `#[cfg(test)]` module today; its correctness is exercised by real-VM verification, exactly like its sibling methods)

**Interfaces:**
- Produces: `ZfsManager::list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError>` - immediate children of `parent` only (not the whole subtree), sorted, full dataset paths (e.g. `"zroot/keel/volumes/web-data"`). `Ok(vec![])` if `parent` itself doesn't exist (mirrors `dataset_exists`'s tolerant handling of ZFS's exit code 1, rather than treating "no such dataset" as an error).
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests in `keel-zfs/src/fake.rs`**

Add to the `#[cfg(test)] mod tests` block:

```rust
#[test]
fn list_child_datasets_returns_only_immediate_children_sorted() {
    let zfs = FakeZfsManager::new();
    zfs.seed_dataset("zroot/keel/volumes/web-data");
    zfs.seed_dataset("zroot/keel/volumes/db-data");
    zfs.seed_dataset("zroot/keel/jails/web-1");
    assert_eq!(
        zfs.list_child_datasets("zroot/keel/volumes").unwrap(),
        vec!["zroot/keel/volumes/db-data".to_string(), "zroot/keel/volumes/web-data".to_string()]
    );
}

#[test]
fn list_child_datasets_excludes_grandchildren() {
    let zfs = FakeZfsManager::new();
    zfs.seed_dataset("zroot/keel/volumes/web-data");
    zfs.seed_dataset("zroot/keel/volumes/web-data/nested");
    assert_eq!(zfs.list_child_datasets("zroot/keel/volumes").unwrap(), vec!["zroot/keel/volumes/web-data".to_string()]);
}

#[test]
fn list_child_datasets_on_an_unseeded_parent_is_empty() {
    let zfs = FakeZfsManager::new();
    assert_eq!(zfs.list_child_datasets("zroot/keel/volumes").unwrap(), Vec::<String>::new());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-zfs list_child_datasets`
Expected: FAIL with "no method named `list_child_datasets` found"

- [ ] **Step 3: Add the trait method in `keel-zfs/src/lib.rs`**

Append inside `pub trait ZfsManager { ... }`, after `receive_snapshot`:

```rust

    /// Immediate children of `parent` only (not the whole subtree), sorted
    /// for deterministic output. `Ok(vec![])` if `parent` itself doesn't
    /// exist yet, mirroring `dataset_exists`'s tolerant handling of ZFS's
    /// "no such dataset" exit code rather than treating it as an error.
    fn list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError>;
```

- [ ] **Step 4: Implement it in `keel-zfs/src/fake.rs`**

Add inside `impl ZfsManager for FakeZfsManager { ... }`, after `receive_snapshot`:

```rust

    fn list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError> {
        let prefix = format!("{parent}/");
        let mut children: Vec<String> = self
            .datasets
            .lock()
            .unwrap()
            .iter()
            .filter(|name| name.starts_with(&prefix) && !name[prefix.len()..].contains('/'))
            .cloned()
            .collect();
        children.sort();
        Ok(children)
    }
```

- [ ] **Step 5: Implement it in `keel-zfs/src/cli.rs`**

Add inside `impl ZfsManager for CliZfsManager { ... }`, after `receive_snapshot`:

```rust

    fn list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError> {
        let output = Self::run(&["list", "-H", "-o", "name", "-r", parent])?;
        if !output.status.success() {
            if output.status.code() == Some(1) {
                return Ok(Vec::new());
            }
            return Err(ZfsError::CommandFailed(
                format!("zfs list -H -o name -r {parent}"),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let prefix = format!("{parent}/");
        let mut children: Vec<String> = stdout
            .lines()
            .map(|line| line.trim().to_string())
            .filter(|name| name.starts_with(&prefix) && !name[prefix.len()..].contains('/'))
            .collect();
        children.sort();
        Ok(children)
    }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p keel-zfs`
Expected: PASS, all tests green

- [ ] **Step 7: Commit**

```bash
git add keel-zfs/src/lib.rs keel-zfs/src/fake.rs keel-zfs/src/cli.rs
git commit -m "feat(keel-zfs): add ZfsManager::list_child_datasets"
```

---

### Task 2: `keel-agentd`: `Command::ListVolumes` and `GET /volumes`

**Files:**
- Modify: `keel-agentd/src/reconciler.rs` (`Reconciler::list_volumes`, tests)
- Modify: `keel-agentd/src/worker.rs` (`Command::ListVolumes` variant + handler, tests)
- Modify: `keel-agentd/src/http.rs` (`handle_list_volumes` + route, tests)

**Interfaces:**
- Consumes: `keel_zfs::ZfsManager::list_child_datasets` (Task 1); `record::volume_dataset_path` (existing); `crate::wire::VolumeStatus { name: String }` (existing).
- Produces: `Reconciler::list_volumes(&self) -> Result<Vec<crate::wire::VolumeStatus>, ReconcileError>`; `worker::Command::ListVolumes(Sender<Result<Vec<crate::wire::VolumeStatus>, ReconcileError>>)`; route `GET /volumes` returning `Vec<VolumeStatus>` as YAML (200), or the existing `status_for_error`-mapped error response.

- [ ] **Step 1: Write the failing test in `keel-agentd/src/reconciler.rs`**

Add near the existing `get_volume`/`delete_volume` tests (search for `fn get_volume_reports_existence`):

```rust
#[test]
fn list_volumes_returns_every_volume_dataset_sorted_by_name() {
    let zfs = FakeZfsManager::new();
    zfs.seed_dataset("zroot/keel/volumes/web-data");
    zfs.seed_dataset("zroot/keel/volumes/db-data");
    zfs.seed_dataset("zroot/keel/jails/web-1");
    let reconciler = Reconciler::new(
        FakeJailRuntime::new(),
        zfs,
        FakeNetManager::new(),
        FakeMountManager::new(),
        "zroot".to_string(),
        test_state_dir("list_volumes_returns_every_volume_dataset_sorted_by_name"),
        Box::new(keel_ingress::FakeAcmeClient::new()),
        Box::new(keel_ingress::FakeDnsProvider::new()),
        Box::new(crate::nginx::FakeNginxController::new()),
        crate::ServiceVipSlot::new(),
    )
    .unwrap();
    let volumes = reconciler.list_volumes().unwrap();
    assert_eq!(volumes, vec![
        crate::wire::VolumeStatus { name: "db-data".to_string() },
        crate::wire::VolumeStatus { name: "web-data".to_string() },
    ]);
}

#[test]
fn list_volumes_on_a_pool_with_no_volumes_is_empty() {
    let reconciler = Reconciler::new(
        FakeJailRuntime::new(),
        FakeZfsManager::new(),
        FakeNetManager::new(),
        FakeMountManager::new(),
        "zroot".to_string(),
        test_state_dir("list_volumes_on_a_pool_with_no_volumes_is_empty"),
        Box::new(keel_ingress::FakeAcmeClient::new()),
        Box::new(keel_ingress::FakeDnsProvider::new()),
        Box::new(crate::nginx::FakeNginxController::new()),
        crate::ServiceVipSlot::new(),
    )
    .unwrap();
    assert_eq!(reconciler.list_volumes().unwrap(), vec![]);
}
```

Check the existing test module's helper name for a scratch state dir (search `fn test_state_dir` in `keel-agentd/src/reconciler.rs`'s test module); if it's named differently, use that name instead of `test_state_dir` above - the point is a fresh temp directory per test, matching every other test in that module.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-agentd list_volumes`
Expected: FAIL with "no method named `list_volumes` found"

- [ ] **Step 3: Implement `Reconciler::list_volumes` in `keel-agentd/src/reconciler.rs`**

Add right after `pub fn delete_volume`:

```rust

    /// Every volume on this node, discovered by enumerating ZFS datasets
    /// under `<pool>/keel/volumes/` rather than consulting `self.records` -
    /// see `get_volume`'s doc comment: a volume can outlive every jail
    /// record that ever referenced it, so `self.records` is never a
    /// complete list of volumes.
    pub fn list_volumes(&self) -> Result<Vec<crate::wire::VolumeStatus>, ReconcileError> {
        let parent = format!("{}/keel/volumes", self.pool);
        let prefix = format!("{parent}/");
        let mut names: Vec<String> =
            self.zfs.list_child_datasets(&parent)?.into_iter().map(|dataset| dataset[prefix.len()..].to_string()).collect();
        names.sort();
        Ok(names.into_iter().map(|name| crate::wire::VolumeStatus { name }).collect())
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p keel-agentd list_volumes`
Expected: PASS

- [ ] **Step 5: Write the failing test in `keel-agentd/src/worker.rs`**

`spawn_test_worker(name: &str) -> Sender<Command>` (already defined right before `get_volume_and_delete_volume_commands_round_trip`) builds its own internal `FakeZfsManager` and doesn't expose it, exactly like `keel-agentd/src/http.rs`'s `start_test_server`. So, exactly as Task 2's later `keel-agentd/src/http.rs` test does, provision the volume the production way: `Command::Apply` a `JailSpec` whose `volumes` list names one. Add right after `get_volume_and_delete_volume_commands_round_trip`:

```rust
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
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cargo test -p keel-agentd list_volumes_command`
Expected: FAIL with "no variant named `ListVolumes` found"

- [ ] **Step 7: Add the `Command` variant and handler in `keel-agentd/src/worker.rs`**

Add to `pub enum Command { ... }`, right after `DeleteVolume(String, Sender<Result<(), ReconcileError>>),`:

```rust
    ListVolumes(Sender<Result<Vec<crate::wire::VolumeStatus>, ReconcileError>>),
```

Add a matching arm in `handle_command`'s `match command { ... }`, right after the `Command::DeleteVolume` arm:

```rust
        Command::ListVolumes(reply) => {
            let _ = reply.send(reconciler.list_volumes());
        }
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cargo test -p keel-agentd`
Expected: PASS, all tests green

- [ ] **Step 9: Write the failing test in `keel-agentd/src/http.rs`**

`start_test_server(name: &str) -> PathBuf` (already defined in this file's test module, right before `sample_spec_yaml_with_volume`) builds its own internal `FakeZfsManager` and doesn't expose it, so the existing `/volumes/{name}` tests provision a volume the same way production code would: `PUT` a jail spec whose `volumes:` list names one, via the existing `sample_spec_yaml_with_volume(jail_name, volume_name)` helper. Add near `get_volume_on_a_provisioned_volume_returns_200`:

```rust
#[test]
fn get_volumes_lists_every_provisioned_volume() {
    let socket_path = start_test_server("get_volumes_lists_every_provisioned_volume");
    send_request(&socket_path, "PUT", "/jails/web-1", &sample_spec_yaml_with_volume("web-1", "web-data"));

    let (status, body) = send_request(&socket_path, "GET", "/volumes", "");
    assert_eq!(status, 200);
    assert!(body.contains("web-data"), "got: {body}");
}

#[test]
fn get_volumes_on_a_pool_with_no_volumes_returns_an_empty_list() {
    let socket_path = start_test_server("get_volumes_on_a_pool_with_no_volumes_returns_an_empty_list");
    let (status, body) = send_request(&socket_path, "GET", "/volumes", "");
    assert_eq!(status, 200);
    assert_eq!(body.trim(), "[]");
}
```

- [ ] **Step 10: Run tests to verify they fail**

Run: `cargo test -p keel-agentd get_volumes`
Expected: FAIL with 404 (no route for GET /volumes)

- [ ] **Step 11: Add the route and handler in `keel-agentd/src/http.rs`**

Add a `handle_list_volumes` function near `handle_get_volume`:

```rust
fn handle_list_volumes(commands: &Sender<Command>) -> (u16, Vec<u8>) {
    let (reply_tx, reply_rx) = mpsc::channel();
    if commands.send(Command::ListVolumes(reply_tx)).is_err() {
        return error_response(500, "reconciler worker is not running".to_string());
    }
    match reply_rx.recv() {
        Ok(Ok(volumes)) => yaml_response(200, &volumes),
        Ok(Err(e)) => error_response(status_for_error(&e), e.to_string()),
        Err(_) => error_response(500, "reconciler worker did not respond".to_string()),
    }
}
```

Add a route arm in `fn route(...)`, right before `("GET", ["volumes", name]) => handle_get_volume(name, commands),`:

```rust
        ("GET", ["volumes"]) => handle_list_volumes(commands),
```

- [ ] **Step 12: Run tests to verify they pass**

Run: `cargo test -p keel-agentd`
Expected: PASS, all tests green

- [ ] **Step 13: Commit**

```bash
git add keel-agentd/src/reconciler.rs keel-agentd/src/worker.rs keel-agentd/src/http.rs
git commit -m "feat(keel-agentd): add Command::ListVolumes and GET /volumes"
```

---

### Task 3: `keel-controlplane`: `GET /nodes/{id}/volumes` forwarding

**Files:**
- Modify: `keel-controlplane/src/http.rs` (route + test)

**Interfaces:**
- Consumes: `handle_forward` (existing, same helper `GET /nodes/{id}/jails` already uses); `keel-agentd`'s new `GET /volumes` (Task 2).
- Produces: route `GET /nodes/{id}/volumes` forwarding to the target node's `GET /volumes`.

- [ ] **Step 1: Write the failing test**

Add right after `get_node_volume_forwards_to_the_right_node` (search for it in this file), reusing the same `start_test_server`/`start_fake_remote_tls_agentd`/`register_node`/`send_request` helpers that test and its neighbors already use:

```rust
#[test]
fn get_nodes_id_volumes_forwards_to_the_nodes_list_route() {
    let cp_addr = start_test_server();
    let node_addr = start_fake_remote_tls_agentd(200, "- name: web-data\n");
    register_node(&cp_addr, "node-1", &node_addr);

    let (status, body) = send_request(&cp_addr, "GET", "/nodes/node-1/volumes", "");
    assert_eq!(status, 200);
    assert!(body.contains("web-data"), "expected relayed body, got: {body}");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p keel-controlplane get_nodes_id_volumes_forwards`
Expected: FAIL with 404 (no route for GET /nodes/node-1/volumes)

- [ ] **Step 3: Add the route in `keel-controlplane/src/http.rs`**

Add in `fn route(...)`, right before `("GET", ["nodes", id, "volumes", name]) => { ... }`:

```rust
        ("GET", ["nodes", id, "volumes"]) => handle_forward(id, "GET", "/volumes", &[], commands, client_config),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p keel-controlplane`
Expected: PASS, all tests green

- [ ] **Step 5: Commit**

```bash
git add keel-controlplane/src/http.rs
git commit -m "feat(keel-controlplane): forward GET /nodes/{id}/volumes to the node's volumes list"
```

---

### Task 4: `keel-controlplane::wire`: `IngressHealth`, `Heartbeat.ingresses`, `NodeStatus.ingresses`

**Files:**
- Modify: `keel-controlplane/src/wire.rs`

**Interfaces:**
- Produces: `pub struct IngressHealth { pub name: String, pub host: String, pub backend_service: String, pub backend_port: u16, pub cert_expires_at_unix: Option<i64> }`; `Heartbeat.ingresses: Vec<IngressHealth>` (`#[serde(default)]`); `NodeStatus.ingresses: Vec<IngressHealth>`.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` block in `keel-controlplane/src/wire.rs`:

```rust
#[test]
fn ingress_health_round_trips_through_yaml() {
    let health = IngressHealth {
        name: "blog".to_string(),
        host: "example.com".to_string(),
        backend_service: "hugo-site".to_string(),
        backend_port: 8080,
        cert_expires_at_unix: Some(1_800_000_000),
    };
    let yaml = serde_yaml::to_string(&health).unwrap();
    let parsed: IngressHealth = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(parsed, health);
}

#[test]
fn heartbeat_with_ingresses_round_trips_through_yaml() {
    let heartbeat = Heartbeat {
        committed_cpu: 2.0,
        committed_memory: 1024 * 1024 * 1024,
        jails: vec![],
        ingresses: vec![IngressHealth {
            name: "blog".to_string(),
            host: "example.com".to_string(),
            backend_service: "hugo-site".to_string(),
            backend_port: 8080,
            cert_expires_at_unix: None,
        }],
    };
    let yaml = serde_yaml::to_string(&heartbeat).unwrap();
    let parsed: Heartbeat = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(parsed, heartbeat);
}

#[test]
fn heartbeat_without_an_ingresses_field_defaults_to_empty() {
    let parsed: Heartbeat = serde_yaml::from_str("committed_cpu: 1\ncommitted_memory: 2\n").unwrap();
    assert_eq!(parsed.ingresses, vec![]);
}

#[test]
fn node_status_with_ingresses_round_trips_through_yaml() {
    let status = NodeStatus {
        id: "node-1".to_string(),
        addr: "192.168.64.4".to_string(),
        pod_cidr: "10.0.4.0/24".to_string(),
        status: NodeState::Alive,
        last_seen_secs: 3,
        capacity_cpu: 4.0,
        capacity_memory: 8 * 1024 * 1024 * 1024,
        committed_cpu: 1.5,
        committed_memory: 512 * 1024 * 1024,
        ingresses: vec![IngressHealth {
            name: "blog".to_string(),
            host: "example.com".to_string(),
            backend_service: "hugo-site".to_string(),
            backend_port: 8080,
            cert_expires_at_unix: Some(1_800_000_000),
        }],
    };
    let yaml = serde_yaml::to_string(&status).unwrap();
    let parsed: NodeStatus = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(parsed, status);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-controlplane ingress_health`
Expected: FAIL to compile (`IngressHealth` doesn't exist; `Heartbeat`/`NodeStatus` have no `ingresses` field)

- [ ] **Step 3: Add the struct and fields in `keel-controlplane/src/wire.rs`**

Add `ingresses` to `Heartbeat`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Heartbeat {
    pub committed_cpu: f64,
    pub committed_memory: u64,
    #[serde(default)]
    pub jails: Vec<JailHealth>,
    #[serde(default)]
    pub ingresses: Vec<IngressHealth>,
}
```

Add the new struct right after `JailHealth`:

```rust

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngressHealth {
    pub name: String,
    pub host: String,
    pub backend_service: String,
    pub backend_port: u16,
    pub cert_expires_at_unix: Option<i64>,
}
```

Add `ingresses` to `NodeStatus`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: String,
    pub addr: String,
    pub pod_cidr: String,
    pub status: NodeState,
    pub last_seen_secs: u64,
    pub capacity_cpu: f64,
    pub capacity_memory: u64,
    pub committed_cpu: f64,
    pub committed_memory: u64,
    pub ingresses: Vec<IngressHealth>,
}
```

- [ ] **Step 4: Fix the now-broken existing struct literals**

`cargo build -p keel-controlplane` will now fail everywhere a `Heartbeat` or `NodeStatus` literal was constructed without `ingresses`. Fix each:

- `keel-controlplane/src/wire.rs`: in `heartbeat_round_trips_through_yaml`, add `jails: vec![], ingresses: vec![]` (it currently has neither field written out - add both if missing); in `heartbeat_with_jails_round_trips_through_yaml`, append `ingresses: vec![]` to the `Heartbeat { ... }` literal; in `node_status_round_trips_through_yaml`, append `ingresses: vec![]` to the `NodeStatus { ... }` literal.
- `keel-controlplane/src/registry.rs`: every `NodeStatus { ... }` test literal and the one inside `Registry::list` (handled in Task 5, skip here).
- Run `cargo build --workspace 2>&1 | grep "missing field"` to find every remaining call site across `keel-agentd` and `keelctl` test code; append `ingresses: vec![]` to each.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-controlplane`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(keel-controlplane): add IngressHealth and Heartbeat/NodeStatus.ingresses"
```

---

### Task 5: `keel-controlplane::registry`: thread `ingresses` through `NodeRecord`/`heartbeat`/`list`

**Files:**
- Modify: `keel-controlplane/src/registry.rs`

**Interfaces:**
- Consumes: `crate::wire::IngressHealth` (Task 4).
- Produces: `Registry::heartbeat(&mut self, id: &str, committed_cpu: f64, committed_memory: u64, jails: Vec<JailHealth>, ingresses: Vec<IngressHealth>, now: Instant) -> Result<(), UnknownNode>` (one new parameter, inserted right after `jails`); `Registry::list` now populates `NodeStatus.ingresses`.

- [ ] **Step 1: Write the failing test**

Add near `heartbeat_records_per_jail_running_status`:

```rust
#[test]
fn heartbeat_records_ingress_health_and_list_reports_it() {
    let mut registry = Registry::new("10.0.0.0/16".parse().unwrap());
    let now = Instant::now();
    registry.register("node-1".to_string(), "192.168.64.4:7621".to_string(), None, 4.0, 8 * 1024 * 1024 * 1024, now).unwrap();
    let ingress = crate::wire::IngressHealth {
        name: "blog".to_string(),
        host: "example.com".to_string(),
        backend_service: "hugo-site".to_string(),
        backend_port: 8080,
        cert_expires_at_unix: Some(1_800_000_000),
    };
    registry.heartbeat("node-1", 1.0, 512 * 1024 * 1024, vec![], vec![ingress.clone()], now).unwrap();
    let statuses = registry.list(now);
    assert_eq!(statuses[0].ingresses, vec![ingress]);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p keel-controlplane heartbeat_records_ingress_health`
Expected: FAIL to compile (`heartbeat` takes 5 args including `now`, not 6)

- [ ] **Step 3: Thread `ingresses` through `NodeRecord`/`heartbeat`/`list`**

Add a field to `NodeRecord`:

```rust
struct NodeRecord {
    addr: String,
    replicate_addr: Option<String>,
    last_heartbeat: Instant,
    capacity_cpu: f64,
    capacity_memory: u64,
    committed_cpu: f64,
    committed_memory: u64,
    pod_cidr: Ipv4Net,
    running_jails: HashMap<String, bool>,
    ingresses: Vec<crate::wire::IngressHealth>,
}
```

Initialize it in `register`'s `NodeRecord { ... }` literal:

```rust
                running_jails: HashMap::new(),
                ingresses: Vec::new(),
```

Update `heartbeat`'s signature and body:

```rust
    pub fn heartbeat(
        &mut self,
        id: &str,
        committed_cpu: f64,
        committed_memory: u64,
        jails: Vec<crate::wire::JailHealth>,
        ingresses: Vec<crate::wire::IngressHealth>,
        now: Instant,
    ) -> Result<(), UnknownNode> {
        match self.nodes.get_mut(id) {
            Some(record) => {
                record.last_heartbeat = now;
                record.committed_cpu = committed_cpu;
                record.committed_memory = committed_memory;
                record.running_jails = jails.into_iter().map(|j| (j.name, j.running)).collect();
                record.ingresses = ingresses;
                Ok(())
            }
            None => Err(UnknownNode(id.to_string())),
        }
    }
```

Update `list`'s `NodeStatus { ... }` literal to add:

```rust
                    ingresses: record.ingresses.clone(),
```

- [ ] **Step 4: Fix existing `heartbeat(...)` call sites in this file's tests**

Every existing test in `keel-controlplane/src/registry.rs` that calls `registry.heartbeat(...)` now needs a `vec![]` inserted as the new `ingresses` argument, right before the trailing `now`. Search for `.heartbeat(` in this file's test module and insert `vec![], ` before each `now)` (or `now,)` depending on formatting) argument.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-controlplane`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-controlplane/src/registry.rs
git commit -m "feat(keel-controlplane): thread ingress health through Registry"
```

---

### Task 6: `keel-controlplane::worker`/`http`: thread `ingresses` through `Command::Heartbeat`

**Files:**
- Modify: `keel-controlplane/src/worker.rs`
- Modify: `keel-controlplane/src/http.rs`

**Interfaces:**
- Consumes: `Registry::heartbeat` (Task 5).
- Produces: `Command::Heartbeat(String, f64, u64, Vec<JailHealth>, Vec<IngressHealth>, Sender<Result<(), UnknownNode>>)` (one new field, inserted right before the reply sender); `handle_heartbeat` now passes `heartbeat.ingresses` through.

- [ ] **Step 1: Write the failing test in `keel-controlplane/src/http.rs`**

Add right after `heartbeat_on_a_registered_node_returns_200` (search for it in this file), reusing the same `start_test_server`/`send_request` helpers:

```rust
#[test]
fn post_heartbeat_with_ingresses_is_reflected_in_get_nodes() {
    let addr = start_test_server();
    send_request(
        &addr,
        "POST",
        "/nodes/register",
        "id: node-1\naddr: 192.168.64.4:7621\ncapacity_cpu: 4\ncapacity_memory: 8589934592\n",
    );
    let (status, _) = send_request(
        &addr,
        "POST",
        "/nodes/node-1/heartbeat",
        "committed_cpu: 1\ncommitted_memory: 2\ningresses:\n  - name: blog\n    host: example.com\n    backend_service: hugo-site\n    backend_port: 8080\n    cert_expires_at_unix: 1800000000\n",
    );
    assert_eq!(status, 200);
    let (_, body) = send_request(&addr, "GET", "/nodes", "");
    assert!(body.contains("backend_service: hugo-site"), "got: {body}");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p keel-controlplane post_heartbeat_with_ingresses`
Expected: FAIL to compile (`Command::Heartbeat` has 5 fields, not 6)

- [ ] **Step 3: Update the `Command` variant in `keel-controlplane/src/worker.rs`**

```rust
    Heartbeat(String, f64, u64, Vec<crate::wire::JailHealth>, Vec<crate::wire::IngressHealth>, Sender<Result<(), UnknownNode>>),
```

Update the matching arm in `handle_command`:

```rust
        Command::Heartbeat(id, committed_cpu, committed_memory, jails, ingresses, reply) => {
            let result = registry.heartbeat(&id, committed_cpu, committed_memory, jails, ingresses, Instant::now());
            let _ = reply.send(result);
        }
```

- [ ] **Step 4: Update `handle_heartbeat` in `keel-controlplane/src/http.rs`**

```rust
    if commands
        .send(Command::Heartbeat(
            id.to_string(),
            heartbeat.committed_cpu,
            heartbeat.committed_memory,
            heartbeat.jails,
            heartbeat.ingresses,
            reply_tx,
        ))
        .is_err()
    {
        return error_response(500, "control plane worker is not running".to_string());
    }
```

- [ ] **Step 5: Fix existing `Command::Heartbeat(...)` call sites**

In `keel-controlplane/src/worker.rs`'s test module, every existing `commands.send(Command::Heartbeat(...))` call (search `Command::Heartbeat(`) needs a `vec![]` inserted as the new 5th positional field, right before the reply sender. For example:

```rust
commands.send(Command::Heartbeat("missing".to_string(), 0.0, 0, vec![], hb_tx)).unwrap();
```

becomes:

```rust
commands.send(Command::Heartbeat("missing".to_string(), 0.0, 0, vec![], vec![], hb_tx)).unwrap();
```

Apply the same fix to every other `Command::Heartbeat(...)` call in that test module.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p keel-controlplane`
Expected: PASS, all tests green

- [ ] **Step 7: Run the full workspace to check for other break sites**

Run: `cargo test --workspace`
Expected: PASS. If `keel-agentd` fails to compile here, that's expected and fixed in Task 7 - do not fix it in this task.

- [ ] **Step 8: Commit**

```bash
git add keel-controlplane/src/worker.rs keel-controlplane/src/http.rs
git commit -m "feat(keel-controlplane): thread ingress health through Command::Heartbeat"
```

---

### Task 7: `keel-agentd::registration`: gather ingresses into every heartbeat

**Files:**
- Modify: `keel-agentd/src/registration.rs` (`heartbeat_once`, `spawn`, tests)
- Modify: `keel-agentd/src/main.rs` (the `registration::spawn` call site)

**Interfaces:**
- Consumes: `keel_agentd::ingress_store::load_all(state_dir: &Path) -> Result<Vec<IngressRecord>, StoreError>` (existing); `keel_controlplane::wire::IngressHealth` (Task 4).
- Produces: `registration::spawn(..., state_dir: PathBuf, ...)` (one new parameter); `heartbeat_once(..., state_dir: &Path) -> Result<Vec<ServiceProxyEntry>, String>` (one new parameter).

- [ ] **Step 1: Write the failing test**

Add right after `heartbeats_report_the_reconcilers_committed_resources` in `keel-agentd/src/registration.rs`'s test module, copying that test's exact fixture shape (`start_test_control_plane()`, a `Reconciler`/`worker::spawn` pair, then `registration::spawn(...)`, then polling `get_nodes(&control_plane_addr)` the same way that test polls for `committed_cpu`) but seeding an ingress record on disk first and asserting on the ingress fields instead:

```rust
#[test]
fn heartbeats_report_ingress_health_gathered_from_the_ingress_store() {
    let state_dir = std::env::temp_dir().join("keel-agentd-registration-test-heartbeats_report_ingress_health");
    let _ = std::fs::remove_dir_all(&state_dir);
    let record = crate::IngressRecord {
        spec: keel_spec::IngressSpec {
            api_version: "keel/v1".to_string(),
            kind: "Ingress".to_string(),
            metadata: keel_spec::Metadata { name: "blog".to_string() },
            spec: keel_spec::IngressSpecBody {
                host: "example.com".to_string(),
                backend: keel_spec::IngressBackend { service: "hugo-site".to_string(), port: 8080 },
                tls: keel_spec::IngressTls { email: "admin@example.com".to_string() },
            },
        },
        cert_expires_at_unix: Some(1_800_000_000),
    };
    crate::ingress_store::save(&state_dir, &record).unwrap();

    let control_plane_addr = start_test_control_plane();
    let zfs = keel_zfs::FakeZfsManager::new();
    zfs.seed_dataset("zroot/keel/base/14.2-web");
    let reconciler = crate::Reconciler::new(
        keel_jail::FakeJailRuntime::new(),
        zfs.clone(),
        keel_net::FakeNetManager::new(),
        keel_jail::FakeMountManager::new(),
        "zroot".to_string(),
        state_dir.clone(),
        Box::new(keel_ingress::FakeAcmeClient::new()),
        Box::new(keel_ingress::FakeDnsProvider::new()),
        Box::new(crate::nginx::FakeNginxController::new()),
        crate::ServiceVipSlot::new(),
    )
    .unwrap();
    let (_worker_handle, commands) = crate::worker::spawn(reconciler, zfs, "zroot".to_string());

    let _handle = spawn(
        "node-1".to_string(),
        "10.0.0.1".to_string(),
        "10.0.0.9:7622".to_string(),
        control_plane_addr.clone(),
        Duration::from_millis(50),
        state_dir,
        4.0,
        8 * 1024 * 1024 * 1024,
        node_reloading_tls(),
        commands,
        crate::PodCidrSlot::new(),
        crate::ServiceVipSlot::new(),
    );

    thread::sleep(Duration::from_millis(200));
    let body = get_nodes(&control_plane_addr);
    assert!(body.contains("backend_service: hugo-site"), "expected reported ingress health, got: {body}");
    assert!(body.contains("host: example.com"), "got: {body}");
    assert!(body.contains("cert_expires_at_unix: 1800000000"), "got: {body}");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p keel-agentd heartbeats_report_ingress_health`
Expected: FAIL to compile (`heartbeat_once` takes 4 args, not 5)

- [ ] **Step 3: Update `heartbeat_once` in `keel-agentd/src/registration.rs`**

```rust
fn heartbeat_once(
    control_plane_addr: &str,
    node_id: &str,
    commands: &Sender<crate::worker::Command>,
    client_config: &Arc<rustls::ClientConfig>,
    state_dir: &std::path::Path,
) -> Result<Vec<keel_controlplane::wire::ServiceProxyEntry>, String> {
    let (resources_tx, resources_rx) = std::sync::mpsc::channel();
    commands
        .send(crate::worker::Command::CommittedResources(resources_tx))
        .map_err(|_| "worker is not running".to_string())?;
    let (committed_cpu, committed_memory) = resources_rx.recv().map_err(|_| "worker did not respond".to_string())?;

    let (jails_tx, jails_rx) = std::sync::mpsc::channel();
    commands
        .send(crate::worker::Command::Get(None, jails_tx))
        .map_err(|_| "worker is not running".to_string())?;
    let statuses = jails_rx.recv().map_err(|_| "worker did not respond".to_string())?;
    let jails: Vec<keel_controlplane::wire::JailHealth> = statuses
        .into_iter()
        .map(|s| keel_controlplane::wire::JailHealth { name: s.record.spec.metadata.name, running: s.running })
        .collect();

    let ingress_records =
        crate::ingress_store::load_all(state_dir).map_err(|e| format!("failed to load ingress records: {e}"))?;
    let ingresses: Vec<keel_controlplane::wire::IngressHealth> = ingress_records
        .into_iter()
        .map(|r| keel_controlplane::wire::IngressHealth {
            name: r.spec.metadata.name,
            host: r.spec.spec.host,
            backend_service: r.spec.spec.backend.service,
            backend_port: r.spec.spec.backend.port,
            cert_expires_at_unix: r.cert_expires_at_unix,
        })
        .collect();

    let heartbeat = keel_controlplane::wire::Heartbeat { committed_cpu, committed_memory, jails, ingresses };
    let body = serde_yaml::to_string(&heartbeat).map_err(|e| format!("failed to serialize heartbeat: {e}"))?;
    let response_body = send_request(control_plane_addr, "POST", &format!("/nodes/{node_id}/heartbeat"), &body, client_config)?;
    serde_yaml::from_slice(&response_body).map_err(|e| format!("malformed heartbeat response: {e}"))
}
```

- [ ] **Step 4: Thread `state_dir` through `registration::spawn`**

Add `state_dir: PathBuf` as a new parameter (after `heartbeat_interval`, to sit next to the other loop-scoped config):

```rust
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    node_id: String,
    advertise_addr: String,
    replicate_addr: String,
    control_plane_addr: String,
    heartbeat_interval: Duration,
    state_dir: PathBuf,
    capacity_cpu: f64,
    capacity_memory: u64,
    reloading_tls: Arc<tls::ReloadingTls>,
    commands: Sender<crate::worker::Command>,
    pod_cidr_slot: crate::PodCidrSlot,
    service_vips: crate::ServiceVipSlot,
) -> JoinHandle<()> {
```

Add `use std::path::PathBuf;` to this file's imports if not already present.

Update the call to `heartbeat_once` inside the loop:

```rust
                match heartbeat_once(&control_plane_addr, &node_id, &commands, &client_config, &state_dir) {
```

- [ ] **Step 5: Update the call site in `keel-agentd/src/main.rs`**

Find `keel_agentd::registration::spawn(` and add `config.state_dir.clone(),` as a new argument, in the same position (right after `heartbeat_interval`, before `capacity_cpu`) as the parameter was added in Step 4. Read the surrounding lines first to match argument order exactly.

- [ ] **Step 6: Fix any other `heartbeat_once`/`registration::spawn` call sites in tests**

Search `keel-agentd/src/registration.rs`'s test module for other direct calls to `heartbeat_once` or `registration::spawn` and add a `&state_dir` / `state_dir` argument to each, using a fresh `std::env::temp_dir().join(...)` per test the same way `heartbeats_report_ingress_health_gathered_from_the_ingress_store` does.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p keel-agentd`
Expected: PASS, all tests green

- [ ] **Step 8: Run the full workspace**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS, clean. This closes out every remaining `Heartbeat`/`NodeStatus`/`Command::Heartbeat` call site left over from Tasks 4-6.

- [ ] **Step 9: Commit**

```bash
git add keel-agentd/src/registration.rs keel-agentd/src/main.rs
git commit -m "feat(keel-agentd): report ingress health in every heartbeat"
```

---

### Task 8: `keel-dashboard`: crate scaffold

**Files:**
- Modify: `Cargo.toml` (root workspace `members`)
- Create: `keel-dashboard/Cargo.toml`
- Create: `keel-dashboard/src/lib.rs`
- Create: `keel-dashboard/src/main.rs`

**Interfaces:**
- Produces: an empty, compiling `keel-dashboard` binary + library crate that later tasks add modules to.
- Consumes: nothing yet.

- [ ] **Step 1: Add the workspace member**

In the root `Cargo.toml`, change:

```toml
members = ["keel-spec", "keel-jail", "keel-zfs", "keel-net", "keel-agentd", "keelctl", "keel-controlplane", "keel-ingress"]
```

to:

```toml
members = ["keel-spec", "keel-jail", "keel-zfs", "keel-net", "keel-agentd", "keelctl", "keel-controlplane", "keel-ingress", "keel-dashboard"]
```

- [ ] **Step 2: Create `keel-dashboard/Cargo.toml`**

```toml
[package]
name = "keel-dashboard"
version = "0.1.0"
edition = "2021"

[dependencies]
keel-controlplane = { path = "../keel-controlplane" }
keel-agentd = { path = "../keel-agentd" }
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
serde_json = "1"
httparse = "1"
rustls = { version = "0.23", default-features = false, features = ["ring", "std"] }
rustls-pemfile = "2"
base64 = "0.22"

[dev-dependencies]
keel-jail = { path = "../keel-jail" }
keel-zfs = { path = "../keel-zfs" }
keel-net = { path = "../keel-net" }
keel-ingress = { path = "../keel-ingress" }
```

- [ ] **Step 3: Create `keel-dashboard/src/lib.rs`**

```rust
//! `keel-dashboard`: a read-only web dashboard for cluster state. An mTLS
//! client of `keel-controlplane` (polling into an in-memory `Snapshot`) and
//! its own Basic-Auth-protected, TLS-terminating HTTP server for browsers.
```

- [ ] **Step 4: Create `keel-dashboard/src/main.rs`**

```rust
fn main() {
    eprintln!("keel-dashboard: scaffold only, not yet implemented");
}
```

- [ ] **Step 5: Run the build to verify the scaffold compiles**

Run: `cargo build -p keel-dashboard`
Expected: PASS (builds an empty crate cleanly)

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml keel-dashboard
git commit -m "feat(keel-dashboard): scaffold new crate"
```

---

### Task 9: `keel-dashboard::tls`: mTLS client config + browser-facing server config

**Files:**
- Create: `keel-dashboard/src/tls.rs`
- Modify: `keel-dashboard/src/lib.rs` (`pub mod tls;`)

**Interfaces:**
- Produces: `tls::load_client_config(cert_path: &Path, key_path: &Path, ca_path: &Path, crl_path: &Path) -> Result<rustls::ClientConfig, String>` (mTLS, to the control plane - identical shape/behavior to `keelctl::tls::load_client_config`); `tls::load_browser_server_config(cert_path: &Path, key_path: &Path) -> Result<rustls::ServerConfig, String>` (single cert/key, no client-certificate verification - browsers don't present client certs, so this can't reuse `keel_controlplane::tls::load_server_config`, which hardcodes a `WebPkiClientVerifier`); `tls::server_name_from_addr(addr: &str) -> Result<rustls::pki_types::ServerName<'static>, String>`.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    #[test]
    fn load_client_config_succeeds_with_valid_fixtures() {
        load_client_config(&fixture("fixture-client.crt"), &fixture("fixture-client.key"), &fixture("ca.crt"), &fixture("crl.pem"))
            .expect("expected a valid client config");
    }

    #[test]
    fn load_client_config_fails_on_a_missing_cert_file() {
        let err = load_client_config(&fixture("does-not-exist.crt"), &fixture("fixture-client.key"), &fixture("ca.crt"), &fixture("crl.pem"))
            .unwrap_err();
        assert!(err.contains("does-not-exist.crt"), "got: {err}");
    }

    #[test]
    fn load_browser_server_config_succeeds_with_a_bare_cert_and_key_and_no_ca() {
        load_browser_server_config(&fixture("fixture-node.crt"), &fixture("fixture-node.key"))
            .expect("expected a valid server config with no client-cert verification");
    }

    #[test]
    fn load_browser_server_config_fails_on_a_missing_key_file() {
        let err = load_browser_server_config(&fixture("fixture-node.crt"), &fixture("does-not-exist.key")).unwrap_err();
        assert!(err.contains("does-not-exist.key"), "got: {err}");
    }

    #[test]
    fn server_name_from_addr_parses_the_host_and_drops_the_port() {
        let name = server_name_from_addr("192.168.64.4:7621").unwrap();
        assert_eq!(name, rustls::pki_types::ServerName::IpAddress(std::net::Ipv4Addr::new(192, 168, 64, 4).into()));
    }

    #[test]
    fn server_name_from_addr_rejects_a_non_ip_host() {
        assert!(server_name_from_addr("not-an-ip:7620").is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-dashboard`
Expected: FAIL to compile (module doesn't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/tls.rs`**

```rust
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName};
use rustls::RootCertStore;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, Once};

static CRYPTO_PROVIDER_INIT: Once = Once::new();

pub fn ensure_crypto_provider() {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// mTLS client config for talking to `keel-controlplane`, identical in
/// shape and behavior to `keelctl::tls::load_client_config` - this project
/// duplicates TLS-loading code per binary rather than sharing it through a
/// library (see `keelctl/src/tls.rs` vs. `keel-controlplane/src/tls.rs`).
pub fn load_client_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
    crl_path: &Path,
) -> Result<rustls::ClientConfig, String> {
    ensure_crypto_provider();
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let roots = load_root_store(ca_path)?;
    let crls = load_crls(crl_path)?;
    let server_verifier = WebPkiServerVerifier::builder(Arc::new(roots))
        .with_crls(crls)
        .build()
        .map_err(|e| format!("failed to build server certificate verifier: {e}"))?;
    rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(server_verifier)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("failed to build TLS client config: {e}"))
}

/// Server config for the browser-facing listener: a bare cert/key with no
/// client-certificate verification, since browsers never present one.
/// Deliberately distinct from `keel_controlplane::tls::load_server_config`,
/// which hardcodes a `WebPkiClientVerifier` for mTLS between cluster
/// components and would reject every browser connection outright.
pub fn load_browser_server_config(cert_path: &Path, key_path: &Path) -> Result<rustls::ServerConfig, String> {
    ensure_crypto_provider();
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("failed to build TLS server config: {e}"))
}

pub fn server_name_from_addr(addr: &str) -> Result<ServerName<'static>, String> {
    let host = addr.rsplit_once(':').map(|(host, _port)| host).unwrap_or(addr);
    let ip: std::net::IpAddr =
        host.parse().map_err(|e| format!("expected an IP address in '{addr}', got '{host}': {e}"))?;
    Ok(ServerName::IpAddress(ip.into()))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open certificate file {}: {e}", path.display()))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse certificate file {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("failed to find any PEM-encoded certificates in {}", path.display()));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open key file {}: {e}", path.display()))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| format!("failed to parse key file {}: {e}", path.display()))?
        .ok_or_else(|| format!("no private key found in {}", path.display()))
}

fn load_root_store(ca_path: &Path) -> Result<RootCertStore, String> {
    let certs = load_certs(ca_path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|e| format!("failed to add CA certificate from {}: {e}", ca_path.display()))?;
    }
    Ok(roots)
}

fn load_crls(path: &Path) -> Result<Vec<CertificateRevocationListDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open CRL file {}: {e}", path.display()))?;
    let crls: Vec<_> = rustls_pemfile::crls(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse CRL file {}: {e}", path.display()))?;
    if crls.is_empty() {
        return Err(format!("failed to find a PEM-encoded CRL in {}", path.display()));
    }
    Ok(crls)
}
```

- [ ] **Step 4: Wire the module into `keel-dashboard/src/lib.rs`**

```rust
pub mod tls;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/tls.rs keel-dashboard/src/lib.rs
git commit -m "feat(keel-dashboard): add TLS client and browser-server config loaders"
```

---

### Task 10: `keel-dashboard::control_plane_client`: `ControlPlaneClient` trait, `TlsControlPlaneClient`, `FakeControlPlaneClient`

**Files:**
- Create: `keel-dashboard/src/control_plane_client.rs`
- Modify: `keel-dashboard/src/lib.rs` (`pub mod control_plane_client;`)

**Interfaces:**
- Consumes: `tls::load_client_config`, `tls::server_name_from_addr` (Task 9); `keel_controlplane::wire::{NodeStatus, ServiceSummary, ServiceProxyEntry}`; `keel_agentd::JailStatus`; `keel_agentd::wire::VolumeStatus`.
- Produces: `pub trait ControlPlaneClient: Send + Sync { fn fetch_nodes(&self) -> Result<Vec<NodeStatus>, String>; fn fetch_jails(&self, node_id: &str) -> Result<Vec<JailStatus>, String>; fn fetch_volumes(&self, node_id: &str) -> Result<Vec<VolumeStatus>, String>; fn fetch_services(&self) -> Result<Vec<ServiceSummary>, String>; fn fetch_service(&self, name: &str) -> Result<ServiceProxyEntry, String>; }`; `pub struct TlsControlPlaneClient` (real, over TCP+rustls); `pub struct FakeControlPlaneClient` (in-memory, with `set_nodes`/`set_jails`/`set_volumes`/`set_services`/`set_service`/`fail_nodes`/`fail_jails`/`fail_volumes`/`fail_services`/`fail_service` test helpers).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use keel_controlplane::wire::NodeState;

    fn sample_node(id: &str) -> keel_controlplane::wire::NodeStatus {
        keel_controlplane::wire::NodeStatus {
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
    fn fake_returns_seeded_nodes() {
        let fake = FakeControlPlaneClient::new();
        fake.set_nodes(vec![sample_node("node-1")]);
        let nodes = fake.fetch_nodes().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "node-1");
    }

    #[test]
    fn fake_fetch_nodes_fails_once_marked_failing() {
        let fake = FakeControlPlaneClient::new();
        fake.set_nodes(vec![sample_node("node-1")]);
        fake.fail_nodes();
        assert!(fake.fetch_nodes().is_err());
    }

    #[test]
    fn fake_fetch_jails_fails_only_for_the_marked_node() {
        let fake = FakeControlPlaneClient::new();
        fake.set_jails("node-1", vec![]);
        fake.set_jails("node-2", vec![]);
        fake.fail_jails("node-1");
        assert!(fake.fetch_jails("node-1").is_err());
        assert!(fake.fetch_jails("node-2").is_ok());
    }

    #[test]
    fn fake_returns_seeded_service_detail() {
        let fake = FakeControlPlaneClient::new();
        let entry = keel_controlplane::wire::ServiceProxyEntry {
            name: "web".to_string(),
            vip: "10.0.250.7".to_string(),
            port: 8080,
            replicas: vec![],
        };
        fake.set_service("web", entry.clone());
        assert_eq!(fake.fetch_service("web").unwrap(), entry);
    }

    #[test]
    fn fake_fetch_service_on_an_unknown_name_fails() {
        let fake = FakeControlPlaneClient::new();
        assert!(fake.fetch_service("missing").is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-dashboard fake_`
Expected: FAIL to compile (module doesn't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/control_plane_client.rs`**

```rust
use keel_agentd::wire::VolumeStatus;
use keel_agentd::JailStatus;
use keel_controlplane::wire::{NodeStatus, ServiceProxyEntry, ServiceSummary};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub trait ControlPlaneClient: Send + Sync {
    fn fetch_nodes(&self) -> Result<Vec<NodeStatus>, String>;
    fn fetch_jails(&self, node_id: &str) -> Result<Vec<JailStatus>, String>;
    fn fetch_volumes(&self, node_id: &str) -> Result<Vec<VolumeStatus>, String>;
    fn fetch_services(&self) -> Result<Vec<ServiceSummary>, String>;
    fn fetch_service(&self, name: &str) -> Result<ServiceProxyEntry, String>;
}

/// The real client: an mTLS TCP connection per request, exactly the same
/// request/response shape as `keelctl`'s `send_request_tcp` and
/// `keel-agentd::registration`'s `send_request`.
pub struct TlsControlPlaneClient {
    addr: String,
    client_config: Arc<rustls::ClientConfig>,
}

impl TlsControlPlaneClient {
    pub fn new(addr: String, client_config: Arc<rustls::ClientConfig>) -> Self {
        Self { addr, client_config }
    }

    fn request(&self, method: &str, path: &str) -> Result<(u16, String), String> {
        let server_name = crate::tls::server_name_from_addr(&self.addr).map_err(|e| e.to_string())?;
        let tcp_stream = TcpStream::connect(&self.addr).map_err(|e| format!("failed to connect to {}: {e}", self.addr))?;
        let conn = rustls::ClientConnection::new(Arc::clone(&self.client_config), server_name).map_err(|e| e.to_string())?;
        let mut stream = rustls::StreamOwned::new(conn, tcp_stream);

        let request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        stream.write_all(request.as_bytes()).map_err(|e| format!("failed to send request: {e}"))?;
        stream.sock.shutdown(std::net::Shutdown::Write).ok();

        let mut response = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => response.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("failed to read response: {e}")),
            }
        }
        parse_response(&response)
    }

    fn get_yaml<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        let (status, body) = self.request("GET", path)?;
        if !(200..300).contains(&status) {
            return Err(format!("GET {path} returned status {status}: {body}"));
        }
        serde_yaml::from_str(&body).map_err(|e| format!("malformed response from GET {path}: {e}"))
    }
}

fn parse_response(response: &[u8]) -> Result<(u16, String), String> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Response::new(&mut headers);
    let header_len = match parsed.parse(response).map_err(|e| format!("malformed response: {e}"))? {
        httparse::Status::Complete(len) => len,
        httparse::Status::Partial => return Err("incomplete response from server".to_string()),
    };
    let status = parsed.code.unwrap_or(0);
    let content_length = parsed
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .and_then(|h| std::str::from_utf8(h.value).ok())
        .and_then(|v| v.trim().parse::<usize>().ok())
        .ok_or_else(|| "response missing Content-Length header".to_string())?;
    let actual = response.len() - header_len;
    if actual != content_length {
        return Err(format!("truncated response: expected {content_length} bytes, got {actual}"));
    }
    Ok((status, String::from_utf8_lossy(&response[header_len..]).to_string()))
}

impl ControlPlaneClient for TlsControlPlaneClient {
    fn fetch_nodes(&self) -> Result<Vec<NodeStatus>, String> {
        self.get_yaml("/nodes")
    }

    fn fetch_jails(&self, node_id: &str) -> Result<Vec<JailStatus>, String> {
        self.get_yaml(&format!("/nodes/{node_id}/jails"))
    }

    fn fetch_volumes(&self, node_id: &str) -> Result<Vec<VolumeStatus>, String> {
        self.get_yaml(&format!("/nodes/{node_id}/volumes"))
    }

    fn fetch_services(&self) -> Result<Vec<ServiceSummary>, String> {
        self.get_yaml("/services")
    }

    fn fetch_service(&self, name: &str) -> Result<ServiceProxyEntry, String> {
        self.get_yaml(&format!("/services/{name}"))
    }
}

/// In-memory test double: seed data with the `set_*` methods, simulate a
/// failed fetch with the `fail_*` methods. Mirrors `FakeZfsManager`'s
/// `Arc<Mutex<...>>`-backed, freely-`Clone`-able shape.
#[derive(Default, Clone)]
pub struct FakeControlPlaneClient {
    nodes: Arc<Mutex<Vec<NodeStatus>>>,
    jails: Arc<Mutex<HashMap<String, Vec<JailStatus>>>>,
    volumes: Arc<Mutex<HashMap<String, Vec<VolumeStatus>>>>,
    services: Arc<Mutex<Vec<ServiceSummary>>>,
    service_details: Arc<Mutex<HashMap<String, ServiceProxyEntry>>>,
    nodes_failing: Arc<Mutex<bool>>,
    failing_jail_nodes: Arc<Mutex<std::collections::HashSet<String>>>,
    failing_volume_nodes: Arc<Mutex<std::collections::HashSet<String>>>,
    services_failing: Arc<Mutex<bool>>,
    failing_services: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl FakeControlPlaneClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_nodes(&self, nodes: Vec<NodeStatus>) {
        *self.nodes.lock().unwrap() = nodes;
    }

    pub fn fail_nodes(&self) {
        *self.nodes_failing.lock().unwrap() = true;
    }

    pub fn set_jails(&self, node_id: &str, jails: Vec<JailStatus>) {
        self.jails.lock().unwrap().insert(node_id.to_string(), jails);
    }

    pub fn fail_jails(&self, node_id: &str) {
        self.failing_jail_nodes.lock().unwrap().insert(node_id.to_string());
    }

    pub fn set_volumes(&self, node_id: &str, volumes: Vec<VolumeStatus>) {
        self.volumes.lock().unwrap().insert(node_id.to_string(), volumes);
    }

    pub fn fail_volumes(&self, node_id: &str) {
        self.failing_volume_nodes.lock().unwrap().insert(node_id.to_string());
    }

    pub fn set_services(&self, services: Vec<ServiceSummary>) {
        *self.services.lock().unwrap() = services;
    }

    pub fn fail_services(&self) {
        *self.services_failing.lock().unwrap() = true;
    }

    pub fn set_service(&self, name: &str, entry: ServiceProxyEntry) {
        self.service_details.lock().unwrap().insert(name.to_string(), entry);
    }

    pub fn fail_service(&self, name: &str) {
        self.failing_services.lock().unwrap().insert(name.to_string());
    }
}

impl ControlPlaneClient for FakeControlPlaneClient {
    fn fetch_nodes(&self) -> Result<Vec<NodeStatus>, String> {
        if *self.nodes_failing.lock().unwrap() {
            return Err("simulated control-plane unreachable".to_string());
        }
        Ok(self.nodes.lock().unwrap().clone())
    }

    fn fetch_jails(&self, node_id: &str) -> Result<Vec<JailStatus>, String> {
        if self.failing_jail_nodes.lock().unwrap().contains(node_id) {
            return Err(format!("simulated failure fetching jails for '{node_id}'"));
        }
        Ok(self.jails.lock().unwrap().get(node_id).cloned().unwrap_or_default())
    }

    fn fetch_volumes(&self, node_id: &str) -> Result<Vec<VolumeStatus>, String> {
        if self.failing_volume_nodes.lock().unwrap().contains(node_id) {
            return Err(format!("simulated failure fetching volumes for '{node_id}'"));
        }
        Ok(self.volumes.lock().unwrap().get(node_id).cloned().unwrap_or_default())
    }

    fn fetch_services(&self) -> Result<Vec<ServiceSummary>, String> {
        if *self.services_failing.lock().unwrap() {
            return Err("simulated failure fetching services".to_string());
        }
        Ok(self.services.lock().unwrap().clone())
    }

    fn fetch_service(&self, name: &str) -> Result<ServiceProxyEntry, String> {
        if self.failing_services.lock().unwrap().contains(name) {
            return Err(format!("simulated failure fetching service '{name}'"));
        }
        self.service_details.lock().unwrap().get(name).cloned().ok_or_else(|| format!("no such service '{name}'"))
    }
}
```

- [ ] **Step 4: Wire the module into `keel-dashboard/src/lib.rs`**

```rust
pub mod control_plane_client;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/control_plane_client.rs keel-dashboard/src/lib.rs
git commit -m "feat(keel-dashboard): add ControlPlaneClient trait with Tls/Fake implementations"
```

---

### Task 11: `keel-dashboard::snapshot`: `Snapshot` and partial-failure merge logic

**Files:**
- Create: `keel-dashboard/src/snapshot.rs`
- Modify: `keel-dashboard/src/lib.rs` (`pub mod snapshot;`)

**Interfaces:**
- Consumes: `control_plane_client::ControlPlaneClient` (Task 10).
- Produces: `pub struct Snapshot { pub nodes: Vec<NodeSnapshot>, pub services: Vec<ServiceSnapshot>, pub stale: bool, pub stale_as_of_unix: Option<i64> }` (derives `Default`, `Serialize`); `pub struct NodeSnapshot { pub status: NodeStatus, pub jails: Vec<JailStatus>, pub volumes: Vec<VolumeStatus>, pub data_stale: bool }`; `pub struct ServiceSnapshot { pub summary: ServiceSummary, pub detail: Option<ServiceProxyEntry>, pub data_stale: bool }`; `pub fn poll_once(client: &dyn ControlPlaneClient, previous: &Snapshot, now_unix: i64) -> Snapshot`.

- [ ] **Step 1: Write the failing tests**

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-dashboard poll_once`
Expected: FAIL to compile (module doesn't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/snapshot.rs`**

```rust
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
```

- [ ] **Step 4: Wire the module into `keel-dashboard/src/lib.rs`**

```rust
pub mod snapshot;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/snapshot.rs keel-dashboard/src/lib.rs
git commit -m "feat(keel-dashboard): add Snapshot merge logic with partial-failure staleness"
```

---

### Task 12: `keel-dashboard::poller`: background refresh thread

**Files:**
- Create: `keel-dashboard/src/poller.rs`
- Modify: `keel-dashboard/src/lib.rs` (`pub mod poller;`)

**Interfaces:**
- Consumes: `control_plane_client::ControlPlaneClient` (Task 10); `snapshot::{Snapshot, poll_once}` (Task 11).
- Produces: `pub fn spawn(client: Box<dyn ControlPlaneClient>, poll_interval: Duration) -> Arc<RwLock<Snapshot>>`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_client::FakeControlPlaneClient;
    use keel_controlplane::wire::{NodeState, NodeStatus};
    use std::time::Duration;

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p keel-dashboard spawn_populates_the_snapshot`
Expected: FAIL to compile (module doesn't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/poller.rs`**

```rust
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
```

- [ ] **Step 4: Wire the module into `keel-dashboard/src/lib.rs`**

```rust
pub mod poller;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/poller.rs keel-dashboard/src/lib.rs
git commit -m "feat(keel-dashboard): add background poller thread"
```

---

### Task 13: `keel-dashboard::basic_auth`: Basic Auth check

**Files:**
- Create: `keel-dashboard/src/basic_auth.rs`
- Modify: `keel-dashboard/src/lib.rs` (`pub mod basic_auth;`)

**Interfaces:**
- Produces: `pub fn check(header: Option<&str>, expected_user: &str, expected_password: &str) -> bool`.
- Consumes: `base64` crate.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    fn header_for(user: &str, password: &str) -> String {
        format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
    }

    #[test]
    fn correct_credentials_pass() {
        assert!(check(Some(&header_for("admin", "hunter2")), "admin", "hunter2"));
    }

    #[test]
    fn wrong_password_fails() {
        assert!(!check(Some(&header_for("admin", "wrong")), "admin", "hunter2"));
    }

    #[test]
    fn wrong_user_fails() {
        assert!(!check(Some(&header_for("someone-else", "hunter2")), "admin", "hunter2"));
    }

    #[test]
    fn missing_header_fails() {
        assert!(!check(None, "admin", "hunter2"));
    }

    #[test]
    fn non_basic_scheme_fails() {
        assert!(!check(Some("Bearer sometoken"), "admin", "hunter2"));
    }

    #[test]
    fn malformed_base64_fails() {
        assert!(!check(Some("Basic not-valid-base64!!"), "admin", "hunter2"));
    }

    #[test]
    fn missing_colon_separator_fails() {
        assert!(!check(Some(&format!("Basic {}", STANDARD.encode("no-colon-here"))), "admin", "hunter2"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-dashboard basic_auth`
Expected: FAIL to compile (module doesn't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/basic_auth.rs`**

```rust
use base64::{engine::general_purpose::STANDARD, Engine as _};

/// Checks a raw `Authorization` header value against the configured
/// username/password. Returns `false` uniformly for a missing header, a
/// non-`Basic` scheme, malformed base64/UTF-8, or a mismatched
/// user/password - the caller responds `401` in every case without
/// distinguishing why, so as not to leak which part of the credential was
/// wrong.
pub fn check(header: Option<&str>, expected_user: &str, expected_password: &str) -> bool {
    let Some(header) = header else { return false };
    let Some(encoded) = header.strip_prefix("Basic ") else { return false };
    let Ok(decoded_bytes) = STANDARD.decode(encoded) else { return false };
    let Ok(decoded) = String::from_utf8(decoded_bytes) else { return false };
    let Some((user, password)) = decoded.split_once(':') else { return false };
    user == expected_user && password == expected_password
}
```

- [ ] **Step 4: Wire the module into `keel-dashboard/src/lib.rs`**

```rust
pub mod basic_auth;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/basic_auth.rs keel-dashboard/src/lib.rs
git commit -m "feat(keel-dashboard): add HTTP Basic Auth check"
```

---

### Task 14: `keel-dashboard::html`: render the dashboard page

**Files:**
- Create: `keel-dashboard/src/html.rs`
- Modify: `keel-dashboard/src/lib.rs` (`pub mod html;`)

**Interfaces:**
- Consumes: `snapshot::{Snapshot, NodeSnapshot, ServiceSnapshot}` (Task 11).
- Produces: `pub fn render(snapshot: &Snapshot, now_unix: i64) -> String` (a complete HTML document, hand-written Rust string templates, no template engine - same style as `keel-ingress::config`'s nginx-config templating).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{NodeSnapshot, ServiceSnapshot};
    use keel_agentd::wire::VolumeStatus;
    use keel_agentd::{BackoffStatus, JailStatus};
    use keel_controlplane::wire::{IngressHealth, NodeState, NodeStatus, ServiceProxyEntry, ServiceReplica, ServiceSummary};

    fn sample_node_status() -> NodeStatus {
        NodeStatus {
            id: "node-1".to_string(),
            addr: "192.168.64.4:7621".to_string(),
            pod_cidr: "10.0.4.0/24".to_string(),
            status: NodeState::Alive,
            last_seen_secs: 3,
            capacity_cpu: 4.0,
            capacity_memory: 8 * 1024 * 1024 * 1024,
            committed_cpu: 1.5,
            committed_memory: 1024 * 1024 * 1024,
            ingresses: vec![IngressHealth {
                name: "blog".to_string(),
                host: "example.com".to_string(),
                backend_service: "hugo-site".to_string(),
                backend_port: 8080,
                cert_expires_at_unix: Some(1_000_000_000 + 10 * 24 * 60 * 60),
            }],
        }
    }

    fn jail_record(name: &str) -> keel_agentd::JailRecord {
        keel_agentd::JailRecord {
            spec: keel_spec::JailSpec {
                api_version: "keel/v1".to_string(),
                kind: "Jail".to_string(),
                metadata: keel_spec::Metadata { name: name.to_string() },
                spec: keel_spec::Spec {
                    image: "base/14.2-web".to_string(),
                    command: vec!["/usr/local/bin/myapp".to_string()],
                    network: keel_spec::NetworkSpec { vnet: true, bridge: "keel0".to_string(), address: "10.0.0.5/24".to_string() },
                    resources: keel_spec::ResourcesSpec { cpu: "1".to_string(), memory: "512M".to_string() },
                    restart_policy: keel_spec::RestartPolicy::Always,
                    volumes: vec![],
                    replicate_to: None,
                },
            },
            epair_ordinal: 0,
        }
    }

    #[test]
    fn renders_a_running_jail_as_running() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![JailStatus { record: jail_record("web-0"), running: true, backoff: BackoffStatus::default() }],
            volumes: vec![],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("web-0"), "got: {html}");
        assert!(html.contains("running"), "got: {html}");
    }

    #[test]
    fn renders_a_backed_off_non_running_jail_as_crash_looping() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![JailStatus {
                record: jail_record("web-0"),
                running: false,
                backoff: BackoffStatus { retry_in_secs: Some(4), current_delay_secs: Some(8) },
            }],
            volumes: vec![],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("crash-looping"), "got: {html}");
    }

    #[test]
    fn renders_a_freshly_applied_non_running_jail_as_not_running_not_crash_looping() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![JailStatus { record: jail_record("web-0"), running: false, backoff: BackoffStatus::default() }],
            volumes: vec![],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(!html.contains("crash-looping"), "got: {html}");
    }

    #[test]
    fn renders_a_volume_name_grouped_under_its_node() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![],
            volumes: vec![VolumeStatus { name: "web-data".to_string() }],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("web-data"), "got: {html}");
    }

    #[test]
    fn renders_a_service_with_desired_and_actual_replica_counts() {
        let service = ServiceSnapshot {
            summary: ServiceSummary { name: "web".to_string(), desired_replicas: 3, vip: "10.0.250.7".to_string(), port: 8080 },
            detail: Some(ServiceProxyEntry {
                name: "web".to_string(),
                vip: "10.0.250.7".to_string(),
                port: 8080,
                replicas: vec![ServiceReplica { name: "web-0".to_string(), node: "node-1".to_string(), address: "10.0.4.5".to_string() }],
            }),
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![], services: vec![service], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("10.0.250.7"), "got: {html}");
        assert!(html.contains("web-0"), "got: {html}");
    }

    #[test]
    fn renders_a_cert_expiry_warning_inside_the_thirty_day_threshold() {
        let mut node_status = sample_node_status();
        node_status.ingresses[0].cert_expires_at_unix = Some(1_000_000_000 + 20 * 24 * 60 * 60);
        let node = NodeSnapshot { status: node_status, jails: vec![], volumes: vec![], data_stale: false };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("expiry-warning"), "got: {html}");
    }

    #[test]
    fn does_not_render_a_cert_expiry_warning_outside_the_threshold() {
        let mut node_status = sample_node_status();
        node_status.ingresses[0].cert_expires_at_unix = Some(1_000_000_000 + 60 * 24 * 60 * 60);
        let node = NodeSnapshot { status: node_status, jails: vec![], volumes: vec![], data_stale: false };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(!html.contains("expiry-warning"), "got: {html}");
    }

    #[test]
    fn renders_a_stale_banner_when_the_snapshot_is_stale() {
        let snapshot = crate::snapshot::Snapshot { nodes: vec![], services: vec![], stale: true, stale_as_of_unix: Some(1_000_000_000) };
        let html = render(&snapshot, 1_000_000_500);
        assert!(html.contains("stale"), "got: {html}");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-dashboard render`
Expected: FAIL to compile (module doesn't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/html.rs`**

```rust
use crate::snapshot::{NodeSnapshot, ServiceSnapshot, Snapshot};

/// Must be kept in sync with `keel-agentd::reconciler::reconcile_certs`'s
/// own `RENEWAL_THRESHOLD_SECS` - this project duplicates the constant
/// rather than sharing it, exactly like its TLS-loading code is duplicated
/// per binary (see `keel-dashboard/src/tls.rs`'s doc comment).
const RENEWAL_THRESHOLD_SECS: i64 = 30 * 24 * 60 * 60;

pub fn render(snapshot: &Snapshot, now_unix: i64) -> String {
    format!(
        "<!doctype html>\n<html><head><title>keel dashboard</title><meta http-equiv=\"refresh\" content=\"5\">\
         <style>body{{font-family:sans-serif;margin:2em}}table{{border-collapse:collapse;margin-bottom:2em}}\
         td,th{{border:1px solid #ccc;padding:4px 8px;text-align:left}}.stale{{background:#fee;padding:8px;margin-bottom:1em}}\
         .expiry-warning{{color:#a00;font-weight:bold}}</style></head><body>\n\
         {stale_banner}<h1>keel dashboard</h1>\n{nodes}\n{jails}\n{services}\n{volumes}\n{ingress}\n</body></html>\n",
        stale_banner = render_stale_banner(snapshot, now_unix),
        nodes = render_nodes(&snapshot.nodes),
        jails = render_jails(&snapshot.nodes),
        services = render_services(&snapshot.services),
        volumes = render_volumes(&snapshot.nodes),
        ingress = render_ingress(&snapshot.nodes, now_unix),
    )
}

fn render_stale_banner(snapshot: &Snapshot, now_unix: i64) -> String {
    if !snapshot.stale {
        return String::new();
    }
    let as_of = snapshot.stale_as_of_unix.unwrap_or(now_unix);
    format!("<div class=\"stale\">stale as of unix time {as_of}: control plane unreachable, showing the last known state</div>")
}

fn render_nodes(nodes: &[NodeSnapshot]) -> String {
    let mut rows = String::new();
    for node in nodes {
        let s = &node.status;
        rows.push_str(&format!(
            "<tr><td>{id}</td><td>{addr}</td><td>{status:?}</td><td>{committed_cpu:.2}/{capacity_cpu:.2}</td>\
             <td>{committed_memory}/{capacity_memory}</td><td>{last_seen_secs}s</td>{stale}</tr>",
            id = s.id,
            addr = s.addr,
            status = s.status,
            committed_cpu = s.committed_cpu,
            capacity_cpu = s.capacity_cpu,
            committed_memory = s.committed_memory,
            capacity_memory = s.capacity_memory,
            last_seen_secs = s.last_seen_secs,
            stale = if node.data_stale { "<td>stale</td>" } else { "<td></td>" },
        ));
    }
    format!(
        "<h2>Nodes</h2><table><tr><th>ID</th><th>Address</th><th>Status</th><th>CPU (committed/capacity)</th>\
         <th>Memory (committed/capacity)</th><th>Last seen</th><th></th></tr>{rows}</table>"
    )
}

fn render_jails(nodes: &[NodeSnapshot]) -> String {
    let mut rows = String::new();
    for node in nodes {
        for jail in &node.jails {
            let crash_looping = !jail.running && jail.backoff.current_delay_secs.is_some();
            let state = if jail.running {
                "running"
            } else if crash_looping {
                "crash-looping"
            } else {
                "not running"
            };
            rows.push_str(&format!(
                "<tr><td>{node_id}</td><td>{name}</td><td>{state}</td></tr>",
                node_id = node.status.id,
                name = jail.record.spec.metadata.name,
                state = state,
            ));
        }
    }
    format!("<h2>Jails</h2><table><tr><th>Node</th><th>Name</th><th>State</th></tr>{rows}</table>")
}

fn render_volumes(nodes: &[NodeSnapshot]) -> String {
    let mut rows = String::new();
    for node in nodes {
        for volume in &node.volumes {
            rows.push_str(&format!("<tr><td>{node_id}</td><td>{name}</td></tr>", node_id = node.status.id, name = volume.name));
        }
    }
    format!("<h2>Volumes</h2><table><tr><th>Node</th><th>Name</th></tr>{rows}</table>")
}

fn render_services(services: &[ServiceSnapshot]) -> String {
    let mut rows = String::new();
    for service in services {
        let s = &service.summary;
        let replica_placement = service
            .detail
            .as_ref()
            .map(|d| {
                d.replicas.iter().map(|r| format!("{} ({}@{})", r.name, r.node, r.address)).collect::<Vec<_>>().join(", ")
            })
            .unwrap_or_default();
        let actual_replicas = service.detail.as_ref().map(|d| d.replicas.len()).unwrap_or(0);
        rows.push_str(&format!(
            "<tr><td>{name}</td><td>{actual}/{desired}</td><td>{vip}:{port}</td><td>{placement}</td>{stale}</tr>",
            name = s.name,
            actual = actual_replicas,
            desired = s.desired_replicas,
            vip = s.vip,
            port = s.port,
            placement = replica_placement,
            stale = if service.data_stale { "<td>stale</td>" } else { "<td></td>" },
        ));
    }
    format!(
        "<h2>Services</h2><table><tr><th>Name</th><th>Replicas (actual/desired)</th><th>VIP:Port</th>\
         <th>Placement</th><th></th></tr>{rows}</table>"
    )
}

fn render_ingress(nodes: &[NodeSnapshot], now_unix: i64) -> String {
    let mut rows = String::new();
    for node in nodes {
        for ingress in &node.status.ingresses {
            let expiry_cell = match ingress.cert_expires_at_unix {
                Some(expires_at) if expires_at - now_unix <= RENEWAL_THRESHOLD_SECS => {
                    format!("<td class=\"expiry-warning\">{expires_at} (renewing soon)</td>")
                }
                Some(expires_at) => format!("<td>{expires_at}</td>"),
                None => "<td>none</td>".to_string(),
            };
            rows.push_str(&format!(
                "<tr><td>{host}</td><td>{backend_service}:{backend_port}</td>{expiry_cell}</tr>",
                host = ingress.host,
                backend_service = ingress.backend_service,
                backend_port = ingress.backend_port,
                expiry_cell = expiry_cell,
            ));
        }
    }
    format!("<h2>Ingress</h2><table><tr><th>Host</th><th>Backend</th><th>Cert expiry (unix)</th></tr>{rows}</table>")
}
```

- [ ] **Step 4: Wire the module into `keel-dashboard/src/lib.rs`**

```rust
pub mod html;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/html.rs keel-dashboard/src/lib.rs
git commit -m "feat(keel-dashboard): render the dashboard HTML page"
```

---

### Task 15: `keel-dashboard::http`: browser-facing TLS listener + routing + Basic Auth

**Files:**
- Create: `keel-dashboard/src/http.rs`
- Modify: `keel-dashboard/src/lib.rs` (`pub mod http;`)

**Interfaces:**
- Consumes: `tls::load_browser_server_config` (Task 9); `snapshot::Snapshot` (Task 11); `basic_auth::check` (Task 13); `html::render` (Task 14).
- Produces: `pub fn run(listener: TcpListener, tls_config: Arc<rustls::ServerConfig>, snapshot: Arc<RwLock<Snapshot>>, basic_auth_user: String, basic_auth_password: String)`. Routes: `GET /` -> rendered HTML (200, `text/html`); `GET /api/snapshot` -> JSON (200, `application/json`); anything else -> 404. Every route requires Basic Auth (401 if missing/wrong).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::Snapshot;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn start_test_server(snapshot: Snapshot) -> std::net::SocketAddr {
        let tls_config = Arc::new(
            crate::tls::load_browser_server_config(&fixture("fixture-node.crt"), &fixture("fixture-node.key")).unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let snapshot = Arc::new(RwLock::new(snapshot));
        std::thread::spawn(move || run(listener, tls_config, snapshot, "admin".to_string(), "hunter2".to_string()));
        addr
    }

    fn request(addr: std::net::SocketAddr, path: &str, auth_header: Option<&str>) -> (u16, String) {
        crate::tls::ensure_crypto_provider();
        let roots = rustls::RootCertStore::empty();
        let verifier = std::sync::Arc::new(NoVerify);
        let client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        let _ = roots;
        let server_name = rustls::pki_types::ServerName::IpAddress(std::net::Ipv4Addr::new(127, 0, 0, 1).into());
        let tcp = std::net::TcpStream::connect(addr).unwrap();
        let conn = rustls::ClientConnection::new(Arc::new(client_config), server_name).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, tcp);
        let auth = auth_header.map(|h| format!("Authorization: {h}\r\n")).unwrap_or_default();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Length: 0\r\n\r\n");
        stream.write_all(req.as_bytes()).unwrap();
        stream.sock.shutdown(std::net::Shutdown::Write).ok();
        let mut response = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => response.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(_) => break,
            }
        }
        let text = String::from_utf8_lossy(&response).to_string();
        let status: u16 = text.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    #[derive(Debug)]
    struct NoVerify;
    impl rustls::client::danger::ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes()
        }
    }

    #[test]
    fn a_request_with_no_auth_header_is_rejected() {
        let addr = start_test_server(Snapshot::default());
        let (status, _) = request(addr, "/", None);
        assert_eq!(status, 401);
    }

    #[test]
    fn a_request_with_wrong_credentials_is_rejected() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:wrongpassword"));
        let (status, _) = request(addr, "/", Some(&header));
        assert_eq!(status, 401);
    }

    #[test]
    fn a_request_with_correct_credentials_gets_the_dashboard_html() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:hunter2"));
        let (status, body) = request(addr, "/", Some(&header));
        assert_eq!(status, 200);
        assert!(body.contains("keel dashboard"), "got: {body}");
    }

    #[test]
    fn api_snapshot_returns_json() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:hunter2"));
        let (status, body) = request(addr, "/api/snapshot", Some(&header));
        assert_eq!(status, 200);
        assert!(body.contains("\"nodes\""), "got: {body}");
    }

    #[test]
    fn an_unknown_path_is_404() {
        let addr = start_test_server(Snapshot::default());
        let header = format!("Basic {}", STANDARD.encode("admin:hunter2"));
        let (status, _) = request(addr, "/nope", Some(&header));
        assert_eq!(status, 404);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-dashboard --lib http::`
Expected: FAIL to compile (module doesn't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/http.rs`**

```rust
use crate::snapshot::Snapshot;
use rustls::{ServerConnection, StreamOwned};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

type TlsStream = StreamOwned<ServerConnection, TcpStream>;

pub fn run(
    listener: TcpListener,
    tls_config: Arc<rustls::ServerConfig>,
    snapshot: Arc<RwLock<Snapshot>>,
    basic_auth_user: String,
    basic_auth_password: String,
) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let tls_config = Arc::clone(&tls_config);
        let snapshot = Arc::clone(&snapshot);
        let basic_auth_user = basic_auth_user.clone();
        let basic_auth_password = basic_auth_password.clone();
        thread::spawn(move || {
            let Ok(conn) = ServerConnection::new(tls_config) else { return };
            let mut tls_stream = TlsStream::new(conn, stream);
            let _ = handle_connection(&mut tls_stream, &snapshot, &basic_auth_user, &basic_auth_password);
        });
    }
}

struct ParsedRequest {
    method: String,
    path: String,
    authorization: Option<String>,
}

fn handle_connection(
    stream: &mut TlsStream,
    snapshot: &Arc<RwLock<Snapshot>>,
    basic_auth_user: &str,
    basic_auth_password: &str,
) -> io::Result<()> {
    let request = match read_request(stream)? {
        Some(r) => r,
        None => return Ok(()),
    };
    if !crate::basic_auth::check(request.authorization.as_deref(), basic_auth_user, basic_auth_password) {
        return write_response(stream, 401, "text/plain", b"unauthorized");
    }
    let (status, content_type, body) = route(&request, snapshot);
    write_response(stream, status, content_type, &body)
}

fn read_request(stream: &mut TlsStream) -> io::Result<Option<ParsedRequest>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(_)) => {
                let method = req.method.unwrap_or("").to_string();
                let path = req.path.unwrap_or("").to_string();
                let authorization = req
                    .headers
                    .iter()
                    .find(|h| h.name.eq_ignore_ascii_case("authorization"))
                    .map(|h| String::from_utf8_lossy(h.value).to_string());
                return Ok(Some(ParsedRequest { method, path, authorization }));
            }
            Ok(httparse::Status::Partial) => {
                if buf.len() >= MAX_MESSAGE_BYTES {
                    return Ok(None);
                }
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Ok(None);
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => return Ok(None),
        }
    }
}

fn write_response(stream: &mut TlsStream, status: u16, content_type: &str, body: &[u8]) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n",
        reason_phrase(status),
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        _ => "Unknown",
    }
}

fn route(request: &ParsedRequest, snapshot: &Arc<RwLock<Snapshot>>) -> (u16, &'static str, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => {
            let snapshot = snapshot.read().unwrap();
            let now_unix =
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
            (200, "text/html", crate::html::render(&snapshot, now_unix).into_bytes())
        }
        ("GET", "/api/snapshot") => {
            let snapshot = snapshot.read().unwrap();
            let body = serde_json::to_vec(&*snapshot).expect("Snapshot serialization should not fail");
            (200, "application/json", body)
        }
        _ => (404, "text/plain", b"not found".to_vec()),
    }
}
```

- [ ] **Step 4: Wire the module into `keel-dashboard/src/lib.rs`**

```rust
pub mod http;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/http.rs keel-dashboard/src/lib.rs
git commit -m "feat(keel-dashboard): add the Basic-Auth-protected browser-facing HTTP/TLS listener"
```

---

### Task 16: `keel-dashboard::main`: CLI wiring

**Files:**
- Modify: `keel-dashboard/src/main.rs`

**Interfaces:**
- Consumes: every module from Tasks 9-15.
- Produces: the real `keel-dashboard` binary, with flags `--control-plane-addr`, `--tls-ca-file`, `--tls-cert-file`, `--tls-key-file`, `--tls-crl-file`, `--listen-addr`, `--dashboard-tls-cert-file`, `--dashboard-tls-key-file`, `--basic-auth-user`, `--basic-auth-password-file`, `--poll-interval-secs` (default 5).

- [ ] **Step 1: Write the failing tests**

Add a `#[cfg(test)] mod tests` block to `keel-dashboard/src/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn args(strs: &[&str]) -> impl Iterator<Item = String> {
        strs.iter().map(|s| s.to_string()).collect::<Vec<_>>().into_iter()
    }

    fn full_args() -> Vec<&'static str> {
        vec![
            "--control-plane-addr", "10.0.0.1:7620",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/dashboard.crt",
            "--tls-key-file", "/etc/keel/dashboard.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
            "--dashboard-tls-cert-file", "/etc/keel/dashboard-browser.crt",
            "--dashboard-tls-key-file", "/etc/keel/dashboard-browser.key",
            "--basic-auth-user", "admin",
            "--basic-auth-password-file", "/etc/keel/dashboard-password",
        ]
    }

    #[test]
    fn parses_all_required_flags_and_applies_defaults() {
        let config = parse_args_from(args(&full_args()));
        assert_eq!(config.control_plane_addr, Some("10.0.0.1:7620".to_string()));
        assert_eq!(config.listen_addr, "0.0.0.0:8443");
        assert_eq!(config.poll_interval_secs, 5);
    }

    #[test]
    fn parses_a_custom_poll_interval() {
        let mut full = full_args();
        full.extend(["--poll-interval-secs", "10"]);
        let config = parse_args_from(args(&full));
        assert_eq!(config.poll_interval_secs, 10);
    }

    #[test]
    #[should_panic(expected = "--control-plane-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required")]
    fn missing_control_plane_tls_flag_panics() {
        parse_args_from(args(&["--tls-ca-file", "/etc/keel/ca.crt"]));
    }

    #[test]
    #[should_panic(expected = "--dashboard-tls-cert-file and --dashboard-tls-key-file are required")]
    fn missing_dashboard_tls_flag_panics() {
        let mut partial: Vec<&str> = full_args().into_iter().take(10).collect();
        partial.retain(|f| *f != "--dashboard-tls-cert-file" && *f != "/etc/keel/dashboard-browser.crt");
        parse_args_from(args(&partial));
    }

    #[test]
    #[should_panic(expected = "--basic-auth-user and --basic-auth-password-file are required")]
    fn missing_basic_auth_flag_panics() {
        let full = full_args();
        let without_auth: Vec<&str> =
            full.into_iter().take_while(|f| *f != "--basic-auth-user").collect();
        parse_args_from(args(&without_auth));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p keel-dashboard --bin keel-dashboard`
Expected: FAIL to compile (`parse_args_from`/`Config` don't exist yet)

- [ ] **Step 3: Implement `keel-dashboard/src/main.rs`**

```rust
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

struct Config {
    control_plane_addr: Option<String>,
    tls_ca_file: Option<PathBuf>,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    tls_crl_file: Option<PathBuf>,
    listen_addr: String,
    dashboard_tls_cert_file: Option<PathBuf>,
    dashboard_tls_key_file: Option<PathBuf>,
    basic_auth_user: Option<String>,
    basic_auth_password_file: Option<PathBuf>,
    poll_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            control_plane_addr: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_crl_file: None,
            listen_addr: "0.0.0.0:8443".to_string(),
            dashboard_tls_cert_file: None,
            dashboard_tls_key_file: None,
            basic_auth_user: None,
            basic_auth_password_file: None,
            poll_interval_secs: 5,
        }
    }
}

fn parse_args() -> Config {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from(args: impl Iterator<Item = String>) -> Config {
    let mut config = Config::default();
    let mut args = args;
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| panic!("missing value for {flag}"));
        match flag.as_str() {
            "--control-plane-addr" => config.control_plane_addr = Some(value),
            "--tls-ca-file" => config.tls_ca_file = Some(PathBuf::from(value)),
            "--tls-cert-file" => config.tls_cert_file = Some(PathBuf::from(value)),
            "--tls-key-file" => config.tls_key_file = Some(PathBuf::from(value)),
            "--tls-crl-file" => config.tls_crl_file = Some(PathBuf::from(value)),
            "--listen-addr" => config.listen_addr = value,
            "--dashboard-tls-cert-file" => config.dashboard_tls_cert_file = Some(PathBuf::from(value)),
            "--dashboard-tls-key-file" => config.dashboard_tls_key_file = Some(PathBuf::from(value)),
            "--basic-auth-user" => config.basic_auth_user = Some(value),
            "--basic-auth-password-file" => config.basic_auth_password_file = Some(PathBuf::from(value)),
            "--poll-interval-secs" => {
                config.poll_interval_secs =
                    value.parse().unwrap_or_else(|e| panic!("invalid --poll-interval-secs '{value}': {e}"))
            }
            other => panic!("unknown flag: {other}"),
        }
    }
    if config.control_plane_addr.is_none()
        || config.tls_ca_file.is_none()
        || config.tls_cert_file.is_none()
        || config.tls_key_file.is_none()
        || config.tls_crl_file.is_none()
    {
        panic!("--control-plane-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required");
    }
    if config.dashboard_tls_cert_file.is_none() || config.dashboard_tls_key_file.is_none() {
        panic!("--dashboard-tls-cert-file and --dashboard-tls-key-file are required");
    }
    if config.basic_auth_user.is_none() || config.basic_auth_password_file.is_none() {
        panic!("--basic-auth-user and --basic-auth-password-file are required");
    }
    config
}

fn main() {
    let config = parse_args();
    let control_plane_addr = config.control_plane_addr.expect("validated as required in parse_args_from");
    let tls_ca_file = config.tls_ca_file.expect("validated as required in parse_args_from");
    let tls_cert_file = config.tls_cert_file.expect("validated as required in parse_args_from");
    let tls_key_file = config.tls_key_file.expect("validated as required in parse_args_from");
    let tls_crl_file = config.tls_crl_file.expect("validated as required in parse_args_from");
    let dashboard_tls_cert_file = config.dashboard_tls_cert_file.expect("validated as required in parse_args_from");
    let dashboard_tls_key_file = config.dashboard_tls_key_file.expect("validated as required in parse_args_from");
    let basic_auth_user = config.basic_auth_user.expect("validated as required in parse_args_from");
    let basic_auth_password_file = config.basic_auth_password_file.expect("validated as required in parse_args_from");
    let basic_auth_password = std::fs::read_to_string(&basic_auth_password_file)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", basic_auth_password_file.display()))
        .trim()
        .to_string();

    let client_config = Arc::new(
        keel_dashboard::tls::load_client_config(&tls_cert_file, &tls_key_file, &tls_ca_file, &tls_crl_file)
            .unwrap_or_else(|e| panic!("failed to load control-plane TLS client config: {e}")),
    );
    let client: Box<dyn keel_dashboard::control_plane_client::ControlPlaneClient> =
        Box::new(keel_dashboard::control_plane_client::TlsControlPlaneClient::new(control_plane_addr, client_config));
    let snapshot = keel_dashboard::poller::spawn(client, Duration::from_secs(config.poll_interval_secs));

    let server_config = Arc::new(
        keel_dashboard::tls::load_browser_server_config(&dashboard_tls_cert_file, &dashboard_tls_key_file)
            .unwrap_or_else(|e| panic!("failed to load dashboard TLS server config: {e}")),
    );

    eprintln!("keel-dashboard: starting (listen_addr={})", config.listen_addr);
    let listener = std::net::TcpListener::bind(&config.listen_addr).expect("failed to bind TCP listener");
    keel_dashboard::http::run(listener, server_config, snapshot, basic_auth_user, basic_auth_password);
}

#[cfg(test)]
mod tests {
    // (Step 1's test module content goes here)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p keel-dashboard --bin keel-dashboard`
Expected: PASS, all tests green

- [ ] **Step 5: Run the full workspace**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS, clean

- [ ] **Step 6: Commit**

```bash
git add keel-dashboard/src/main.rs
git commit -m "feat(keel-dashboard): wire CLI flags to the poller and browser-facing listener"
```

---

### Task 17: `keel-dashboard/rc.d/keel_dashboard`

**Files:**
- Create: `keel-dashboard/rc.d/keel_dashboard`

**Interfaces:**
- Consumes: nothing (a shell script).
- Produces: an rc.d service definition matching the existing `keel_agentd`/`keel_controlplane` pattern exactly.

- [ ] **Step 1: Create the script**

```sh
#!/bin/sh
#
# PROVIDE: keel_dashboard
# REQUIRE: NETWORKING keel_controlplane
# KEYWORD: shutdown

. /etc/rc.subr

name="keel_dashboard"
rcvar="keel_dashboard_enable"

load_rc_config "$name"

: ${keel_dashboard_enable:="NO"}
: ${keel_dashboard_bin:="/usr/local/bin/keel-dashboard"}
: ${keel_dashboard_control_plane_addr:=""}
: ${keel_dashboard_tls_ca_file:=""}
: ${keel_dashboard_tls_cert_file:=""}
: ${keel_dashboard_tls_key_file:=""}
: ${keel_dashboard_tls_crl_file:=""}
: ${keel_dashboard_listen_addr:="0.0.0.0:8443"}
: ${keel_dashboard_dashboard_tls_cert_file:=""}
: ${keel_dashboard_dashboard_tls_key_file:=""}
: ${keel_dashboard_basic_auth_user:=""}
: ${keel_dashboard_basic_auth_password_file:=""}
: ${keel_dashboard_poll_interval_secs:=""}

pidfile="/var/run/${name}.pid"
command="/usr/sbin/daemon"

# Built up rather than a single quoted string: each of these flags is only
# passed when the operator has actually set the matching rc.conf variable,
# since keel-dashboard panics on startup if the control-plane/TLS/basic-auth
# flag groups are only partially supplied (see keel-dashboard/src/main.rs).
flags=""
[ -n "$keel_dashboard_control_plane_addr" ] && flags="$flags --control-plane-addr $keel_dashboard_control_plane_addr"
[ -n "$keel_dashboard_tls_ca_file" ] && flags="$flags --tls-ca-file $keel_dashboard_tls_ca_file"
[ -n "$keel_dashboard_tls_cert_file" ] && flags="$flags --tls-cert-file $keel_dashboard_tls_cert_file"
[ -n "$keel_dashboard_tls_key_file" ] && flags="$flags --tls-key-file $keel_dashboard_tls_key_file"
[ -n "$keel_dashboard_tls_crl_file" ] && flags="$flags --tls-crl-file $keel_dashboard_tls_crl_file"
[ -n "$keel_dashboard_dashboard_tls_cert_file" ] && flags="$flags --dashboard-tls-cert-file $keel_dashboard_dashboard_tls_cert_file"
[ -n "$keel_dashboard_dashboard_tls_key_file" ] && flags="$flags --dashboard-tls-key-file $keel_dashboard_dashboard_tls_key_file"
[ -n "$keel_dashboard_basic_auth_user" ] && flags="$flags --basic-auth-user $keel_dashboard_basic_auth_user"
[ -n "$keel_dashboard_basic_auth_password_file" ] && flags="$flags --basic-auth-password-file $keel_dashboard_basic_auth_password_file"
[ -n "$keel_dashboard_poll_interval_secs" ] && flags="$flags --poll-interval-secs $keel_dashboard_poll_interval_secs"

command_args="-r -P ${pidfile} -S -T ${name} -- \
  ${keel_dashboard_bin} --listen-addr ${keel_dashboard_listen_addr} \
  ${flags}"

run_rc_command "$1"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x keel-dashboard/rc.d/keel_dashboard`

- [ ] **Step 3: Commit**

```bash
git add keel-dashboard/rc.d/keel_dashboard
git commit -m "feat(keel-dashboard): add rc.d service script"
```

---

### Task 18: End-to-end integration test

**Files:**
- Create: `keel-dashboard/tests/integration.rs`

**Interfaces:**
- Consumes: every module built in Tasks 9-15.
- Produces: a black-box test proving the poller, the merged snapshot, the JSON API, and the rendered HTML agree with each other after a real poll cycle - the "Integration test" the design doc calls for.

- [ ] **Step 1: Write the test**

```rust
use base64::{engine::general_purpose::STANDARD, Engine as _};
use keel_controlplane::wire::{NodeState, NodeStatus, ServiceProxyEntry, ServiceSummary};
use keel_dashboard::control_plane_client::FakeControlPlaneClient;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
}

#[derive(Debug)]
struct NoVerify;
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes()
    }
}

fn request(addr: std::net::SocketAddr, path: &str, user: &str, password: &str) -> (u16, String) {
    keel_dashboard::tls::ensure_crypto_provider();
    let client_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    let server_name = rustls::pki_types::ServerName::IpAddress(std::net::Ipv4Addr::new(127, 0, 0, 1).into());
    let tcp = std::net::TcpStream::connect(addr).unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(client_config), server_name).unwrap();
    let mut stream = rustls::StreamOwned::new(conn, tcp);
    let auth = format!("Basic {}", STANDARD.encode(format!("{user}:{password}")));
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {auth}\r\nContent-Length: 0\r\n\r\n");
    stream.write_all(req.as_bytes()).unwrap();
    stream.sock.shutdown(std::net::Shutdown::Write).ok();
    let mut response = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&response).to_string();
    let status: u16 = text.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[test]
fn a_poll_cycle_is_reflected_in_both_the_json_api_and_the_rendered_html() {
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
    client.set_jails("node-1", vec![]);
    client.set_volumes("node-1", vec![]);
    client.set_services(vec![ServiceSummary { name: "web".to_string(), desired_replicas: 1, vip: "10.0.250.7".to_string(), port: 8080 }]);
    client.set_service("web", ServiceProxyEntry { name: "web".to_string(), vip: "10.0.250.7".to_string(), port: 8080, replicas: vec![] });

    let snapshot = keel_dashboard::poller::spawn(Box::new(client), Duration::from_millis(20));

    let mut attempts = 0;
    loop {
        if !snapshot.read().unwrap().nodes.is_empty() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "the poller never populated the snapshot");
        std::thread::sleep(Duration::from_millis(20));
    }

    let tls_config = Arc::new(
        keel_dashboard::tls::load_browser_server_config(&fixture("fixture-node.crt"), &fixture("fixture-node.key")).unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        keel_dashboard::http::run(listener, tls_config, snapshot, "admin".to_string(), "hunter2".to_string())
    });

    let (unauth_status, _) = request(addr, "/", "admin", "wrongpassword");
    assert_eq!(unauth_status, 401, "wrong credentials must be rejected even after a successful poll");

    let (json_status, json_body) = request(addr, "/api/snapshot", "admin", "hunter2");
    assert_eq!(json_status, 200);
    assert!(json_body.contains("\"node-1\""), "got: {json_body}");
    assert!(json_body.contains("\"web\""), "got: {json_body}");

    let (html_status, html_body) = request(addr, "/", "admin", "hunter2");
    assert_eq!(html_status, 200);
    assert!(html_body.contains("node-1"), "got: {html_body}");
    assert!(html_body.contains("10.0.250.7"), "got: {html_body}");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p keel-dashboard --test integration`
Expected: FAIL (this is the first test in a new `tests/` directory, so it should at least compile against everything built in Tasks 9-15; if it fails to compile, fix the mismatch against the real signatures those tasks produced before proceeding - do not change the design, only the plumbing)

- [ ] **Step 3: Run the test to verify it passes**

Run: `cargo test -p keel-dashboard --test integration`
Expected: PASS

- [ ] **Step 4: Run the full workspace one last time**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS, clean

- [ ] **Step 5: Commit**

```bash
git add keel-dashboard/tests/integration.rs
git commit -m "test(keel-dashboard): add end-to-end poll-to-HTTP integration test"
```

---

## Verification plan (manual, not a task for a coding subagent)

Per this project's discipline (see `[[feedback_verify_infra_before_planning]]` in memory), this milestone is not "done" until it's verified against Milestone 21's real FreeBSD VPS deployment:

1. Build `keel-dashboard` for FreeBSD and copy it (plus `rc.d/keel_dashboard`) to a node alongside the already-verified `keel-controlplane`/`keel-agentd`/`keel-ingress`.
2. Issue it a client certificate from the cluster's real CA (same CA `keelctl` uses - the control plane authorizes by CA membership only, not client identity, so no control-plane changes are needed).
3. Point `keel-dashboard` at the real control plane; confirm live nodes, jails, services, volumes, and the real Let's Encrypt-issued ingress certificate (with its real expiry) all show up correctly.
4. Confirm Basic Auth actually blocks an unauthenticated request from a real external client.
5. Confirm the browser-facing TLS listener is reachable and presents its certificate correctly from a real external client (e.g. `curl -k` or a real browser).
6. Kill the control plane briefly and confirm the dashboard falls back to the last-good snapshot with a visible "stale as of ..." banner rather than blanking the page.
