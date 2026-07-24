# Milestone 22: keel dashboard

## Motivation

Today the only window into cluster state is `keelctl get`, one resource at
a time, raw YAML, one control-plane round trip per call. There is no
single place to see cluster health at a glance: which nodes are alive,
what's running where, which services are under-replicated, which
certificates are close to expiry. This milestone adds a read-only web
dashboard giving that single-pane-of-glass view.

## Goals

- A live (auto-refreshing) web view of: nodes, jails, services, volumes,
  and ingress/certificate state, across the whole cluster.
- Read-only. No mutations from the dashboard; `keelctl` remains the only
  way to apply/delete/force-repin.
- Consistent with the project's existing style: hand-rolled TLS/HTTP, no
  web framework, no JS build step, minimal new dependencies.

## Non-goals

- Any write path (apply/delete/scale/force-repin) from the UI.
- Historical metrics/graphing or persistence of past cluster state; the
  dashboard only ever shows current state.
- Direct dashboard-to-node communication; the dashboard talks only to the
  control plane, exactly like `keelctl` does today.

## Architecture

A new workspace member, `keel-dashboard` (binary crate), added to the
`[workspace] members` list. It plays two roles:

1. **mTLS client to the control plane.** Same shape as `keelctl`'s
   `ControlPlane` target: `--control-plane-addr`, `--tls-ca-file`,
   `--tls-cert-file`, `--tls-key-file`, `--tls-crl-file`. The control
   plane does not authorize by client identity today (any cert signed by
   the cluster CA can hit any route), so `keel-dashboard` just needs a
   cert issued from the same CA `keelctl` uses. No control-plane
   auth-model changes are needed.
2. **Browser-facing HTTPS server.** Its own `rustls` listener (operator
   supplies a cert/key, self-signed is fine for v1), with HTTP Basic Auth
   in front of every route (`--basic-auth-user`, checked against a
   password read from `--basic-auth-password-file`, never a CLI arg).
   Basic Auth over a network is only safe because this listener
   terminates its own TLS. This needs its own `rustls::ServerConfig`
   builder: `keel_controlplane::tls::load_server_config` can't be reused
   as-is, since it hardcodes a `WebPkiClientVerifier` for mTLS and
   browsers won't present a client certificate.

A background poller thread refreshes an in-memory cluster snapshot every
`--poll-interval-secs` (default 5) and stores it behind
`Arc<RwLock<Snapshot>>`. Browser requests only ever read the cache; they
never block on a live control-plane round trip. This deliberately departs
from `keel-controlplane`'s worker-owns-state-via-command-channel pattern:
that pattern exists there to serialize scheduling/write decisions, but
`keel-dashboard` only ever caches and serves reads, so a plain `RwLock`
is the simpler, correct fit.

### Poller behavior and partial failure

Each poll cycle:

1. `GET /nodes` &rarr; node list (id, addr, status, capacity/committed
   CPU/memory, `ingresses` &mdash; see below). This is also how the set
   of node IDs to fan out to is discovered.
2. For each node: `GET /nodes/{id}/jails` and `GET /nodes/{id}/volumes`
   (new, see below).
3. `GET /services` &rarr; summaries, then `GET /services/{name}` per
   service for replica placement.

Fetches happen sequentially within a poll cycle; this project's real
clusters are small (3 nodes in verification), so a full cycle is cheap
and concurrency isn't worth the complexity yet.

If the control plane itself is unreachable, the last good snapshot is
kept and served with a "stale as of &lt;time&gt;" banner rather than
blanking the page. If one per-node or per-service fetch fails mid-cycle,
only that node/service's data is marked stale in the merged snapshot;
the rest of the snapshot still updates normally.

## Control-plane extensions

Two additions, both mirroring existing patterns exactly.

### 1. Ingress reporting via heartbeat

`Heartbeat` (`keel-controlplane/src/wire.rs`) gains a new field:

```rust
pub struct Heartbeat {
    pub committed_cpu: f64,
    pub committed_memory: u64,
    #[serde(default)]
    pub jails: Vec<JailHealth>,
    #[serde(default)]
    pub ingresses: Vec<IngressHealth>,
}

pub struct IngressHealth {
    pub name: String,
    pub host: String,
    pub backend_service: String,
    pub backend_port: u16,
    pub cert_expires_at_unix: Option<i64>,
}
```

`#[serde(default)]` keeps this backward compatible, exactly like `jails`
already is. `NodeStatus` gains the same `ingresses: Vec<IngressHealth>`
field, populated from the latest heartbeat, exactly like
`committed_cpu`/`committed_memory` already are. `keel-agentd` populates
its heartbeat's `ingresses` from `ingress_store::load_all` (already
exists, returns every local `IngressRecord { spec, cert_expires_at_unix
}`) on every heartbeat tick, the same way it already gathers per-jail
health. No new control-plane route is needed: `GET /nodes` already
returns `NodeStatus`, so ingress data rides along for free.

### 2. Volumes-list endpoint

Mirrors the existing per-node jails-list route:

- `keel-agentd`: new `Command::ListVolumes`, handled by enumerating ZFS
  datasets under `<pool>/keel/volumes/` (same dataset-path convention
  `record.rs::volume_dataset_path` already defines), returning
  `Vec<VolumeStatus>`. New route: `GET /volumes` (list), alongside the
  existing `GET /volumes/{name}` (by-name) and `DELETE /volumes/{name}`.
- `keel-controlplane`: new route `GET /nodes/{id}/volumes` forwarding to
  the node's `GET /volumes`, alongside the existing
  `GET /nodes/{id}/volumes/{name}` and `DELETE /nodes/{id}/volumes/{name}`.

Unlike jails, the reconciler keeps no in-memory record of which volumes
exist (`Reconciler::get_volume`/`delete_volume` deliberately never
consult `self.records`, since a volume can outlive every jail record
that ever referenced it): ZFS enumeration is the only source of truth
here, not just the simplest option. `keel_zfs::ZfsManager` currently has
no dataset-listing method at all (only `dataset_exists`,
`create_volume`, `destroy_dataset`, and the snapshot/send/receive
methods), so this milestone also adds one, e.g.
`list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError>`,
implemented for both `CliZfsManager` (`zfs list -H -o name -r <parent>`,
filtered down to immediate children) and `FakeZfsManager` (filter its
in-memory dataset set by prefix).

## Dashboard content

A single page, auto-refreshing every 5 seconds via a small vanilla-JS
`fetch()` loop against a new `GET /api/snapshot` JSON endpoint internal to
`keel-dashboard` (this JSON boundary is between the browser and
`keel-dashboard` only; the control plane's own wire format stays YAML,
unaffected). No template engine: HTML is hand-written Rust string
templates, the same style `keel-ingress`'s nginx config templating
already uses.

Sections:

- **Nodes** &mdash; id, address, Alive/Dead, capacity vs. committed
  CPU/memory, last-seen age.
- **Jails** &mdash; grouped by node: name, running/crash-looping. There's
  no dedicated boolean for this: `JailStatus` only has `running` and a
  `backoff: BackoffStatus { retry_in_secs, current_delay_secs }`, so
  crash-looping is derived as `!running &&
  backoff.current_delay_secs.is_some()`.
- **Services** &mdash; name, desired vs. actual replica count, VIP:port,
  replica placement (node + address).
- **Volumes** &mdash; grouped by node: name.
- **Ingress** &mdash; host, backend service:port, cert expiry, with a
  visual warning once inside the existing 30-day renewal threshold
  (matching the threshold `keel-agentd`'s cert-renewal logic already
  uses, so the dashboard's warning and the daemon's actual renewal
  trigger stay in sync).

## Configuration and deployment

`keel-dashboard` takes CLI flags (mirroring `keelctl`/`keel-agentd`
style, settable via an `rc.d` conf file):

- Control-plane mTLS client: `--control-plane-addr`, `--tls-ca-file`,
  `--tls-cert-file`, `--tls-key-file`, `--tls-crl-file`.
- Browser-facing server: `--listen-addr`, `--dashboard-tls-cert-file`,
  `--dashboard-tls-key-file`, `--basic-auth-user`,
  `--basic-auth-password-file`.
- `--poll-interval-secs` (default 5).

A new `keel-dashboard/rc.d/keel_dashboard` script follows the existing
`keel_agentd`/`keel_controlplane` rc.d pattern: `REQUIRE: NETWORKING
keel_controlplane`, flags built up conditionally from `rc.conf`
variables, run via `/usr/sbin/daemon`.

## Testing

- Unit tests: snapshot merging (including partial-failure/staleness
  marking), HTML rendering of each section, Basic Auth middleware,
  ingress cert-expiry warning threshold, crash-looping derivation from
  `JailStatus`/`BackoffStatus`.
- `keel_zfs::ZfsManager::list_child_datasets` gets its own unit tests in
  `keel-zfs` (both `CliZfsManager` and `FakeZfsManager`), plus a
  `keel-agentd` test that `Command::ListVolumes` returns exactly the
  datasets created under `<pool>/keel/volumes/`.
- A `Fake` control-plane test double (in-process, same trait-based
  Fake/Real split this project uses everywhere else) lets
  `keel-dashboard`'s poller and HTTP layer be tested fully without a real
  control plane or FreeBSD.
- Integration test: spin up the fake control plane, run a poll cycle,
  assert the served `/api/snapshot` and rendered HTML reflect it.

## Verification plan

Per this project's discipline, real-VM/VPS verification before calling
this done: point a running `keel-dashboard` at the real control plane
from Milestone 21's verified deployment, and confirm live nodes, jails,
services, volumes, and the real Let's Encrypt-issued ingress certificate
(with its real expiry) all show up correctly; confirm Basic Auth actually
blocks unauthenticated requests; confirm the browser-facing TLS listener
works from a real external client.
