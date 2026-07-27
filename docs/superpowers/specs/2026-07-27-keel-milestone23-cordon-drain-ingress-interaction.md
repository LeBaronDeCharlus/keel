# Milestone 23: Cordon/Drain — Ingress Interaction (contributed section)

Status: Draft — contributed section, for folding into the full Sub-Project 11
(cordon/drain) design doc

Date: 2026-07-27

This is not a standalone milestone. It is the Ingress-interaction piece of the
Sub-Project 11 "cordon/drain" design (proposed by Rust, endorsed by Bumble,
2026-07-27 channel discussion), written up separately at Corentin's request
so it can be merged into the full design doc by whoever writes it.

## Context

`kind: Ingress` (Milestone 21) is deliberately never control-plane-routed:
`keelctl run_apply`'s `Ingress` branch (`keelctl/src/main.rs:159-166`) posts a
bare `/ingress/{name}` request straight at the target node's `keel-agentd`,
skipping `jails_path`'s `--node`-aware `/nodes/{node}{suffix}` prefixing that
every other kind uses. An operator picks the node directly when they apply
an Ingress spec; there is no scheduler decision, no `Placements` entry, and
nothing in `pick_node`/`pick_node_for_service` (`keel-controlplane/src/
services.rs`) is aware Ingress exists at all.

`keel-agentd::ensure_ingress_jail` (`keel-agentd/src/reconciler.rs:367`)
provisions that node's singleton nginx jail and drives real ACME issuance
(Let's Encrypt, DNS-01) for whatever host was applied there. That cert and
that nginx jail are physically local to the node: nothing elsewhere in the
cluster can serve that host if the node goes away.

So if `keelctl drain <node>` only reschedules `Jail`/`Service` workloads and
force-repins pinned stateful replicas (the mechanisms Rust's proposal
already reuses from Milestones 4, 9/10, and 19), it would report a node
"safe to take down" while silently stranding a live, publicly-trusted HTTPS
endpoint. That is a correctness gap in the drain contract, not a cosmetic
one: "drained" has to mean "nothing is depending on this node anymore,"
and today an Ingress is exactly the kind of thing drain would miss.

The good news: the control plane is *not* actually blind to per-node
Ingress state, even though scheduling is. Every heartbeat already carries
it: `Command::Heartbeat(..., heartbeat.ingresses, ...)`
(`keel-controlplane/src/http.rs:618`) feeds `Registry::heartbeat`, and
`registry.list(now)` (exercised directly by
`heartbeat_records_ingress_health_and_list_reports_it`,
`keel-controlplane/src/registry.rs:418`) already returns each node's current
`Vec<IngressHealth>` (`name`, `host`, `backend_service`, `backend_port`,
`cert_expires_at_unix`). This is exactly the data Milestone 22's dashboard
already surfaces. Drain does not need a new query mechanism, a new route, or
new node-to-controlplane plumbing to know whether a node it's about to
drain is serving live Ingress traffic — it just needs to read data that
already flows through the registry on every heartbeat.

## Goals

- `keelctl drain <node>` refuses to report success while that node's most
  recent heartbeat lists any non-empty `ingresses`.
- The operator gets a specific, actionable error naming the stranded
  host(s), not a generic failure.
- No change to how Ingress is applied, scheduled, or reconciled. This is
  purely a read of already-heartbeated data at drain time, consistent with
  Milestone 21's explicit choice to keep Ingress placement manual and
  node-local.

## Non-Goals

- **No automatic relocation of an Ingress off a draining node.** Moving a
  host to another node means a fresh ACME order (DNS-01 challenge,
  propagation delay, and consumption of Let's Encrypt's rate limits) against
  a different node's nginx jail, not a data-plane reschedule like a `Jail`
  or `Service`. That is a materially larger, separate design decision
  (effectively a second, harder migration mechanism alongside Milestone 19's
  `zfs send`/`receive` one) and is explicitly deferred. If this is ever
  wanted, it earns its own spec and its own real-VM verification pass, not
  a clause bolted onto cordon/drain.
- **No change to `keel-controlplane`'s scheduling awareness of Ingress.**
  `pick_node`/`pick_node_for_service` stay exactly as ignorant of Ingress
  placement as they are today; this section only reads existing heartbeat
  data at drain time, it does not add Ingress to any placement decision.
- **No polling or freshness guarantee beyond what heartbeats already
  provide.** If a node's heartbeat is stale (already `Dead` by the existing
  timeout), drain against a dead node is a different, pre-existing case
  (arguably moot — nothing to gracefully drain from a node that's already
  down) and out of scope here.
- **No handling of an Ingress applied *during* an in-progress drain.**
  Since Ingress apply bypasses the control plane entirely, the control
  plane cannot block or intercept it. The drain check is a point-in-time
  read of the last heartbeat, not a lock. Treated as an accepted, named gap
  rather than solved: an operator who applies a fresh Ingress to a node
  mid-drain is working against their own maintenance window.

## Architecture

### Detecting a live Ingress at drain time

`keelctl drain <node>`'s control-plane-side handler, before (or interleaved
with) rescheduling `Jail`/`Service` workloads:

1. Call the same `Registry::list`/status-lookup path `GET /nodes` already
   uses to find `node`'s current `NodeStatus`.
2. If `status.ingresses` is non-empty, refuse immediately with a `409`
   naming every stranded host, e.g.:

   ```
   409 Conflict
   node-2 has 2 live Ingress host(s) that would be stranded by drain: example.com, stats.example.com.
   Move them first: keelctl apply -f <ingress>.yaml --node <other-node>, then delete the old one, then retry drain.
   ```

3. Only once `ingresses` is empty does drain proceed with its existing
   Jail/Service rescheduling and stateful force-repin steps.

This means `Cordon` (marking a node unschedulable) and `Drain` (actually
emptying it) stay orthogonal exactly as Rust's proposal already has them:
cordoning a node with a live Ingress still succeeds (it only blocks *new*
scheduling, and Ingress was never scheduler-routed to begin with); only
`drain`'s stronger "empty and safe to take down" claim is gated on Ingress
being clear.

### Operator remediation path

Because Ingress placement is already a manual, direct-to-node operation
(Milestone 21's own design, not something this milestone changes), the
remediation is symmetric and requires no new primitives:

1. `keelctl apply -f <ingress>.yaml --node <replacement-node>` — issues a
   fresh cert against the new node via the existing `ensure_ingress_jail`
   path. Operator-driven, so the ACME/DNS-01 latency is visible and
   expected, not hidden inside an automatic drain.
2. `keelctl delete -f <ingress>.yaml` (or equivalent) against the
   *original* node once the new one is confirmed serving.
3. Retry `keelctl drain <node>`; it now proceeds past the Ingress check.

## Error Handling

- Drain's Ingress check is a precondition, evaluated before any
  Jail/Service rescheduling side effects begin, so a `409` here leaves the
  node completely untouched — no partial drain, nothing to roll back.
- The error message enumerates hosts, not just a count, so the operator
  doesn't have to separately query `GET /nodes` or the dashboard to find
  out what's blocking them.

## Testing Strategy

- Unit test on `keel-controlplane`: a node heartbeating with a non-empty
  `ingresses` list causes `drain` to return `409` before touching
  `Placements`/`Standbys`, using the existing `FakeJailRuntime`-style test
  harness the rest of the control plane's command tests already use.
- Unit test: a node with an empty `ingresses` list (or none reported yet)
  proceeds through drain's existing Jail/Service/stateful-replica path
  unaffected — proves this check is additive, not a regression on the
  Ingress-free case every prior milestone's tests already cover.
- Real-VM verification (per this project's standing discipline of never
  assuming FreeBSD-specific behavior): reproduce the scenario this section
  exists to prevent. Apply a real Ingress to a node in the 3-node cluster,
  confirm a real Let's Encrypt-staging cert is issued and serving, then
  attempt `keelctl drain` against that node and confirm it refuses with the
  expected host name rather than silently succeeding and leaving a dangling
  nginx jail with an orphaned cert behind. Then perform the remediation
  path above against a second node and confirm drain succeeds once the
  original is clear.
