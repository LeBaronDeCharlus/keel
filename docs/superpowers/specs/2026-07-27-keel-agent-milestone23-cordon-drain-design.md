# Milestone 23: Cordon and Drain (Sub-Project 11)

Status: Draft

Date: 2026-07-27

## Context

Proposed by Rust in the 2026-07-27 channel discussion, endorsed by Bumble and
Corentin over two alternatives (a `kind: Secret` primitive, and CI/dependency
hygiene work, the latter tracked separately as ungated infra rather than a
numbered milestone). The README's roadmap explicitly states no undesigned
sub-projects remain, so this is a fresh proposal, not a queued gap.

Today the only way a node stops receiving work is dying: `NodeState` is a
plain two-variant enum, `Alive`/`Dead` (`keel-controlplane/src/wire.rs:41-43`),
computed fresh on every `Registry::list` call from heartbeat recency alone
(`elapsed < DEAD_THRESHOLD`, `keel-controlplane/src/registry.rs:155-166`,
`DEAD_THRESHOLD` is 15s at `registry.rs:7`). Nothing persists a node's
schedulability independent of that timer. The only operator-triggered
recovery mechanism is Milestone 19's `keelctl force-repin`, which is
reactive (it requires the primary to already be confirmed `Dead`) and
narrow (it only ever acts on one stateful replica named on the command
line, not "everything on this node").

There is no operator verb for "I'm about to patch/reboot node-2, move its
work off first." An operator's only lever today is to actually take the
node down and wait 15 seconds for `Dead` to propagate, then run
`force-repin` once per stateful replica it hosted — during which every
stateless `Service` replica and every plain `kind: Jail` it hosted is
either mid-reschedule-elsewhere (Service) or simply gone until someone
manually re-applies it (plain Jail; see "Architecture" below for why
these two need different treatment). This milestone adds an explicit,
graceful path that never requires that node to actually go down first.

`pick_node`/`pick_node_for_service` and every scheduling call site that
builds their candidate list filter on nothing but `NodeState::Alive` today
— three separate call sites, not one shared helper:
`Command::ResolveOrSchedule` (`keel-controlplane/src/worker.rs:250`),
`Command::ReconcileServices`'s initial candidate build
(`worker.rs:319`), and its per-replica standby selection
(`worker.rs:571`). None of them know about anything except heartbeat-derived
liveness.

## Goals

- `keelctl cordon <node>`: marks a node unschedulable for **new** work.
  Everything already running on it keeps running untouched. Idempotent —
  cordoning an already-cordoned node is a no-op success, not an error.
- `keelctl uncordon <node>`: re-admits a cordoned node to scheduling.
- `keelctl drain <node>`: actively empties a schedulable-or-not node of
  everything it hosts, so it becomes safe to take down. Implicitly cordons
  the node first if it wasn't already (draining a node that's still
  accepting new work makes no sense). Reports success only once the node
  is confirmed empty.
- Cordoned/draining state survives a control-plane restart, the same
  durability bar every other piece of mutable control-plane state added
  since `--state-dir` (commit `d06ce35`) already meets (`Placements`,
  `Standbys`, `PendingFences`, `Services` — see `persist_placements`,
  `persist_standbys`, `persist_pending_fences` at
  `keel-controlplane/src/worker.rs:191-210`).
- `drain` refuses (rather than silently stranding a live HTTPS endpoint) if
  the target node has any live `kind: Ingress` — this section is
  contributed and already written up in full by BSD; see
  "Ingress interaction" below, folded in verbatim from
  `docs/superpowers/specs/2026-07-27-keel-milestone23-cordon-drain-ingress-interaction.md`.
- Milestone 22's dashboard shows cordoned state alongside `Alive`/`Dead`.

## Non-Goals

- **No automatic cordon/drain on any condition.** Exactly like Milestone
  18/19's stance against auto-promotion (avoiding the split-brain risk of
  guessing a node is "about to fail"), cordon and drain are always an
  explicit operator action, never triggered by resource pressure, age, or
  anything else.
- **No automatic relocation of a live Ingress off a draining node.**
  Covered by the contributed Ingress section: an Ingress must be moved
  manually, first, by the operator, before drain will proceed past it.
- **No configurable drain policy (grace periods, PodDisruptionBudget-style
  concurrency limits, etc.).** Drain moves everything it can, as fast as
  the existing per-replica reschedule/force-repin mechanisms allow, with
  no throttling knob. If this turns out to matter in practice, it is a
  later milestone's problem.
- **No draining of a `Dead` node.** `drain` against a node whose most
  recent heartbeat already places it past `DEAD_THRESHOLD` returns an
  error directing the operator to Milestone 19's `force-repin` instead
  (per-replica, already the correct tool for a node that's actually gone)
  rather than reinventing that path under a new name.
- **No change to how a still-pinned stateful replica behaves on a node
  that is merely temporarily `Dead`** (Milestone 18's health-blind
  `present_indices` logic). Cordon/drain only ever act on a node that is
  currently `Alive`.
- **Not a replacement for `force-repin`.** `force-repin` still exists,
  unchanged, for the reactive "node is already gone" case; `drain` is the
  new, additional graceful/proactive case, sharing its underlying
  standby-promotion mechanism (see Architecture) rather than duplicating it.
- **CI enforcement and the `serde_yaml` dependency migration are explicitly
  out of scope for this milestone.** Per the channel's resolution
  (Bumble, endorsed by BSD): real, but process/dependency debt, not a
  feature milestone in the sense this roadmap tracks. Tracked as ungated
  parallel hygiene work, ships whenever, competes with nothing here for
  the "Sub-Project 11" slot.
- **Milestone 15's real-3-node-VM verification debt is untouched by this
  milestone.** The roadmap already flags service discovery as "code
  complete... real 3-node VM verification not yet run"; that is separate,
  pre-existing owed work, not something cordon/drain needs to touch or
  block on.

## Architecture

### Control-plane state: `Cordoned`

A new flat set, matching the existing style of `Placements`/`Standbys`/
`PendingFences` (plain in-memory collection, owned exclusively by the
single-writer `worker::spawn` thread, touched only via `Command` round-trips
through the existing `mpsc::Sender<Command>` / oneshot-reply pattern):

```rust
struct Cordoned { ids: HashSet<String> }
```

Deliberately **not** folded into `NodeState`. `NodeState::Alive`/`Dead` is a
pure function of heartbeat recency, recomputed fresh on every `list()` call
with no persisted backing store at all (`registry.rs:155-166`) — that is
exactly right for liveness, which must never lag reality, but exactly wrong
for cordon, which is operator-set and must survive both a heartbeat gap and
a control-plane restart without being silently reset. A missed heartbeat
must never accidentally uncordon a node, and a control-plane restart must
never forget a cordon was in effect.

New `Command` variants, following the existing shape (`Cordon`/`Uncordon`
each take a node id and a oneshot reply; `IsCordoned`/`ListCordoned` are
read-only):

```rust
Cordon(String, Sender<Result<(), UnknownNode>>),
Uncordon(String, Sender<Result<(), UnknownNode>>),
```

`Cordon`/`Uncordon` validate the node id against the `Registry` (so
cordoning a name that was never registered is a clear `404`, matching the
existing `UnknownNode` error every other node-id-taking path already uses),
then insert/remove from `Cordoned.ids` and call a new `persist_cordoned`
(mirroring `persist_placements`/`persist_standbys` at `worker.rs:191-210`,
writing `state_dir.join("cordoned.yaml")`) and load it at startup alongside
the other four persisted collections.

### Scheduling: one shared `is_schedulable` helper, not three filters

Today's three independent `.filter(|s| s.status == NodeState::Alive)` call
sites (`worker.rs:250`, `319`, `571`) each need the identical additional
condition. Rather than tripling `&& !cordoned.contains(&s.id)` across all
three (the same kind of copy-paste Fizz's audit already flagged elsewhere
in this codebase), this milestone introduces one function:

```rust
fn is_schedulable(status: &NodeStatus, cordoned: &Cordoned) -> bool {
    status.status == NodeState::Alive && !cordoned.ids.contains(&status.id)
}
```

used at all three sites. This changes nothing about *which* node ends up
picked among schedulable candidates (`scheduler::pick_node`'s bin-packing
and `pick_node_for_service`'s same-service-spreading, `services.rs:199-207`,
are untouched) — it only changes the candidate set they're picked from.

Cordoning a node therefore takes effect purely by excluding it from future
`ResolveOrSchedule` and `ReconcileServices` candidate lists. Nothing already
placed on it is touched, disturbed, or even inspected by cordon alone.

### `drain`: three cases, three different mechanisms

A node hosts three fundamentally different kinds of placed work, and — this
is the main design decision this milestone has to get right — they do not
share one reschedule mechanism today:

1. **Stateless `kind: Service` replicas** (no `template.volumes`) already
   self-heal once a node stops resolving: `ReconcileServices`'s
   `present_indices` computation excludes any placement whose node fails
   `registry.resolve` (`worker.rs:352-357`), so `diff_replicas` naturally
   schedules a replacement on next reconcile — which runs on *every*
   heartbeat from any node, this project's established piggyback
   self-healing pattern (the same spot Milestones 15/16/19's fencing hook
   into). `drain` needs no new reschedule logic for this case: it only
   needs to call the existing `Command::RemovePlacement` for each such
   replica pinned to the draining node. The very next `ReconcileServices`
   pass then sees it missing and places it on a schedulable node,
   automatically excluding the now-cordoned node via `is_schedulable`
   above. Because the node is still `Alive` (not actually dead), `drain`
   must also forward a `DELETE /jails/<replica_name>` to the draining node
   itself after removing the placement — unlike the Dead-node case, the
   old replica is still physically running there and won't tear itself
   down on its own.

2. **Stateful replicas** (`template.volumes` non-empty) already have a
   purpose-built, already-verified relocation mechanism: Milestone 19's
   standby-promotion path. `drain` reuses it directly rather than
   inventing a second one: it is, in effect, `force-repin` without waiting
   for `Dead` first. This needs one real change to `PrepareForceRepin`
   (`keel-controlplane/src/worker.rs`, the command `force-repin` already
   uses): today its Non-Goal-turned-guard step 3 in Milestone 19's design
   (`registry.resolve(current_node)` must fail, `409` if the node still
   resolves `Alive`) exists specifically to prevent promoting a standby
   out from under a still-live primary — the split-brain guard. `drain`
   needs a second, explicit entry path into the same promotion logic that
   is allowed to proceed against a *still-`Alive`, but cordoned-and-being-drained*
   node — the split-brain guard's actual invariant ("don't promote while
   the primary might still be independently running and reachable") holds
   just as well here, because `drain` itself explicitly fences the old
   primary's jail (tears it down via a direct `DELETE`) as part of the same
   operation, the same way Milestone 19's `PendingFences` mechanism fences
   a resurrected-from-disk jail after a reactive `force-repin`. Concretely:
   promote the standby (steps 5-7 of Milestone 19's `force-repin`,
   unchanged), then `DELETE /jails/<replica_name>` against the *old*
   primary directly and synchronously (not via `PendingFences`, since the
   node is confirmed reachable right now, unlike the reactive case which
   fences an unreachable node's *next* heartbeat).

3. **Plain `kind: Jail`** (not a `Service` replica, no `volumes`): this is
   the one case with **no existing self-heal or relocation mechanism at
   all**, and is new work this milestone actually has to design. A plain
   Jail's placement lives in the same `Placements` map as a Service
   replica's (`PUT /jails/<name>` with no explicit node goes through
   `Command::ResolveOrSchedule`, `http.rs:176,200`, then
   `Command::RecordPlacement`, `http.rs:336`, exactly like a Service
   replica's initial placement) but `ReconcileServices`'s `present_indices`
   self-heal is scoped to `services.list()` — a plain Jail is invisible to
   it. `drain` handles this the only way the existing primitives allow:
   `GET /jails/<name>` already proxies straight to whatever node currently
   holds the placement and returns the full `JailSpec` body
   (`handle_scheduled_read`, `http.rs:220-233`) — so drain fetches the
   spec from the old node, computes a fresh schedulable node via the same
   candidate-building logic `ResolveOrSchedule` uses (now `is_schedulable`-filtered),
   `PUT`s that same spec to the new node, and only on that PUT's success
   calls `DELETE` against the old node and updates `Placements`. This is
   new orchestration logic, not a reuse of an existing "move a Jail" path,
   because no such path exists yet.

`drain`'s overall sequence, run synchronously against `keelctl drain <node>`:

1. If `node` is not currently `Alive` (already past `DEAD_THRESHOLD`),
   refuse with an error directing the operator to `force-repin` instead
   (Non-Goal above) — there is nothing to *gracefully* drain from a node
   that's already gone.
2. Implicitly cordon the node if not already (insert into `Cordoned.ids`,
   persist), so nothing new lands on it mid-drain.
3. Refuse (`409`) if the node's most recent heartbeat lists any non-empty
   `ingresses` — see "Ingress interaction" below. Evaluated before any of
   the following steps begin, so a `409` here leaves the node completely
   untouched.
4. For each stateful replica pinned to the node: promote its standby
   (case 2 above), then fence the old primary directly.
5. For each stateless Service replica pinned to the node: remove its
   placement and delete the old jail (case 1 above); rely on the next
   `ReconcileServices` pass (any subsequent heartbeat) to place its
   replacement.
6. For each plain Jail pinned to the node: migrate it via the fetch/place-
   elsewhere/delete-old sequence (case 3 above).
7. Report success once every placement this node held at the start of the
   call has either moved or been confirmed torn down.

### Ingress interaction (contributed by BSD)

Originally contributed by BSD as a standalone section (PR #3,
`docs/superpowers/specs/2026-07-27-keel-milestone23-cordon-drain-ingress-interaction.md`),
written at Corentin's request ahead of the full design doc existing.
Folded in here so the full milestone reads as one design, and the
standalone file removed as part of this change to avoid two documents
describing the same interaction:

`kind: Ingress` (Milestone 21) is deliberately never control-plane-routed:
`keelctl run_apply`'s `Ingress` branch (`keelctl/src/main.rs:159-166`) posts
directly to the target node's `keel-agentd`, bypassing `jails_path`'s
`--node`-aware prefixing entirely. Nothing in `pick_node`/
`pick_node_for_service` is aware Ingress placement exists at all — it is
purely a manual, node-local, operator decision, and stays that way; this
milestone does not change that.

`keel-agentd::ensure_ingress_jail` (`keel-agentd/src/reconciler.rs:367`)
provisions that node's singleton nginx jail with a real, node-local
Let's Encrypt certificate. If `drain` only handled cases 1-3 above, it
would report a node "safe to take down" while silently stranding a live,
publicly-trusted HTTPS endpoint — a correctness gap in what "drained" is
supposed to mean.

The control plane is not actually blind to this, even though scheduling
is: every heartbeat already carries per-node Ingress health
(`Command::Heartbeat(..., heartbeat.ingresses, ...)`, `http.rs:618`, feeding
`Registry::heartbeat`, surfaced back out by `registry.list(now)` as each
node's `Vec<IngressHealth>` — the same data Milestone 22's dashboard
already renders). So `drain` needs no new query mechanism: before any of
the reschedule/promotion steps above run, it checks the node's current
`ingresses` from that same heartbeat data, and if non-empty, refuses
immediately with a `409` naming every stranded host:

```
409 Conflict
node-2 has 2 live Ingress host(s) that would be stranded by drain: example.com, stats.example.com.
Move them first: keelctl apply -f <ingress>.yaml --node <other-node>, then delete the old one, then retry drain.
```

Remediation is symmetric to how Ingress already works, requiring no new
primitives: apply the Ingress spec fresh to a replacement node (a new ACME
order against that node), delete the old one, then retry `drain`, which
now proceeds past the check.

Explicitly deferred (BSD's Non-Goals, carried forward unchanged): automatic
relocation of a live Ingress (a materially bigger scope add — a fresh
DNS-01 challenge and Let's Encrypt rate-limit consumption, not a data-plane
reschedule); any change to `pick_node`/`pick_node_for_service`'s existing
Ingress-blindness; any freshness guarantee beyond the last heartbeat; and
handling an Ingress applied to the node *during* an in-progress drain (an
accepted, named gap — the operator is working against their own
maintenance window).

Cordon alone does not gate on Ingress: cordoning only blocks *new*
scheduler-routed work, and Ingress was never scheduler-routed to begin
with, so a cordoned node with a live Ingress is unaffected until an
actual `drain` is attempted.

### Dashboard (Milestone 22)

`NodeSnapshot`/`NodeStatus` gains a `cordoned: bool` field, populated from
the new `Cordoned` set the same way `ingresses` already rides along on
`NodeStatus` today. `render_nodes` (`keel-dashboard/src/html.rs:46-66`)
gets one additional column rendered alongside the existing `{status:?}`
column, since cordoned is orthogonal to `Alive`/`Dead` and must not be
folded into that Debug-rendered enum.

## Error Handling

- **`cordon`/`uncordon`/`drain` against an unregistered node id:** `404`,
  the existing `UnknownNode` shape every other node-id-taking path uses.
- **`cordon` an already-cordoned node, or `uncordon` an already-schedulable
  one:** success, no-op — idempotent by design, no error.
- **`drain` a `Dead` node:** refused with a message pointing at
  `force-repin` (Non-Goal above), not silently treated as a successful
  drain of nothing.
- **`drain` a node with a live Ingress:** `409`, naming every stranded
  host, evaluated before any other side effect begins (BSD's section).
- **`drain` a node with a pinned stateful replica whose standby has never
  completed a first full replication** (`ReplicaTarget.last_snapshot` is
  `None`): same `409` Milestone 19's `force-repin` already returns for this
  case — nothing new to promote, this milestone does not weaken that
  guard just because the primary happens to still be reachable.
- **`drain` partway through, one migration step fails** (e.g. the new
  node picked for a plain Jail rejects the `PUT`): that one placement is
  left as-is on the original node, the old jail is *not* deleted (deletion
  only happens after the new placement's `PUT` succeeds), and `drain`
  reports which placement(s) failed to move rather than a blanket success
  or silent partial completion. The node is not implicitly uncordoned;
  the operator retries `drain` once the underlying failure (e.g. capacity)
  is addressed.
- **`uncordon` racing an in-progress `drain` of the same node:** `drain`'s
  own implicit re-cordon at the start of its run (step 2) means a
  concurrent `uncordon` can only affect scheduling *after* drain
  completes or is retried; it does not abort an in-flight drain.

## Testing Strategy

- **`keel-controlplane` unit tests** against the existing `FakeJailRuntime`-
  style harness the rest of the control plane's command tests already use:
  - `cordon`/`uncordon` round-trip: a cordoned node is excluded from
    `ResolveOrSchedule` and `ReconcileServices` candidates; an uncordoned
    one is included again; both persist across a simulated restart
    (load from `state_dir` the same way `Placements`/`Standbys` tests
    already verify persistence, e.g. the pattern in
    `keel-controlplane/src/registry.rs`'s and `worker.rs`'s existing
    restart-survival tests).
  - `drain` case 1 (stateless Service replica): placement removed, old
    jail's `DELETE` forwarded, next `ReconcileServices` pass places a
    replacement on a different, schedulable node.
  - `drain` case 2 (stateful replica): standby promoted via the same
    outcome `force-repin`'s own tests already assert on
    (`Placements`/`Standbys`/fencing updated identically), old primary's
    jail deleted directly and synchronously rather than via
    `PendingFences`.
  - `drain` case 3 (plain Jail): spec fetched from old node, placed on a
    new schedulable node, old node's jail deleted only after the new
    placement succeeds; a failure on the new `PUT` leaves the original
    placement and jail untouched.
  - `drain` refusals: `409` for a live Ingress (naming the exact hosts);
    `409` for a stateful replica whose standby has no completed
    replication; refusal (not silent success) against an already-`Dead`
    node.
  - Regression: a node with an empty `ingresses` list and nothing pinned
    to it drains as a trivial no-op success — proves this milestone adds
    nothing on the common case every prior milestone's tests already cover.
- **`keel-dashboard`:** `render_nodes` shows a cordoned column
  independently of `Alive`/`Dead`.
- **Real 3-node VM verification** (per this project's standing discipline
  of never assuming FreeBSD-specific behavior holds until proven on real
  hardware): stand up a 3-node cluster running a mix of a plain `kind: Jail`,
  a stateless `Service`, and a stateful `Service` with its standby on a
  second node. `cordon` one node hosting all three kinds and confirm
  nothing already running is disturbed while a fresh `apply` of a new
  replica lands only on the other two nodes. `drain` that node and confirm:
  the plain Jail reappears running on another node with the same spec; the
  stateless replica is recreated elsewhere within one heartbeat interval;
  the stateful replica's standby is promoted with its data intact and the
  old primary's jail is confirmed gone. Separately, apply a real Ingress
  with a real Let's Encrypt-staging cert to a node, attempt `drain` against
  it, and confirm it refuses naming that host rather than silently
  succeeding and leaving a dangling nginx jail with an orphaned cert behind;
  then perform the documented remediation and confirm `drain` succeeds
  once the Ingress is moved.

## Rollout / Sequencing

This is Sub-Project 11. CI enforcement (`cargo fmt`/`clippy`/`test` on
push, plus a `cargo-deny`/`cargo-audit` advisory scan) is real, separately
tracked hygiene work per the channel's resolution — it needs no design doc
or real-VM verification of its own and can land at any time, before,
during, or after this milestone, without contending for the milestone
number. Milestone 15's owed real-3-node-VM verification pass is likewise
independent and untouched by this work.

Implementation follows this design doc via the project's existing
convention: a task-by-task plan document under `docs/superpowers/plans/`,
then implementation with per-task review, same as every prior milestone.
