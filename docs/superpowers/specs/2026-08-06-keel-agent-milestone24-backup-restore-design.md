# Milestone 24: Cluster Backup and Restore (Sub-Project 12)

Status: Implemented

Date: 2026-08-06

## Context

The README's roadmap has no queued sub-project after Milestone 23 (cordon
and drain, Sub-Project 11, designed and planned but not yet implemented);
this is a fresh proposal for a separate, independent sub-project, brainstormed
directly with Corentin rather than picked off an existing backlog.

Today, keel has no way to recover from data loss short of the mechanisms
each individual milestone happened to build for its own purposes: Milestone
19 gave stateful replicas a standby via continuous `zfs send`/`receive`
replication, and Milestone 22 gave operators read-only visibility into
current state, but there is no way to capture a point-in-time copy of
*everything* (control-plane state and volume data) and restore it later.
An operator who deletes the wrong volume, or whose control-plane state file
gets corrupted, has no recovery path today beyond what Milestone 19's
per-replica standby happens to cover.

## Goals

- An operator can run one command to capture a point-in-time, cluster-wide
  backup: the control plane's own persisted state, every node's persisted
  agent state, and every node's persistent volume data.
- An operator can run one command to restore a previously captured backup
  onto the *same* cluster (same control plane, same nodes) it was taken
  from.
- Backup and restore are cluster-wide operations triggered from one place
  (the control plane), even though the resulting artifacts live locally on
  each component's own disk.
- Partial failure (a node down during backup, or a corrupt/missing backup
  during restore) is reported clearly, never silently swallowed.

## Non-Goals

- Shipping backups off-node (to another node, or to remote/object storage).
  This milestone is local-disk-only; off-node shipping is future work once
  a transport is actually needed.
- Scheduled/automatic backups. On-demand only; a timer-driven backup loop
  is future work.
- Disaster recovery onto a *fresh* cluster (new nodes, new control plane).
  This milestone only restores onto the same topology it backed up.
- Partial restore (restoring a single volume or jail out of a larger
  backup). This milestone is full-cluster restore only; partial restore is
  explicitly deferred, though the on-disk layout (per-node, per-dataset
  files under a single backup ID) is chosen so it doesn't foreclose partial
  restore later.
- Incremental ZFS sends. Every backup takes a full `zfs send` of each
  volume dataset, the same way Milestone 19's replication takes its first
  snapshot of a new standby. Incremental backups are future work.

## Architecture

### Backup ID

The control plane generates one timestamp-based backup ID per
`backup create` call (e.g. `2026-08-06T12-34-56Z`) and passes it to every
node it fans out to, so every artifact produced by that one backup
operation - across the control plane and every node - shares the same ID
and can be correlated later.

### What gets captured, and where

**Control-plane state.** `keel-controlplane`'s entire `state_dir` (today:
`placements.yaml`, `used_addresses.yaml`, `standbys.yaml`,
`pending_fences.yaml`, `services.yaml` - see `keel-controlplane/src/store.rs`
and `keel-controlplane/src/services.rs`) is copied, as a tree, into
`<state_dir>/backups/<id>/controlplane/`. Copying the whole tree rather
than naming files individually means a future new persisted collection is
automatically included without the backup code needing to change. The node
registry itself is not captured - it isn't persisted today (nodes
re-register on heartbeat; see `keel-controlplane/src/registry.rs`), so
there is nothing to back up there.

**Per-node agent state.** Each node's `keel-agentd` `state_dir` (JailRecords
at the top level - `keel-agentd/src/store.rs` - plus the `replica-targets/`
and `ingress/` subdirectories - `keel-agentd/src/replica_target_store.rs`,
`keel-agentd/src/ingress_store.rs`) is copied the same way, into that node's
own local `<state_dir>/backups/<id>/agent/`.

**Volume data.** For every dataset under `<pool>/keel/volumes/*` on a node
(enumerated with the same `ZfsManager::list_child_datasets` call
`Reconciler::list_volumes` already uses, `keel-agentd/src/reconciler.rs`),
take a ZFS snapshot named `<id>` and `send_snapshot` it
(`keel-zfs/src/lib.rs`) to a local file at
`<state_dir>/backups/<id>/zfs/<volume-name>.zfs`. `send_snapshot` and
`receive_snapshot` already take a generic `Write`/`Read` sink rather than
a TCP-specific type, so no new `keel-zfs` API is needed here - only a
`File` passed in where Milestone 19's replication passes a TCP stream.

Jail *rootfs* datasets (`<pool>/keel/jails/*`, `record::jail_dataset_path`)
are deliberately **not** snapshotted. They're ZFS clones of a base image
that the reconciler already knows how to recreate from a JailRecord/spec
during normal provisioning (`clone_from_base`); backing them up would just
be redundant bytes that restore would immediately discard and recreate
anyway.

### Backup flow

1. `keelctl backup create` sends `POST /backup` to the control plane.
2. The control plane generates the backup ID and backs up its own state as
   described above.
3. The control plane calls `POST /backup {id}` on every currently-Alive
   node, reusing the same direct, per-node mTLS HTTP push
   `handle_force_repin` already uses to reach a specific node
   (`keel-controlplane/src/http.rs`) - no new transport mechanism.
4. Each node backs up its own agent state and volume snapshots, and
   responds with per-dataset success/failure.
5. The control plane aggregates every node's response (plus its own
   result) into `<state_dir>/backups/<id>/manifest.yaml`: which nodes and
   datasets succeeded, which were skipped (node not Alive) or failed (e.g.
   I/O error, out of space). `keelctl` prints this summary.

This is not an atomic distributed transaction - it can't be, across
independently-failing nodes. A node that's Dead or errors mid-backup is
recorded as failed/skipped in the manifest, visibly, rather than silently
dropped from an otherwise-successful-looking backup.

### Restore flow

Restore is destructive - it overwrites live state and volume data - so
`keelctl restore <id>` requires an explicit `--yes` flag, and the control
plane responds 404 if `<id>` doesn't match a backup it knows about.

1. `keelctl restore <id> --yes` sends `POST /restore {id}` to the control
   plane.
2. The control plane fans the same `{id}` out to every node it can reach,
   the same way `backup create` does.
3. On each node, restore reuses the *existing* teardown/provision code
   paths rather than new ones:
   - Tear down every jail this node currently manages and unmount its
     volumes - the same path an explicit delete already uses.
   - Restore the node's agent `state_dir` files from
     `backups/<id>/agent/`.
   - For every volume dataset present in the backup, destroy the live
     dataset (if it exists) and `receive_snapshot` from the saved
     `backups/<id>/zfs/<volume-name>.zfs` file. A volume dataset that
     exists live but has no corresponding file in the backup (e.g. created
     after the backup was taken) is left untouched, not destroyed - it
     simply won't be referenced by any restored JailRecord unless a
     surviving jail spec still points at it.
   - Requires an operator-triggered restart of `keel-agentd` afterward to
     load the restored `JailRecords` into memory: `Reconciler` only reads
     `state_dir` once, inside `Reconciler::new` at process startup
     (`keel-agentd/src/reconciler.rs`) - restoring the files on disk while
     the process keeps running would leave its in-memory `records` map
     (already emptied by the teardown step above) unaware of the restored
     records, so nothing would reconcile them back into existence. This is
     the same constraint the control plane already has (see step 4 below),
     not a new one introduced here. Once restarted, the reconciler's first
     reconcile pass recreates every jail from the restored `JailRecords`,
     exactly like normal provisioning: clone rootfs fresh from the base
     image, remount the now-restored volumes.
4. The control plane restores its own state files from
   `backups/<id>/controlplane/` the same way, and requires an
   operator-triggered restart afterward to pick them up - consistent with
   how it already only reads this state at process startup
   (`keel-controlplane/src/main.rs`); no new hot-reload mechanism is
   introduced for this milestone.

### CLI surface

- `keelctl backup create` - triggers a cluster-wide backup, prints the
  resulting manifest summary.
- `keelctl backup list` - reads and prints known backup manifests.
- `keelctl restore <id> --yes` - triggers a cluster-wide restore; refuses
  to run without `--yes`. Prints a reminder that a restart of
  `keel-controlplane` and every restored node's `keel-agentd` is required
  before the restored state takes effect (see the Restore flow's steps 3
  and 4).

## Known Limitations

A backup or restore runs on `keel-agentd`'s single reconciler worker
thread, and holds it for the whole operation - a state-dir copy plus a
full `zfs send`/`receive` per volume. While that's in flight the node
sends no heartbeats, so the control plane's 15-second liveness window
expires and the node shows up as `Dead` in `keelctl get nodes` and in the
dashboard for the entire duration of a large backup, even though it is
perfectly healthy and doing exactly what it was asked to do. (A node
marked `Dead` mid-backup is also skipped by any *other* cluster-wide
operation that fans out only to Alive nodes while it's busy.) This is a
known limitation, not a bug: nothing is lost or corrupted, and the node
resumes heartbeating as soon as the operation completes. A future
milestone could remove it by moving the ZFS I/O off the worker thread
(e.g. a dedicated backup thread the worker hands the job to, with the HTTP
handler polling for completion), which is a large enough change to the
agent's concurrency model to deserve its own task rather than being folded
into this one.

## Error Handling

- A node that is not Alive when `backup create` runs is recorded in the
  manifest as skipped, not silently omitted.
- A node or the control plane that errors mid-backup (I/O error, `zfs
  send` failure, disk full) is recorded as failed for that specific
  dataset/component; other datasets and other nodes still complete
  independently.
- `restore` on an unknown backup ID returns 404 before anything is torn
  down on any node.
- `restore` never runs without `--yes` from the operator, given it
  destroys live state and volume data on every node it reaches.
- A node unreachable during `restore` is reported as not restored for that
  node; the operator decides whether to retry once it's back.

## Testing Strategy

Following this project's existing discipline: unit and integration tests
against `FakeZfsManager` and fake node HTTP clients first (mirroring how
Milestone 19's replication and Milestone 17's volumes were tested), then
real verification on the FreeBSD VM(s) already used for prior milestones:

- Create a backup, destroy a volume's live data, restore, and confirm the
  restored content is byte-identical to what was backed up.
- Kill a node partway through a cluster-wide backup and confirm the
  manifest reports it as failed/skipped rather than the backup looking
  complete.
- Restore onto a node whose live JailRecords differ from the backup (e.g.
  a jail created after the backup was taken) and confirm that jail is torn
  down as part of restoring to the backup's point in time.
- Attempt `restore` on an unknown ID and confirm a 404 with no side
  effects on any node.

**Real-VM verification (2026-08-07): done, all four cases above confirmed**,
plus a full 3-node cluster-wide backup and restore (`node-1`/`node-2`/`node-3`
on separate FreeBSD 15.1 VMs, distinct volume data on each, restored
byte-identical and independently per node). Found and fixed one real bug
invisible to every fake-backed test: every backup leaves a permanent ZFS
snapshot on the volume it backs up (no retention exists yet), so restoring
a volume that had ever been backed up before always failed on real ZFS
with `cannot destroy ...: filesystem has children` (a plain, non-recursive
`zfs destroy`). Fixed by adding `ZfsManager::destroy_dataset_recursive`
(`zfs destroy -r`), used only by restore's pre-receive cleanup, and by
making `FakeZfsManager::destroy_dataset` accurately model real ZFS's
snapshot-blocks-destroy behavior so this class of bug can't hide again.

## Rollout / Sequencing

This is a new, independent sub-project; it does not depend on Milestone 23
(cordon/drain) and can be implemented before, after, or in parallel with
it. Suggested task breakdown for the implementation plan:

1. Control-plane state backup/restore (`POST /backup`, `POST /restore` on
   `keel-controlplane`, local state-dir copy, manifest writing).
2. Per-node agent state backup/restore (`POST /backup`, `POST /restore` on
   `keel-agentd`, local state-dir copy).
3. Per-node volume snapshot/send-to-file and receive-from-file, reusing
   `ZfsManager::send_snapshot`/`receive_snapshot` against a file sink
   instead of a TCP stream.
4. Control-plane fan-out and manifest aggregation for both backup and
   restore.
5. `keelctl backup create` / `backup list` / `restore <id> --yes`.
6. Real single-node, then multi-node, FreeBSD VM verification.
