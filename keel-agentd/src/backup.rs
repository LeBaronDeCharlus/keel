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
    let final_path = zfs_dir.join(format!("{volume_name}.zfs"));
    let tmp_path = zfs_dir.join(format!("{volume_name}.zfs.tmp"));
    match send_snapshot_to_file(zfs, &dataset, backup_id, &tmp_path, &final_path) {
        Ok(()) => Ok(()),
        Err(e) => {
            // A half-written stream must never be left where `restore_agent_state`
            // would find it: it enumerates `*.zfs` and force-feeds each file to
            // `zfs receive` *after* destroying the live dataset, so a truncated
            // file there means destroying live data with nothing usable to put
            // back. Both cleanups are best-effort -- the send failure is what
            // the caller needs to hear about.
            let _ = std::fs::remove_file(&tmp_path);
            let _ = zfs.destroy_snapshot(&dataset, backup_id);
            Err(e)
        }
    }
}

/// Streams the snapshot into a `.zfs.tmp` file and only renames it onto its
/// final `.zfs` name once the whole send has succeeded -- the same
/// write-temp-then-rename idiom this project's state stores already use
/// (`keel-agentd/src/store.rs`, `keel-controlplane/src/store.rs`), so a
/// reader only ever sees a complete file.
fn send_snapshot_to_file<Z: ZfsManager>(
    zfs: &Z,
    dataset: &str,
    backup_id: &str,
    tmp_path: &std::path::Path,
    final_path: &std::path::Path,
) -> Result<(), BackupError> {
    let mut file = File::create(tmp_path)?;
    zfs.send_snapshot(dataset, backup_id, None, &mut file)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(tmp_path, final_path)?;
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

    // `send_snapshot`/`receive_snapshot` deliberately carry no ZFS
    // properties (no `-p`/`-o`, shared with Milestone 19's replication
    // path), so a received volume dataset comes back with *no* quota, and
    // `create_volume` won't fix it later: it no-ops on a dataset that
    // already exists. Without re-applying it here, every restored volume
    // would silently stop enforcing its declared `size:` forever. The sizes
    // come from the JailRecords this restore just put back on disk, read
    // the same way `Reconciler::new` reads them at startup.
    let declared_sizes: std::collections::HashMap<String, String> =
        crate::store::load_all(&state_dir)
            .map_err(crate::reconciler::ReconcileError::from)?
            .iter()
            .flat_map(|record| record.spec.spec.volumes.iter())
            .map(|volume| (volume.name.clone(), volume.size.clone()))
            .collect();

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
                // Recursive: every backup leaves a permanent snapshot on the
                // volume it backs up (no retention exists yet), so by the
                // time a volume is restored, its live dataset almost always
                // has at least one snapshot from a prior backup — a plain
                // (non-recursive) destroy fails on real ZFS with "filesystem
                // has children" in that case. See `destroy_dataset_recursive`'s
                // doc comment.
                zfs.destroy_dataset_recursive(&dataset)?;
            }
            let mut file = File::open(entry.path())?;
            zfs.receive_snapshot(&dataset, &mut file)?;
            match declared_sizes.get(volume_name) {
                Some(size) => zfs.set_quota(&dataset, size)?,
                // Shouldn't normally happen -- a volume and the record
                // declaring it are captured by the same backup -- so warn
                // loudly rather than abandoning an otherwise-good restore.
                None => eprintln!(
                    "keel-agentd: restored volume '{volume_name}' is not declared by any \
                     restored jail record; leaving it without a quota"
                ),
            }
        }
    }
    Ok(())
}

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
        assert!(state_dir.join("backups/backup-1/agent/web-1.yaml").exists());
        assert!(state_dir
            .join("backups/backup-1/zfs/web-1-data.zfs")
            .exists());
    }

    #[test]
    fn backup_agent_state_continues_past_one_failed_volume_and_still_backs_up_the_rest() {
        // Reproduces the design doc's Error Handling requirement: "A node
        // or the control plane that errors mid-backup (I/O error, `zfs
        // send` failure, disk full) is recorded as failed for that
        // specific dataset/component; other datasets... still complete
        // independently."
        //
        // Note: unlike a plain `zfs.destroy_dataset()` on the live volume
        // (which would just remove it from `Reconciler::list_volumes()`'s
        // `zfs list -d1` enumeration entirely, per the design doc, rather
        // than making it show up as an attempted-and-failed volume), this
        // forces a genuine per-volume I/O error by occupying web-2-data's
        // destination path with a directory ahead of time, while web-1's
        // dataset and destination path are untouched and still backable.
        // The occupied path is the `.zfs.tmp` staging name, since that's
        // what `File::create` now opens first.
        let (mut reconciler, zfs) = test_reconciler("backup_partial_failure");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        reconciler.apply(stateful_spec("web-2")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        let zfs_dir = reconciler.state_dir().join("backups/backup-1/zfs");
        std::fs::create_dir_all(zfs_dir.join("web-2-data.zfs.tmp")).unwrap();

        let result = backup_agent_state(&reconciler, &zfs, "zroot", "backup-1").unwrap();
        assert_eq!(result.volumes, vec!["web-1-data".to_string()]);
        assert_eq!(result.failed_volumes, vec!["web-2-data".to_string()]);
        assert!(reconciler
            .state_dir()
            .join("backups/backup-1/zfs/web-1-data.zfs")
            .exists());
    }

    #[test]
    fn a_failed_volume_backup_leaves_no_zfs_file_for_restore_to_find_and_no_orphan_snapshot() {
        // The send used to write straight to the final `<volume>.zfs` path,
        // so a send that died partway (disk full, I/O error) left a
        // truncated file there -- and `restore_agent_state` destroys the
        // live dataset *before* feeding each `*.zfs` file to `zfs receive`,
        // so that truncated file meant destroying live data with nothing
        // restorable to put back.
        let (mut reconciler, zfs) = test_reconciler("backup_failure_leaves_no_zfs_file");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        let zfs_dir = reconciler.state_dir().join("backups/backup-1/zfs");
        // Fault injection: occupy the staging path `File::create` opens
        // first, so the send fails before a single byte is written.
        std::fs::create_dir_all(zfs_dir.join("web-1-data.zfs.tmp")).unwrap();

        let result = backup_agent_state(&reconciler, &zfs, "zroot", "backup-1").unwrap();
        assert_eq!(result.failed_volumes, vec!["web-1-data".to_string()]);

        assert!(
            !zfs_dir.join("web-1-data.zfs").exists(),
            "a failed send must not leave anything at the final .zfs path"
        );
        // The only leftover is the directory this test deliberately put at
        // the staging path; the cleanup's `remove_file` can't remove a
        // directory, and deliberately doesn't try harder -- it's not a
        // `.zfs` file, so restore never looks at it.
        assert!(zfs_dir.join("web-1-data.zfs.tmp").is_dir());

        // The snapshot taken for this volume is cleaned up too, so a failed
        // backup doesn't strand one on the pool.
        assert!(
            matches!(
                zfs.destroy_snapshot("zroot/keel/volumes/web-1-data", "backup-1"),
                Err(keel_zfs::ZfsError::NotFound(_))
            ),
            "expected the failed volume's snapshot to have been destroyed already"
        );
    }

    #[test]
    fn restore_agent_state_reapplies_each_restored_volumes_declared_quota() {
        // `zfs receive` carries no properties, so a restored volume dataset
        // has no quota at all until restore re-applies the size its
        // (restored) JailRecord declares -- and nothing else ever will,
        // since `create_volume` no-ops on an existing dataset.
        let (mut reconciler, zfs) = test_reconciler("restore_reapplies_quota");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        assert_eq!(
            zfs.quota_of("zroot/keel/volumes/web-1-data"),
            Some("1G".to_string()),
            "sanity: provisioning applies the declared size at create time"
        );
        backup_agent_state(&reconciler, &zfs, "zroot", "backup-1").unwrap();

        restore_agent_state(&mut reconciler, &zfs, "zroot", "backup-1").unwrap();

        assert_eq!(
            zfs.quota_of("zroot/keel/volumes/web-1-data"),
            Some("1G".to_string()),
            "a restored volume must get its declared size re-applied as a quota"
        );
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
        assert!(reconciler.get("web-2", std::time::Instant::now()).is_some());

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
    fn restore_agent_state_succeeds_against_a_volume_backed_up_more_than_once() {
        // Reproduces a bug found during Milestone 24's real FreeBSD VM
        // verification: every backup leaves a permanent snapshot on the
        // volume it backs up (no retention exists yet), so a volume that
        // has been backed up two or more times has multiple snapshots by
        // the time it's restored. `restore_agent_state` used to call
        // `zfs.destroy_dataset` (non-recursive) on the live dataset before
        // receiving, which on real ZFS fails with "filesystem has
        // children" whenever any snapshot is present -- reliably
        // reproduced on real hardware, invisible here until
        // `FakeZfsManager::destroy_dataset` was made to accurately model
        // that same real-ZFS behavior.
        let (mut reconciler, zfs) = test_reconciler("restore_after_two_backups");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        backup_agent_state(&reconciler, &zfs, "zroot", "backup-1").unwrap();
        backup_agent_state(&reconciler, &zfs, "zroot", "backup-2").unwrap();

        restore_agent_state(&mut reconciler, &zfs, "zroot", "backup-2").unwrap();

        assert!(zfs.dataset_exists("zroot/keel/volumes/web-1-data").unwrap());
    }

    #[test]
    fn restore_agent_state_on_an_unknown_backup_id_fails_without_tearing_anything_down() {
        let (mut reconciler, zfs) = test_reconciler("restore_unknown");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());

        let result = restore_agent_state(&mut reconciler, &zfs, "zroot", "missing-backup");
        assert!(matches!(result, Err(BackupError::UnknownBackup(_))));
        assert!(reconciler.get("web-1", std::time::Instant::now()).is_some());
    }
}
