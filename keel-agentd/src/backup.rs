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
        // destination `.zfs` file path with a directory ahead of time,
        // while web-1's dataset and destination path are untouched and
        // still backable.
        let (mut reconciler, zfs) = test_reconciler("backup_partial_failure");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        reconciler.apply(stateful_spec("web-2")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());
        let zfs_dir = reconciler.state_dir().join("backups/backup-1/zfs");
        std::fs::create_dir_all(zfs_dir.join("web-2-data.zfs")).unwrap();

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
    fn restore_agent_state_on_an_unknown_backup_id_fails_without_tearing_anything_down() {
        let (mut reconciler, zfs) = test_reconciler("restore_unknown");
        reconciler.apply(stateful_spec("web-1")).unwrap();
        let _ = reconciler.reconcile(std::time::Instant::now());

        let result = restore_agent_state(&mut reconciler, &zfs, "zroot", "missing-backup");
        assert!(matches!(result, Err(BackupError::UnknownBackup(_))));
        assert!(reconciler.get("web-1", std::time::Instant::now()).is_some());
    }
}
