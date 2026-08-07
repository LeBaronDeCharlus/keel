use std::io;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackupError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("no backup '{0}' found")]
    UnknownBackup(String),
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

pub fn backup_control_plane_state(state_dir: &Path, backup_id: &str) -> Result<(), BackupError> {
    let dest = state_dir
        .join("backups")
        .join(backup_id)
        .join("controlplane");
    keel_spec::fs_copy::copy_dir_recursive(state_dir, &dest, SKIP_TOP_LEVEL)?;
    Ok(())
}

pub fn restore_control_plane_state(state_dir: &Path, backup_id: &str) -> Result<(), BackupError> {
    let src = state_dir
        .join("backups")
        .join(backup_id)
        .join("controlplane");
    // Validate before destroying anything live: if this backup's
    // control-plane step never completed (or its directory was removed),
    // wiping first would empty the live state dir and then fail on the
    // copy, leaving nothing to restore from. Mirrors the same
    // check-before-teardown guard `keel_agentd::backup::restore_agent_state`
    // already does on `backups/<id>/agent`.
    if !src.is_dir() {
        return Err(BackupError::UnknownBackup(backup_id.to_string()));
    }
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
    manifests.sort_by(
        |a: &crate::wire::BackupManifest, b: &crate::wire::BackupManifest| a.id.cmp(&b.id),
    );
    manifests
}

pub fn get_manifest(state_dir: &Path, backup_id: &str) -> Option<crate::wire::BackupManifest> {
    let manifest_path = state_dir
        .join("backups")
        .join(backup_id)
        .join("manifest.yaml");
    let content = std::fs::read_to_string(manifest_path).ok()?;
    serde_yaml::from_str(&content).ok()
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
    fn restore_control_plane_state_on_a_missing_backup_leaves_live_state_untouched() {
        // The wipe used to run before anything validated that the backup's
        // `controlplane/` directory existed at all, so restoring an id whose
        // control-plane step had failed (or never existed) emptied the live
        // state dir and *then* errored -- total loss with nothing to restore
        // from.
        let state_dir = fresh_state_dir("restore_missing");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::write(state_dir.join("placements.yaml"), "web-0: node-1\n").unwrap();

        let result = restore_control_plane_state(&state_dir, "never-taken");
        assert!(
            matches!(result, Err(BackupError::UnknownBackup(_))),
            "expected UnknownBackup, got: {result:?}"
        );
        assert_eq!(
            std::fs::read_to_string(state_dir.join("placements.yaml")).unwrap(),
            "web-0: node-1\n",
            "live control-plane state must survive a restore of an unusable backup"
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
}
