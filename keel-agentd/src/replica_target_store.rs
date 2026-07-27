use crate::replica_target::ReplicaTarget;
use crate::store::StoreError;
use std::fs;
use std::path::Path;

fn dir(state_dir: &Path) -> std::path::PathBuf {
    state_dir.join("replica-targets")
}

pub fn load_all(state_dir: &Path) -> Result<Vec<ReplicaTarget>, StoreError> {
    let dir = dir(state_dir);
    fs::create_dir_all(&dir).map_err(|e| StoreError::Io(dir.clone(), e))?;
    let mut targets = Vec::new();
    for entry in fs::read_dir(&dir).map_err(|e| StoreError::Io(dir.clone(), e))? {
        let entry = entry.map_err(|e| StoreError::Io(dir.clone(), e))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let content = fs::read_to_string(&path).map_err(|e| StoreError::Io(path.clone(), e))?;
        let target: ReplicaTarget =
            serde_yaml::from_str(&content).map_err(|e| StoreError::Parse(path.clone(), e))?;
        targets.push(target);
    }
    Ok(targets)
}

pub fn save(state_dir: &Path, target: &ReplicaTarget) -> Result<(), StoreError> {
    let dir = dir(state_dir);
    fs::create_dir_all(&dir).map_err(|e| StoreError::Io(dir.clone(), e))?;
    let path = dir.join(format!("{}.yaml", target.replica_name));
    let tmp_path = dir.join(format!("{}.yaml.tmp", target.replica_name));
    let content =
        serde_yaml::to_string(target).expect("ReplicaTarget serialization should not fail");
    fs::write(&tmp_path, content).map_err(|e| StoreError::Io(tmp_path.clone(), e))?;
    fs::rename(&tmp_path, &path).map_err(|e| StoreError::Io(path.clone(), e))?;
    Ok(())
}

/// Deletes `replica_name`'s persisted `ReplicaTarget`, if any. No path in
/// this codebase ever called this before it existed: a standby's
/// bookkeeping for a replica accumulated on disk forever once created,
/// with no eviction mechanism at all. Idempotent -- removing an
/// already-absent target is a no-op success, not an error, matching this
/// project's usual "safe to call more than once" convention for teardown.
pub fn remove(state_dir: &Path, replica_name: &str) -> Result<(), StoreError> {
    let path = dir(state_dir).join(format!("{replica_name}.yaml"));
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::Io(path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("keel-agentd-replica-target-store-test-{name}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn sample(name: &str) -> ReplicaTarget {
        ReplicaTarget {
            replica_name: name.to_string(),
            volume_dataset: format!("zroot/keel/volumes/{name}-data"),
            source_node_addr: "10.0.0.4:7621".to_string(),
            last_snapshot: None,
            highest_generation_seen: 0,
        }
    }

    #[test]
    fn save_then_load_all_roundtrips() {
        let dir = test_state_dir("save_then_load_all_roundtrips");
        let target = sample("db-0");
        save(&dir, &target).unwrap();
        assert_eq!(load_all(&dir).unwrap(), vec![target]);
    }

    #[test]
    fn load_all_on_missing_dir_creates_it_and_returns_empty() {
        let dir = test_state_dir("load_all_on_missing_dir_creates_it_and_returns_empty");
        assert_eq!(load_all(&dir).unwrap(), vec![]);
        assert!(dir.join("replica-targets").exists());
    }

    #[test]
    fn replica_targets_live_in_their_own_subdirectory_not_alongside_jail_records() {
        let dir = test_state_dir("replica_targets_live_in_their_own_subdirectory");
        let target = sample("db-0");
        save(&dir, &target).unwrap();

        // A JailRecord loader pointed at the same top-level state_dir must
        // see nothing here -- proving replica targets don't collide with
        // `store::load_all`'s own `.yaml` scan of `state_dir` itself.
        assert_eq!(crate::store::load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn remove_deletes_a_persisted_target() {
        let dir = test_state_dir("remove_deletes_a_persisted_target");
        let target = sample("db-0");
        save(&dir, &target).unwrap();
        assert_eq!(load_all(&dir).unwrap(), vec![target]);

        remove(&dir, "db-0").unwrap();

        assert_eq!(load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn remove_on_an_already_absent_target_is_a_no_op_success() {
        let dir = test_state_dir("remove_on_an_already_absent_target_is_a_no_op_success");
        assert!(remove(&dir, "never-existed").is_ok());
    }

    #[test]
    fn remove_only_deletes_the_named_target_not_others() {
        let dir = test_state_dir("remove_only_deletes_the_named_target_not_others");
        save(&dir, &sample("db-0")).unwrap();
        save(&dir, &sample("db-1")).unwrap();

        remove(&dir, "db-0").unwrap();

        let remaining = load_all(&dir).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].replica_name, "db-1");
    }

    #[test]
    fn save_overwrites_rather_than_duplicating() {
        let dir = test_state_dir("save_overwrites_rather_than_duplicating");
        let mut target = sample("db-0");
        save(&dir, &target).unwrap();
        target.last_snapshot = Some("keel-repl-1".to_string());
        save(&dir, &target).unwrap();

        let loaded = load_all(&dir).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].last_snapshot, Some("keel-repl-1".to_string()));
    }
}
