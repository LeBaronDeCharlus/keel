use crate::record::JailRecord;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("I/O error at {0}: {1}")]
    Io(PathBuf, io::Error),
    #[error("failed to parse state file {0}: {1}")]
    Parse(PathBuf, serde_yaml::Error),
}

pub fn load_all(state_dir: &Path) -> Result<Vec<JailRecord>, StoreError> {
    fs::create_dir_all(state_dir).map_err(|e| StoreError::Io(state_dir.to_path_buf(), e))?;
    let mut records = Vec::new();
    for entry in fs::read_dir(state_dir).map_err(|e| StoreError::Io(state_dir.to_path_buf(), e))? {
        let entry = entry.map_err(|e| StoreError::Io(state_dir.to_path_buf(), e))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let content = fs::read_to_string(&path).map_err(|e| StoreError::Io(path.clone(), e))?;
        let record: JailRecord =
            serde_yaml::from_str(&content).map_err(|e| StoreError::Parse(path.clone(), e))?;
        records.push(record);
    }
    Ok(records)
}

pub fn save(state_dir: &Path, record: &JailRecord) -> Result<(), StoreError> {
    fs::create_dir_all(state_dir).map_err(|e| StoreError::Io(state_dir.to_path_buf(), e))?;
    let path = state_dir.join(format!("{}.yaml", record.spec.metadata.name));
    let tmp_path = state_dir.join(format!("{}.yaml.tmp", record.spec.metadata.name));
    let content = serde_yaml::to_string(record).expect("JailRecord serialization should not fail");
    fs::write(&tmp_path, content).map_err(|e| StoreError::Io(tmp_path.clone(), e))?;
    fs::rename(&tmp_path, &path).map_err(|e| StoreError::Io(path.clone(), e))?;
    Ok(())
}

pub fn remove(state_dir: &Path, spec_name: &str) -> Result<(), StoreError> {
    let path = state_dir.join(format!("{spec_name}.yaml"));
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::Io(path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_spec::{Metadata, NetworkSpec, RestartPolicy, ResourcesSpec, Spec};

    fn sample_spec(name: &str) -> keel_spec::JailSpec {
        keel_spec::JailSpec {
            api_version: "keel/v1".to_string(),
            kind: "Jail".to_string(),
            metadata: Metadata { name: name.to_string() },
            spec: Spec {
                image: "base/14.2-web".to_string(),
                command: vec!["/usr/local/bin/myapp".to_string()],
                network: NetworkSpec {
                    vnet: true,
                    bridge: "keel0".to_string(),
                    address: "10.0.0.5/24".to_string(),
                },
                resources: ResourcesSpec { cpu: "2".to_string(), memory: "512M".to_string() },
                restart_policy: RestartPolicy::Always,
                volumes: vec![],
                replicate_to: None,
                generation: 0,
            },
        }
    }

    fn sample_record(name: &str) -> JailRecord {
        JailRecord { spec: sample_spec(name), epair_ordinal: 5, deleting: false }
    }

    fn test_state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-agentd-store-test-{name}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn save_then_load_all_roundtrips() {
        let dir = test_state_dir("save_then_load_all_roundtrips");
        let record = sample_record("web-1");
        save(&dir, &record).unwrap();
        let loaded = load_all(&dir).unwrap();
        assert_eq!(loaded, vec![record]);
    }

    #[test]
    fn load_all_on_missing_dir_creates_it_and_returns_empty() {
        let dir = test_state_dir("load_all_on_missing_dir_creates_it_and_returns_empty");
        assert!(!dir.exists());
        let loaded = load_all(&dir).unwrap();
        assert_eq!(loaded, vec![]);
        assert!(dir.exists());
    }

    #[test]
    fn remove_on_nonexistent_file_is_a_no_op_success() {
        let dir = test_state_dir("remove_on_nonexistent_file_is_a_no_op_success");
        fs::create_dir_all(&dir).unwrap();
        remove(&dir, "never-existed").unwrap();
    }

    #[test]
    fn save_then_remove_then_load_all_is_empty() {
        let dir = test_state_dir("save_then_remove_then_load_all_is_empty");
        let record = sample_record("web-1");
        save(&dir, &record).unwrap();
        remove(&dir, "web-1").unwrap();
        assert_eq!(load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn load_all_ignores_non_yaml_files() {
        let dir = test_state_dir("load_all_ignores_non_yaml_files");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("readme.txt"), "not a record").unwrap();
        assert_eq!(load_all(&dir).unwrap(), vec![]);
    }
}
