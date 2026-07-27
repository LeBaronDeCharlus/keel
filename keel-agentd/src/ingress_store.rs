use crate::ingress_record::IngressRecord;
use crate::store::StoreError;
use std::fs;
use std::path::{Path, PathBuf};

fn dir(state_dir: &Path) -> PathBuf {
    state_dir.join("ingress")
}

pub fn load_all(state_dir: &Path) -> Result<Vec<IngressRecord>, StoreError> {
    let dir = dir(state_dir);
    fs::create_dir_all(&dir).map_err(|e| StoreError::Io(dir.clone(), e))?;
    let mut records = Vec::new();
    for entry in fs::read_dir(&dir).map_err(|e| StoreError::Io(dir.clone(), e))? {
        let entry = entry.map_err(|e| StoreError::Io(dir.clone(), e))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let content = fs::read_to_string(&path).map_err(|e| StoreError::Io(path.clone(), e))?;
        let record: IngressRecord =
            serde_yaml::from_str(&content).map_err(|e| StoreError::Parse(path.clone(), e))?;
        records.push(record);
    }
    Ok(records)
}

pub fn save(state_dir: &Path, record: &IngressRecord) -> Result<(), StoreError> {
    let dir = dir(state_dir);
    fs::create_dir_all(&dir).map_err(|e| StoreError::Io(dir.clone(), e))?;
    let path = dir.join(format!("{}.yaml", record.spec.metadata.name));
    let tmp_path = dir.join(format!("{}.yaml.tmp", record.spec.metadata.name));
    let content =
        serde_yaml::to_string(record).expect("IngressRecord serialization should not fail");
    fs::write(&tmp_path, content).map_err(|e| StoreError::Io(tmp_path.clone(), e))?;
    fs::rename(&tmp_path, &path).map_err(|e| StoreError::Io(path.clone(), e))?;
    Ok(())
}

pub fn remove(state_dir: &Path, spec_name: &str) -> Result<(), StoreError> {
    let path = dir(state_dir).join(format!("{spec_name}.yaml"));
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::Io(path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_spec::{IngressBackend, IngressSpecBody, IngressTls, Metadata};

    fn test_state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-agentd-ingress-store-test-{name}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn sample(name: &str) -> IngressRecord {
        IngressRecord {
            spec: keel_spec::IngressSpec {
                api_version: "keel/v1".to_string(),
                kind: "Ingress".to_string(),
                metadata: Metadata {
                    name: name.to_string(),
                },
                spec: IngressSpecBody {
                    host: "example.com".to_string(),
                    backend: IngressBackend {
                        service: "hugo-site".to_string(),
                        port: 8080,
                    },
                    tls: IngressTls {
                        email: "admin@example.com".to_string(),
                    },
                },
            },
            cert_expires_at_unix: None,
        }
    }

    #[test]
    fn save_then_load_all_roundtrips() {
        let dir = test_state_dir("save_then_load_all_roundtrips");
        let record = sample("blog");
        save(&dir, &record).unwrap();
        assert_eq!(load_all(&dir).unwrap(), vec![record]);
    }

    #[test]
    fn save_then_remove_then_load_all_is_empty() {
        let dir = test_state_dir("save_then_remove_then_load_all_is_empty");
        let record = sample("blog");
        save(&dir, &record).unwrap();
        remove(&dir, "blog").unwrap();
        assert_eq!(load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn ingress_records_live_in_their_own_subdirectory_not_alongside_jail_records() {
        let dir = test_state_dir("ingress_records_live_in_their_own_subdirectory");
        save(&dir, &sample("blog")).unwrap();
        assert_eq!(crate::store::load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn save_overwrites_rather_than_duplicating() {
        let dir = test_state_dir("save_overwrites_rather_than_duplicating");
        let mut record = sample("blog");
        save(&dir, &record).unwrap();
        record.cert_expires_at_unix = Some(1_800_000_000);
        save(&dir, &record).unwrap();
        let loaded = load_all(&dir).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].cert_expires_at_unix, Some(1_800_000_000));
    }
}
