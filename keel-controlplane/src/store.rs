use std::fs;
use std::io;
use std::path::Path;

pub fn load_or_default<T: Default + serde::de::DeserializeOwned>(path: &Path) -> T {
    match fs::read_to_string(path) {
        Ok(content) => serde_yaml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse state file {}: {e}", path.display())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => T::default(),
        Err(e) => panic!("failed to read state file {}: {e}", path.display()),
    }
}

pub fn save<T: serde::Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("yaml.tmp");
    let content = serde_yaml::to_string(value).expect("state serialization should not fail");
    fs::write(&tmp_path, content)?;
    fs::rename(&tmp_path, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct Scratch {
        names: Vec<String>,
        count: u32,
    }

    fn test_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-controlplane-store-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        dir.join(format!("{name}.yaml"))
    }

    #[test]
    fn save_then_load_or_default_roundtrips() {
        let path = test_path("save_then_load_or_default_roundtrips");
        let value = Scratch { names: vec!["a".to_string(), "b".to_string()], count: 2 };
        save(&path, &value).unwrap();
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, value);
    }

    #[test]
    fn load_or_default_on_a_missing_file_returns_default() {
        let path = test_path("load_or_default_on_a_missing_file_returns_default");
        let _ = fs::remove_file(&path);
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, Scratch::default());
    }

    #[test]
    fn save_creates_the_parent_directory_if_missing() {
        let dir = std::env::temp_dir().join(format!("keel-controlplane-store-test-missing-parent-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("scratch.yaml");
        let value = Scratch { names: vec![], count: 0 };
        save(&path, &value).unwrap();
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, value);
    }

    #[test]
    fn save_overwrites_a_previous_value_rather_than_merging() {
        let path = test_path("save_overwrites_a_previous_value_rather_than_merging");
        save(&path, &Scratch { names: vec!["old".to_string()], count: 1 }).unwrap();
        let new_value = Scratch { names: vec!["new".to_string()], count: 2 };
        save(&path, &new_value).unwrap();
        let loaded: Scratch = load_or_default(&path);
        assert_eq!(loaded, new_value);
    }
}
