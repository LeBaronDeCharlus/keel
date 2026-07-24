use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Standbys {
    by_replica: HashMap<String, String>,
}

impl Standbys {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, replica_name: &str) -> Option<&str> {
        self.by_replica.get(replica_name).map(|s| s.as_str())
    }

    pub fn set(&mut self, replica_name: String, node_id: String) {
        self.by_replica.insert(replica_name, node_id);
    }

    pub fn remove(&mut self, replica_name: &str) {
        self.by_replica.remove(replica_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_on_an_empty_table_returns_none() {
        assert_eq!(Standbys::new().get("db-0"), None);
    }

    #[test]
    fn set_then_get_returns_the_recorded_node() {
        let mut standbys = Standbys::new();
        standbys.set("db-0".to_string(), "node-2".to_string());
        assert_eq!(standbys.get("db-0"), Some("node-2"));
    }

    #[test]
    fn set_again_overwrites_rather_than_duplicating() {
        let mut standbys = Standbys::new();
        standbys.set("db-0".to_string(), "node-2".to_string());
        standbys.set("db-0".to_string(), "node-3".to_string());
        assert_eq!(standbys.get("db-0"), Some("node-3"));
    }

    #[test]
    fn remove_clears_the_entry() {
        let mut standbys = Standbys::new();
        standbys.set("db-0".to_string(), "node-2".to_string());
        standbys.remove("db-0");
        assert_eq!(standbys.get("db-0"), None);
    }

    #[test]
    fn standbys_round_trips_through_yaml() {
        let mut standbys = Standbys::new();
        standbys.set("db-0".to_string(), "node-2".to_string());
        let path = std::env::temp_dir().join(format!("keel-controlplane-standbys-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &standbys).unwrap();
        let loaded: Standbys = crate::store::load_or_default(&path);
        assert_eq!(loaded.get("db-0"), Some("node-2"));
    }
}
