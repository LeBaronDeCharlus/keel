use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PendingFences {
    by_replica: HashMap<String, String>, // replica_name -> node_id owed a forced delete
}

impl PendingFences {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, replica_name: String, node_id: String) {
        self.by_replica.insert(replica_name, node_id);
    }

    pub fn remove(&mut self, replica_name: &str) {
        self.by_replica.remove(replica_name);
    }

    /// Every replica_name currently owed a forced delete on `node_id`.
    pub fn for_node(&self, node_id: &str) -> Vec<String> {
        self.by_replica
            .iter()
            .filter(|(_, owed_node)| owed_node.as_str() == node_id)
            .map(|(name, _)| name.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_node_on_an_empty_table_is_empty() {
        assert_eq!(PendingFences::new().for_node("node-1"), Vec::<String>::new());
    }

    #[test]
    fn for_node_finds_only_entries_owed_on_that_node() {
        let mut fences = PendingFences::new();
        fences.set("db-0".to_string(), "node-1".to_string());
        fences.set("db-1".to_string(), "node-2".to_string());
        assert_eq!(fences.for_node("node-1"), vec!["db-0".to_string()]);
    }

    #[test]
    fn remove_clears_the_entry() {
        let mut fences = PendingFences::new();
        fences.set("db-0".to_string(), "node-1".to_string());
        fences.remove("db-0");
        assert_eq!(fences.for_node("node-1"), Vec::<String>::new());
    }

    #[test]
    fn a_node_with_no_owed_fences_gets_an_empty_result_not_every_entry() {
        let mut fences = PendingFences::new();
        fences.set("db-0".to_string(), "node-1".to_string());
        assert_eq!(fences.for_node("node-9"), Vec::<String>::new());
    }

    #[test]
    fn pending_fences_round_trips_through_yaml() {
        let mut fences = PendingFences::new();
        fences.set("db-0".to_string(), "node-1".to_string());
        let path = std::env::temp_dir().join(format!("keel-controlplane-pending-fences-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &fences).unwrap();
        let loaded: PendingFences = crate::store::load_or_default(&path);
        assert_eq!(loaded.for_node("node-1"), vec!["db-0".to_string()]);
    }
}
