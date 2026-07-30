use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Cordoned {
    ids: HashSet<String>,
}

impl Cordoned {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cordon(&mut self, node_id: String) {
        self.ids.insert(node_id);
    }

    pub fn uncordon(&mut self, node_id: &str) {
        self.ids.remove(node_id);
    }

    pub fn is_cordoned(&self, node_id: &str) -> bool {
        self.ids.contains(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_node_is_not_cordoned() {
        assert!(!Cordoned::new().is_cordoned("node-1"));
    }

    #[test]
    fn cordon_marks_a_node_cordoned() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        assert!(c.is_cordoned("node-1"));
        assert!(!c.is_cordoned("node-2"));
    }

    #[test]
    fn cordoning_twice_is_idempotent() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        c.cordon("node-1".to_string());
        assert!(c.is_cordoned("node-1"));
    }

    #[test]
    fn uncordon_clears_it() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        c.uncordon("node-1");
        assert!(!c.is_cordoned("node-1"));
    }

    #[test]
    fn uncordoning_an_uncordoned_node_is_a_harmless_no_op() {
        let mut c = Cordoned::new();
        c.uncordon("node-1");
        assert!(!c.is_cordoned("node-1"));
    }

    #[test]
    fn cordoned_round_trips_through_yaml() {
        let mut c = Cordoned::new();
        c.cordon("node-1".to_string());
        let path = std::env::temp_dir().join(format!(
            "keel-controlplane-cordoned-test-{}.yaml",
            std::process::id()
        ));
        crate::store::save(&path, &c).unwrap();
        let loaded: Cordoned = crate::store::load_or_default(&path);
        assert!(loaded.is_cordoned("node-1"));
        assert!(!loaded.is_cordoned("node-2"));
    }
}
