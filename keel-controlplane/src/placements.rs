use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Placements {
    by_jail: HashMap<String, String>,
    /// Monotonic per-replica-name counter, bumped on every `set` (including
    /// the first). Deliberately never cleared by `remove` -- a replica name
    /// reused after teardown must still get a strictly higher generation
    /// than any prior incarnation, so a partitioned former-primary's
    /// outdated generation can never look valid again just because the
    /// name was freed and reused. See `keel_spec::Spec::generation`'s doc
    /// comment for how this rides through to the replication wire
    /// protocol.
    #[serde(default)]
    generations: HashMap<String, u64>,
}

impl Placements {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, jail_name: &str) -> Option<&str> {
        self.by_jail.get(jail_name).map(|s| s.as_str())
    }

    pub fn generation(&self, jail_name: &str) -> u64 {
        self.generations.get(jail_name).copied().unwrap_or(0)
    }

    pub fn set(&mut self, jail_name: String, node_id: String) {
        *self.generations.entry(jail_name.clone()).or_insert(0) += 1;
        self.by_jail.insert(jail_name, node_id);
    }

    pub fn remove(&mut self, jail_name: &str) {
        self.by_jail.remove(jail_name);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.by_jail.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_on_an_empty_table_returns_none() {
        let placements = Placements::new();
        assert_eq!(placements.get("web-1"), None);
    }

    #[test]
    fn set_then_get_returns_the_recorded_node() {
        let mut placements = Placements::new();
        placements.set("web-1".to_string(), "node-1".to_string());
        assert_eq!(placements.get("web-1"), Some("node-1"));
    }

    #[test]
    fn set_again_on_the_same_jail_overwrites_rather_than_duplicating() {
        let mut placements = Placements::new();
        placements.set("web-1".to_string(), "node-1".to_string());
        placements.set("web-1".to_string(), "node-2".to_string());
        assert_eq!(placements.get("web-1"), Some("node-2"));
    }

    #[test]
    fn remove_clears_the_placement() {
        let mut placements = Placements::new();
        placements.set("web-1".to_string(), "node-1".to_string());
        placements.remove("web-1");
        assert_eq!(placements.get("web-1"), None);
    }

    #[test]
    fn generation_is_zero_for_a_jail_that_has_never_been_placed() {
        let placements = Placements::new();
        assert_eq!(placements.generation("web-1"), 0);
    }

    #[test]
    fn generation_increments_on_every_set_including_the_first() {
        let mut placements = Placements::new();
        placements.set("web-1".to_string(), "node-1".to_string());
        assert_eq!(placements.generation("web-1"), 1);
        placements.set("web-1".to_string(), "node-2".to_string());
        assert_eq!(placements.generation("web-1"), 2);
    }

    #[test]
    fn remove_does_not_reset_the_generation_counter() {
        // A stale sender fenced via a real DELETE and a later re-placement
        // of the same replica name must never see its generation counter
        // restart from zero -- doing so would let an old, partitioned
        // primary's outdated generation look valid again after the name is
        // reused, defeating the whole point of the counter.
        let mut placements = Placements::new();
        placements.set("web-1".to_string(), "node-1".to_string());
        placements.set("web-1".to_string(), "node-2".to_string());
        placements.remove("web-1");
        placements.set("web-1".to_string(), "node-3".to_string());
        assert_eq!(placements.generation("web-1"), 3, "generation must keep counting up across a remove, never restart from zero");
    }

    #[test]
    fn iter_yields_every_entry() {
        let mut placements = Placements::new();
        placements.set("web-1".to_string(), "node-1".to_string());
        placements.set("web-2".to_string(), "node-2".to_string());
        let mut entries: Vec<(&str, &str)> = placements.iter().collect();
        entries.sort();
        assert_eq!(entries, vec![("web-1", "node-1"), ("web-2", "node-2")]);
    }

    #[test]
    fn placements_round_trips_through_yaml() {
        let mut placements = Placements::new();
        placements.set("web-0".to_string(), "node-1".to_string());
        placements.set("web-1".to_string(), "node-2".to_string());
        let path = std::env::temp_dir().join(format!("keel-controlplane-placements-test-{}.yaml", std::process::id()));
        crate::store::save(&path, &placements).unwrap();
        let loaded: Placements = crate::store::load_or_default(&path);
        assert_eq!(loaded.get("web-0"), Some("node-1"));
        assert_eq!(loaded.get("web-1"), Some("node-2"));
    }
}
