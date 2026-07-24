use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicaTarget {
    pub replica_name: String,
    pub volume_dataset: String,
    pub source_node_addr: String,
    pub last_snapshot: Option<String>,
    /// The highest generation (see `keel_spec::Spec::generation`'s doc
    /// comment) any sender has successfully identified itself with so far.
    /// `#[serde(default)]` matters here: every `ReplicaTarget` persisted
    /// before this field existed must load as generation 0, the same value
    /// a from-scratch replica starts at, rather than failing to parse.
    #[serde(default)]
    pub highest_generation_seen: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_target_round_trips_through_yaml() {
        let target = ReplicaTarget {
            replica_name: "db-0".to_string(),
            volume_dataset: "zroot/keel/volumes/db-0-data".to_string(),
            source_node_addr: "10.0.0.4:7621".to_string(),
            last_snapshot: None,
            highest_generation_seen: 3,
        };
        let yaml = serde_yaml::to_string(&target).unwrap();
        let parsed: ReplicaTarget = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed, target);
    }

    #[test]
    fn a_replica_target_persisted_before_this_field_existed_loads_at_generation_zero() {
        let yaml = "replica_name: db-0\nvolume_dataset: zroot/keel/volumes/db-0-data\nsource_node_addr: 10.0.0.4:7621\nlast_snapshot: null\n";
        let parsed: ReplicaTarget = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.highest_generation_seen, 0);
    }
}
