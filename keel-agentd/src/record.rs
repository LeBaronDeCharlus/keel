use keel_spec::JailSpec;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JailRecord {
    pub spec: JailSpec,
    pub epair_ordinal: u32,
    /// Persisted before `Reconciler::delete` runs any destructive action, so
    /// a crash partway through a delete leaves a record on disk that still
    /// says "delete me" rather than "provision me" -- `reconcile` resumes
    /// (rather than reprovisions) any record it loads with this set.
    /// `#[serde(default)]` keeps state files written before this field
    /// existed loading as `false`.
    #[serde(default)]
    pub deleting: bool,
}

pub fn jail_name(spec_name: &str) -> String {
    format!("keel-{spec_name}")
}

/// The singleton `keel-ingress` jail's internal bridge address. Shared with
/// Task 14's `pf` rule so both read this one constant instead of repeating
/// the literal in two files.
pub const INGRESS_JAIL_BRIDGE_ADDR: &str = "10.0.0.2";

pub fn base_dataset_path(pool: &str, image: &str) -> String {
    format!("{pool}/keel/{image}")
}

pub fn jail_dataset_path(pool: &str, spec_name: &str) -> String {
    format!("{pool}/keel/jails/{spec_name}")
}

pub fn jail_rootfs_path(pool: &str, spec_name: &str) -> PathBuf {
    PathBuf::from(format!("/{}", jail_dataset_path(pool, spec_name)))
}

pub fn epair_base_name(ordinal: u32) -> String {
    format!("epair{ordinal}")
}

pub fn volume_dataset_path(pool: &str, name: &str) -> String {
    format!("{pool}/keel/volumes/{name}")
}

pub fn volume_mountpoint(pool: &str, name: &str) -> PathBuf {
    PathBuf::from(format!("/{}", volume_dataset_path(pool, name)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_spec::{Metadata, NetworkSpec, RestartPolicy, ResourcesSpec, Spec};

    fn sample_spec(name: &str) -> JailSpec {
        JailSpec {
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

    #[test]
    fn jail_name_adds_keel_prefix() {
        assert_eq!(jail_name("web-1"), "keel-web-1");
    }

    #[test]
    fn base_dataset_path_appends_image_directly() {
        assert_eq!(base_dataset_path("zroot", "base/14.2-web"), "zroot/keel/base/14.2-web");
    }

    #[test]
    fn jail_dataset_path_uses_jails_subdirectory() {
        assert_eq!(jail_dataset_path("zroot", "web-1"), "zroot/keel/jails/web-1");
    }

    #[test]
    fn jail_rootfs_path_is_leading_slash_plus_dataset_path() {
        assert_eq!(jail_rootfs_path("zroot", "web-1"), PathBuf::from("/zroot/keel/jails/web-1"));
    }

    #[test]
    fn epair_base_name_formats_the_ordinal() {
        assert_eq!(epair_base_name(7), "epair7");
    }

    #[test]
    fn volume_dataset_path_uses_volumes_subdirectory() {
        assert_eq!(volume_dataset_path("zroot", "web-data"), "zroot/keel/volumes/web-data");
    }

    #[test]
    fn volume_mountpoint_is_leading_slash_plus_dataset_path() {
        assert_eq!(volume_mountpoint("zroot", "web-data"), PathBuf::from("/zroot/keel/volumes/web-data"));
    }

    #[test]
    fn jail_record_round_trips_through_yaml() {
        let record = JailRecord { spec: sample_spec("web-1"), epair_ordinal: 3, deleting: false };
        let yaml = serde_yaml::to_string(&record).unwrap();
        let parsed: JailRecord = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed, record);
    }
}
