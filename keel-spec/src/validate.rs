use crate::error::SpecError;
use ipnet::IpNet;

pub fn validate_name(name: &str) -> Result<(), SpecError> {
    let valid = !name.is_empty()
        && name.len() <= 63
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && name.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
        && name.chars().last().is_some_and(|c| c.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(SpecError::InvalidName(name.to_string()))
    }
}

pub fn validate_address(address: &str) -> Result<(), SpecError> {
    address
        .parse::<IpNet>()
        .map(|_| ())
        .map_err(|e| SpecError::InvalidAddress(address.to_string(), e.to_string()))
}

pub fn validate_volumes(volumes: &[crate::types::VolumeMount]) -> Result<(), SpecError> {
    let mut seen = std::collections::HashSet::new();
    for volume in volumes {
        validate_name(&volume.name)?;
        if !seen.insert(volume.name.clone()) {
            return Err(SpecError::DuplicateVolumeName(volume.name.clone()));
        }
        crate::resources::parse_zfs_quota(&volume.size)?;
        validate_mount_path(&volume.mount_path)?;
    }
    Ok(())
}

/// A mount path is an absolute, in-jail path with no `..` segments. Enforced
/// because the reconciler joins this onto the jail's rootfs with only a
/// leading-slash trim, not real path normalization — an unvalidated `..`
/// segment lets a spec mount onto the host filesystem outside the jail.
pub fn validate_mount_path(mount_path: &str) -> Result<(), SpecError> {
    let valid = mount_path.starts_with('/')
        && mount_path != "/"
        && mount_path.split('/').all(|segment| segment != "..");
    if valid {
        Ok(())
    } else {
        Err(SpecError::InvalidMountPath(mount_path.to_string()))
    }
}

/// `image` is interpolated verbatim into a ZFS dataset path
/// (`<pool>/keel/<image>`) that's then cloned as the jail's rootfs. Requiring
/// the `base/` prefix keeps it inside the intended base-image namespace and
/// out of `jails/`/`volumes/`, which share the same `<pool>/keel/` parent and
/// hold other tenants' live datasets.
pub fn validate_image(image: &str) -> Result<(), SpecError> {
    let valid = image
        .strip_prefix("base/")
        .is_some_and(|rest| {
            !rest.is_empty()
                && rest.split('/').all(|segment| {
                    !segment.is_empty()
                        && segment != "."
                        && segment != ".."
                        && segment
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
                })
        });
    if valid {
        Ok(())
    } else {
        Err(SpecError::InvalidImage(image.to_string()))
    }
}

/// Bridges follow keel's own naming convention (`keel0`, `keel1`, ...).
/// `keel-net::ensure_bridge_exists` treats an already-existing interface as
/// success and skips creation, so accepting an arbitrary name here would let
/// a spec attach jail traffic to any pre-existing host interface (`lo0`, a
/// WAN NIC, another node's bridge).
pub fn validate_bridge(bridge: &str) -> Result<(), SpecError> {
    let valid = bridge
        .strip_prefix("keel")
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()));
    if valid {
        Ok(())
    } else {
        Err(SpecError::InvalidBridge(bridge.to_string()))
    }
}

pub fn validate_transition(old: &crate::types::JailSpec, new: &crate::types::JailSpec) -> Result<(), SpecError> {
    if old.spec.image != new.spec.image {
        return Err(SpecError::ImmutableField("spec.image"));
    }
    if old.spec.network.address != new.spec.network.address {
        return Err(SpecError::ImmutableField("spec.network.address"));
    }
    if old.spec.volumes != new.spec.volumes {
        return Err(SpecError::ImmutableField("spec.volumes"));
    }
    Ok(())
}

pub fn validate_host(host: &str) -> Result<(), SpecError> {
    let labels: Vec<&str> = host.split('.').collect();
    let valid = labels.len() >= 2
        && labels.iter().all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
                && label.chars().next().is_some_and(|c| c.is_ascii_alphanumeric())
                && label.chars().last().is_some_and(|c| c.is_ascii_alphanumeric())
        });
    if valid {
        Ok(())
    } else {
        Err(SpecError::InvalidHost(host.to_string()))
    }
}

pub fn validate_email(email: &str) -> Result<(), SpecError> {
    let valid = match email.split_once('@') {
        Some((local, domain)) => {
            !local.is_empty()
                && !domain.is_empty()
                && !local.contains(char::is_whitespace)
                && !domain.contains(char::is_whitespace)
                && domain.contains('.')
        }
        None => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SpecError::InvalidEmail(email.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    #[test]
    fn accepts_well_formed_names() {
        assert!(validate_name("web-1").is_ok());
        assert!(validate_name("a").is_ok());
        assert!(validate_name(&"a".repeat(63)).is_ok());
    }

    #[test]
    fn rejects_malformed_names() {
        assert_eq!(validate_name(""), Err(SpecError::InvalidName("".to_string())));
        assert_eq!(
            validate_name(&"a".repeat(64)),
            Err(SpecError::InvalidName("a".repeat(64)))
        );
        assert_eq!(
            validate_name("-leading-hyphen"),
            Err(SpecError::InvalidName("-leading-hyphen".to_string()))
        );
        assert_eq!(
            validate_name("trailing-hyphen-"),
            Err(SpecError::InvalidName("trailing-hyphen-".to_string()))
        );
        assert_eq!(
            validate_name("Has_Upper_And_Underscore"),
            Err(SpecError::InvalidName("Has_Upper_And_Underscore".to_string()))
        );
    }

    #[test]
    fn accepts_well_formed_cidr_addresses() {
        assert!(validate_address("10.0.0.5/24").is_ok());
        assert!(validate_address("192.168.1.1/32").is_ok());
    }

    #[test]
    fn rejects_malformed_addresses() {
        assert!(validate_address("not-an-address").is_err());
        assert!(validate_address("10.0.0.5").is_err()); // missing prefix length
        assert!(validate_address("10.0.0.5/33").is_err()); // prefix out of range
    }

    fn volume(name: &str, mount_path: &str, size: &str) -> VolumeMount {
        VolumeMount { name: name.to_string(), mount_path: mount_path.to_string(), size: size.to_string() }
    }

    #[test]
    fn validate_volumes_accepts_an_empty_list() {
        assert!(validate_volumes(&[]).is_ok());
    }

    #[test]
    fn validate_volumes_accepts_well_formed_distinct_volumes() {
        let volumes = vec![volume("web-data", "/data", "1G"), volume("web-cache", "/cache", "512M")];
        assert!(validate_volumes(&volumes).is_ok());
    }

    #[test]
    fn validate_volumes_rejects_a_malformed_name() {
        let volumes = vec![volume("Invalid_Name", "/data", "1G")];
        assert!(matches!(validate_volumes(&volumes), Err(SpecError::InvalidName(_))));
    }

    #[test]
    fn validate_volumes_rejects_a_duplicate_name() {
        let volumes = vec![volume("web-data", "/data", "1G"), volume("web-data", "/other", "2G")];
        assert_eq!(validate_volumes(&volumes), Err(SpecError::DuplicateVolumeName("web-data".to_string())));
    }

    #[test]
    fn validate_volumes_rejects_a_malformed_size() {
        let volumes = vec![volume("web-data", "/data", "not-a-size")];
        assert!(matches!(validate_volumes(&volumes), Err(SpecError::InvalidMemory(_))));
    }

    #[test]
    fn validate_volumes_rejects_a_traversal_mount_path() {
        let volumes = vec![volume("web-data", "../../../etc", "1G")];
        assert_eq!(
            validate_volumes(&volumes),
            Err(SpecError::InvalidMountPath("../../../etc".to_string()))
        );
    }

    #[test]
    fn accepts_well_formed_mount_paths() {
        assert!(validate_mount_path("/data").is_ok());
        assert!(validate_mount_path("/var/db").is_ok());
    }

    #[test]
    fn rejects_relative_mount_paths() {
        assert_eq!(
            validate_mount_path("data"),
            Err(SpecError::InvalidMountPath("data".to_string()))
        );
    }

    #[test]
    fn rejects_mount_path_traversal_segments() {
        assert_eq!(
            validate_mount_path("/data/../../etc"),
            Err(SpecError::InvalidMountPath("/data/../../etc".to_string()))
        );
        assert_eq!(
            validate_mount_path("../../../etc"),
            Err(SpecError::InvalidMountPath("../../../etc".to_string()))
        );
    }

    #[test]
    fn rejects_the_bare_root_mount_path() {
        assert_eq!(validate_mount_path("/"), Err(SpecError::InvalidMountPath("/".to_string())));
    }

    #[test]
    fn accepts_well_formed_images() {
        assert!(validate_image("base/14.2-web").is_ok());
        assert!(validate_image("base/keel-ingress").is_ok());
    }

    #[test]
    fn rejects_images_outside_the_base_namespace() {
        assert_eq!(
            validate_image("jails/web-1"),
            Err(SpecError::InvalidImage("jails/web-1".to_string()))
        );
        assert_eq!(
            validate_image("volumes/web-data"),
            Err(SpecError::InvalidImage("volumes/web-data".to_string()))
        );
    }

    #[test]
    fn rejects_image_traversal_segments() {
        assert_eq!(
            validate_image("base/../jails/web-1"),
            Err(SpecError::InvalidImage("base/../jails/web-1".to_string()))
        );
    }

    #[test]
    fn rejects_malformed_images() {
        assert!(validate_image("").is_err());
        assert!(validate_image("base/").is_err());
        assert!(validate_image("base").is_err());
        assert!(validate_image("base/Has_Upper").is_err());
    }

    #[test]
    fn accepts_well_formed_bridges() {
        assert!(validate_bridge("keel0").is_ok());
        assert!(validate_bridge("keel12").is_ok());
    }

    #[test]
    fn rejects_bridges_outside_the_keel_naming_convention() {
        assert_eq!(validate_bridge("lo0"), Err(SpecError::InvalidBridge("lo0".to_string())));
        assert_eq!(validate_bridge("em0"), Err(SpecError::InvalidBridge("em0".to_string())));
        assert_eq!(validate_bridge("bridge0"), Err(SpecError::InvalidBridge("bridge0".to_string())));
        assert_eq!(validate_bridge("keel"), Err(SpecError::InvalidBridge("keel".to_string())));
    }

    fn sample_spec() -> JailSpec {
        JailSpec {
            api_version: "keel/v1".to_string(),
            kind: "Jail".to_string(),
            metadata: Metadata { name: "web-1".to_string() },
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
            },
        }
    }

    #[test]
    fn allows_changing_resources_and_restart_policy() {
        let old = sample_spec();
        let mut new = sample_spec();
        new.spec.resources.cpu = "4".to_string();
        new.spec.restart_policy = RestartPolicy::Never;
        assert!(validate_transition(&old, &new).is_ok());
    }

    #[test]
    fn rejects_changing_image() {
        let old = sample_spec();
        let mut new = sample_spec();
        new.spec.image = "base/14.2-other".to_string();
        assert_eq!(
            validate_transition(&old, &new),
            Err(SpecError::ImmutableField("spec.image"))
        );
    }

    #[test]
    fn rejects_changing_network_address() {
        let old = sample_spec();
        let mut new = sample_spec();
        new.spec.network.address = "10.0.0.6/24".to_string();
        assert_eq!(
            validate_transition(&old, &new),
            Err(SpecError::ImmutableField("spec.network.address"))
        );
    }

    #[test]
    fn rejects_changing_volumes() {
        let old = sample_spec();
        let mut new = sample_spec();
        new.spec.volumes = vec![volume("web-data", "/data", "1G")];
        assert_eq!(validate_transition(&old, &new), Err(SpecError::ImmutableField("spec.volumes")));
    }

    #[test]
    fn allows_reapplying_with_the_same_volumes() {
        let mut old = sample_spec();
        old.spec.volumes = vec![volume("web-data", "/data", "1G")];
        let new = old.clone();
        assert!(validate_transition(&old, &new).is_ok());
    }

    #[test]
    fn accepts_well_formed_hosts() {
        assert!(validate_host("example.com").is_ok());
        assert!(validate_host("blog.example.com").is_ok());
    }

    #[test]
    fn rejects_malformed_hosts() {
        assert!(validate_host("localhost").is_err()); // no dot
        assert!(validate_host("not a host!").is_err());
        assert!(validate_host(".example.com").is_err()); // empty label
        assert!(validate_host("-example.com").is_err()); // leading hyphen label
    }

    #[test]
    fn accepts_well_formed_emails() {
        assert!(validate_email("admin@example.com").is_ok());
    }

    #[test]
    fn rejects_malformed_emails() {
        assert!(validate_email("not-an-email").is_err());
        assert!(validate_email("admin@").is_err());
        assert!(validate_email("@example.com").is_err());
        assert!(validate_email("admin@no-dot").is_err());
    }
}
