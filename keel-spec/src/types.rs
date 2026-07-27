use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JailSpec {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub image: String,
    pub command: Vec<String>,
    pub network: NetworkSpec,
    pub resources: ResourcesSpec,
    #[serde(rename = "restartPolicy")]
    pub restart_policy: RestartPolicy,
    #[serde(default)]
    pub volumes: Vec<VolumeMount>,
    #[serde(rename = "replicateTo", default)]
    pub replicate_to: Option<String>,
    /// Set by the control plane, never by an operator-authored spec (hence
    /// no dedicated YAML key -- an incoming apply always starts at 0, same
    /// treatment as `replicate_to`). Fences a partitioned former-primary
    /// out of replicating to a standby that's since been promoted: each
    /// (re)placement of a replica mints a strictly higher generation than
    /// the last (see `keel-controlplane`'s `Placements::generation`), which
    /// rides along in the replication wire protocol's header so the
    /// receiving standby can reject a stale sender using only its own
    /// local state, with no control-plane connectivity required.
    #[serde(default)]
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolumeMount {
    pub name: String,
    #[serde(rename = "mountPath")]
    pub mount_path: String,
    pub size: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkSpec {
    pub vnet: bool,
    pub bridge: String,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourcesSpec {
    pub cpu: String,
    pub memory: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestartPolicy {
    Always,
    OnFailure,
    Never,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceSpec {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: ServiceSpecBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceSpecBody {
    pub replicas: u32,
    pub port: u16,
    pub template: JailTemplate,
}

/// The same fields `kind: Jail`'s `Spec` has, minus `network.address` — a
/// replica's address is always auto-assigned (see `keel-controlplane`'s
/// `addresses` module), never given directly in a `Service`'s template.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JailTemplate {
    pub image: String,
    pub command: Vec<String>,
    pub network: TemplateNetworkSpec,
    pub resources: ResourcesSpec,
    #[serde(rename = "restartPolicy")]
    pub restart_policy: RestartPolicy,
    #[serde(default)]
    pub volumes: Vec<VolumeMount>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateNetworkSpec {
    pub vnet: bool,
    pub bridge: String,
}

impl JailTemplate {
    /// Builds the concrete `JailSpec` for one replica: the template's fields
    /// plus the deterministic replica `name` and its auto-assigned `address`
    /// (already formatted as a CIDR string, e.g. `"10.0.60.2/24"`).
    pub fn to_jail_spec(&self, name: &str, address: &str) -> JailSpec {
        JailSpec {
            api_version: "keel/v1".to_string(),
            kind: "Jail".to_string(),
            metadata: Metadata { name: name.to_string() },
            spec: Spec {
                image: self.image.clone(),
                command: self.command.clone(),
                network: NetworkSpec {
                    vnet: self.network.vnet,
                    bridge: self.network.bridge.clone(),
                    address: address.to_string(),
                },
                resources: self.resources.clone(),
                restart_policy: self.restart_policy,
                volumes: self
                    .volumes
                    .iter()
                    .map(|v| VolumeMount {
                        name: format!("{name}-{}", v.name),
                        mount_path: v.mount_path.clone(),
                        size: v.size.clone(),
                    })
                    .collect(),
                replicate_to: None,
                generation: 0,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_YAML: &str = r#"
apiVersion: keel/v1
kind: Jail
metadata:
  name: web-1
spec:
  image: base/14.2-web
  command: ["/usr/local/bin/myapp"]
  network:
    vnet: true
    bridge: keel0
    address: 10.0.0.5/24
  resources:
    cpu: "2"
    memory: "512M"
  restartPolicy: Always
"#;

    const EXAMPLE_YAML_WITH_VOLUME: &str = r#"
apiVersion: keel/v1
kind: Jail
metadata:
  name: web-1
spec:
  image: base/14.2-web
  command: ["/usr/local/bin/myapp"]
  network:
    vnet: true
    bridge: keel0
    address: 10.0.0.5/24
  resources:
    cpu: "2"
    memory: "512M"
  restartPolicy: Always
  volumes:
    - name: web-data
      mountPath: /data
      size: 1G
"#;

    #[test]
    fn parses_a_jail_with_one_volume() {
        let spec: JailSpec = serde_yaml::from_str(EXAMPLE_YAML_WITH_VOLUME).unwrap();
        assert_eq!(spec.spec.volumes.len(), 1);
        assert_eq!(spec.spec.volumes[0].name, "web-data");
        assert_eq!(spec.spec.volumes[0].mount_path, "/data");
        assert_eq!(spec.spec.volumes[0].size, "1G");
    }

    #[test]
    fn a_jail_with_no_volumes_key_parses_with_an_empty_list() {
        let spec: JailSpec = serde_yaml::from_str(EXAMPLE_YAML).unwrap();
        assert_eq!(spec.spec.volumes, vec![]);
    }

    #[test]
    fn an_extra_typo_d_field_in_spec_is_rejected_instead_of_silently_dropped() {
        // Every real, required field is present and correctly named here -
        // `imageTypo` is purely an extra, unrecognized field alongside them
        // (e.g. a typo of `image` the author meant to override), so the
        // only thing that can make this fail to parse is deny_unknown_fields
        // itself, not a missing required field.
        let yaml = r#"
apiVersion: keel/v1
kind: Jail
metadata:
  name: web-1
spec:
  image: base/14.2-web
  imageTypo: base/14.2-web
  command: ["/usr/local/bin/myapp"]
  network:
    vnet: true
    bridge: keel0
    address: 10.0.0.5/24
  resources:
    cpu: "2"
    memory: "512M"
  restartPolicy: Always
"#;
        assert!(serde_yaml::from_str::<JailSpec>(yaml).is_err(), "expected an extra, unrecognized field to be rejected at parse time");
    }

    #[test]
    fn an_extra_typo_d_field_in_network_spec_is_rejected_instead_of_silently_dropped() {
        let yaml = r#"
apiVersion: keel/v1
kind: Jail
metadata:
  name: web-1
spec:
  image: base/14.2-web
  command: ["/usr/local/bin/myapp"]
  network:
    vnet: true
    bridge: keel0
    address: 10.0.0.5/24
    addressTypo: 10.0.0.5/24
  resources:
    cpu: "2"
    memory: "512M"
  restartPolicy: Always
"#;
        assert!(serde_yaml::from_str::<JailSpec>(yaml).is_err(), "expected an extra, unrecognized field to be rejected at parse time");
    }

    #[test]
    fn parses_the_design_spec_example_yaml() {
        let spec: JailSpec = serde_yaml::from_str(EXAMPLE_YAML).unwrap();
        assert_eq!(spec.api_version, "keel/v1");
        assert_eq!(spec.kind, "Jail");
        assert_eq!(spec.metadata.name, "web-1");
        assert_eq!(spec.spec.image, "base/14.2-web");
        assert_eq!(spec.spec.command, vec!["/usr/local/bin/myapp".to_string()]);
        assert!(spec.spec.network.vnet);
        assert_eq!(spec.spec.network.bridge, "keel0");
        assert_eq!(spec.spec.network.address, "10.0.0.5/24");
        assert_eq!(spec.spec.resources.cpu, "2");
        assert_eq!(spec.spec.resources.memory, "512M");
        assert_eq!(spec.spec.restart_policy, RestartPolicy::Always);
    }

    const SERVICE_EXAMPLE_YAML: &str = r#"
apiVersion: keel/v1
kind: Service
metadata:
  name: web
spec:
  replicas: 3
  port: 8080
  template:
    image: base/14.2-web
    command: ["/usr/local/bin/myapp"]
    network:
      vnet: true
      bridge: keel0
    resources:
      cpu: "1"
      memory: "256M"
    restartPolicy: Always
"#;

    #[test]
    fn parses_the_service_example_yaml() {
        let spec: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML).unwrap();
        assert_eq!(spec.api_version, "keel/v1");
        assert_eq!(spec.kind, "Service");
        assert_eq!(spec.metadata.name, "web");
        assert_eq!(spec.spec.replicas, 3);
        assert_eq!(spec.spec.port, 8080);
        assert_eq!(spec.spec.template.image, "base/14.2-web");
        assert!(spec.spec.template.network.vnet);
        assert_eq!(spec.spec.template.network.bridge, "keel0");
        assert_eq!(spec.spec.template.resources.cpu, "1");
        assert_eq!(spec.spec.template.restart_policy, RestartPolicy::Always);
    }

    #[test]
    fn rejects_a_template_with_an_embedded_network_address() {
        let yaml = SERVICE_EXAMPLE_YAML.replace(
            "    network:\n      vnet: true\n      bridge: keel0\n",
            "    network:\n      vnet: true\n      bridge: keel0\n      address: 10.0.0.5/24\n",
        );
        assert!(
            serde_yaml::from_str::<ServiceSpec>(&yaml).is_err(),
            "template.network.address is not a valid field and must be rejected"
        );
    }

    #[test]
    fn to_jail_spec_builds_a_replica_spec_from_the_template_plus_name_and_address() {
        let service: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML).unwrap();
        let jail = service.spec.template.to_jail_spec("web-0", "10.0.60.2/24");
        assert_eq!(jail.api_version, "keel/v1");
        assert_eq!(jail.kind, "Jail");
        assert_eq!(jail.metadata.name, "web-0");
        assert_eq!(jail.spec.image, "base/14.2-web");
        assert_eq!(jail.spec.command, vec!["/usr/local/bin/myapp".to_string()]);
        assert!(jail.spec.network.vnet);
        assert_eq!(jail.spec.network.bridge, "keel0");
        assert_eq!(jail.spec.network.address, "10.0.60.2/24");
        assert_eq!(jail.spec.resources.cpu, "1");
        assert_eq!(jail.spec.restart_policy, RestartPolicy::Always);
    }

    #[test]
    fn to_jail_spec_with_no_template_volumes_produces_no_replica_volumes() {
        let service: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML).unwrap();
        let jail = service.spec.template.to_jail_spec("web-0", "10.0.60.2/24");
        assert_eq!(jail.spec.volumes, vec![]);
    }

    const SERVICE_EXAMPLE_YAML_WITH_VOLUME: &str = r#"
apiVersion: keel/v1
kind: Service
metadata:
  name: db
spec:
  replicas: 2
  port: 8080
  template:
    image: base/14.2-web
    command: ["/usr/local/bin/myapp"]
    network:
      vnet: true
      bridge: keel0
    resources:
      cpu: "1"
      memory: "256M"
    restartPolicy: Always
    volumes:
      - name: data
        mountPath: /var/db
        size: 5G
"#;

    #[test]
    fn parses_a_service_template_with_one_volume() {
        let spec: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML_WITH_VOLUME).unwrap();
        assert_eq!(spec.spec.template.volumes.len(), 1);
        assert_eq!(spec.spec.template.volumes[0].name, "data");
        assert_eq!(spec.spec.template.volumes[0].mount_path, "/var/db");
        assert_eq!(spec.spec.template.volumes[0].size, "5G");
    }

    #[test]
    fn a_service_template_with_no_volumes_key_parses_with_an_empty_list() {
        let spec: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML).unwrap();
        assert_eq!(spec.spec.template.volumes, vec![]);
    }

    #[test]
    fn to_jail_spec_maps_template_volumes_into_per_replica_volumes_with_renamed_prefix() {
        let service: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML_WITH_VOLUME).unwrap();

        let jail0 = service.spec.template.to_jail_spec("web-0", "10.0.60.2/24");
        assert_eq!(jail0.spec.volumes.len(), 1);
        assert_eq!(jail0.spec.volumes[0].name, "web-0-data");
        assert_eq!(jail0.spec.volumes[0].mount_path, "/var/db");
        assert_eq!(jail0.spec.volumes[0].size, "5G");

        let jail1 = service.spec.template.to_jail_spec("web-1", "10.0.60.3/24");
        assert_eq!(jail1.spec.volumes[0].name, "web-1-data");
    }

    const EXAMPLE_YAML_WITH_REPLICATE_TO: &str = r#"
apiVersion: keel/v1
kind: Jail
metadata:
  name: db-0
spec:
  image: base/14.2-web
  command: ["/usr/local/bin/myapp"]
  network:
    vnet: true
    bridge: keel0
    address: 10.0.0.5/24
  resources:
    cpu: "2"
    memory: "512M"
  restartPolicy: Always
  volumes:
    - name: db-0-data
      mountPath: /var/db
      size: 5G
  replicateTo: "10.0.0.9:7622"
"#;

    #[test]
    fn parses_replicate_to_when_present() {
        let spec: JailSpec = serde_yaml::from_str(EXAMPLE_YAML_WITH_REPLICATE_TO).unwrap();
        assert_eq!(spec.spec.replicate_to, Some("10.0.0.9:7622".to_string()));
    }

    #[test]
    fn replicate_to_defaults_to_none_when_absent() {
        let spec: JailSpec = serde_yaml::from_str(EXAMPLE_YAML).unwrap();
        assert_eq!(spec.spec.replicate_to, None);
    }

    #[test]
    fn to_jail_spec_always_starts_with_no_replicate_to() {
        let service: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML).unwrap();
        let jail = service.spec.template.to_jail_spec("web-0", "10.0.60.2/24");
        assert_eq!(jail.spec.replicate_to, None);
    }

    #[test]
    fn generation_defaults_to_zero_when_absent() {
        let spec: JailSpec = serde_yaml::from_str(EXAMPLE_YAML).unwrap();
        assert_eq!(spec.spec.generation, 0);
    }

    #[test]
    fn to_jail_spec_always_starts_at_generation_zero() {
        let service: ServiceSpec = serde_yaml::from_str(SERVICE_EXAMPLE_YAML).unwrap();
        let jail = service.spec.template.to_jail_spec("web-0", "10.0.60.2/24");
        assert_eq!(jail.spec.generation, 0, "the control plane sets a real generation separately, same as replicate_to");
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngressSpec {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: IngressSpecBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngressSpecBody {
    pub host: String,
    pub backend: IngressBackend,
    pub tls: IngressTls,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngressBackend {
    pub service: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngressTls {
    pub email: String,
}
