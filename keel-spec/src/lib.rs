pub mod error;
pub mod resources;
pub mod types;
pub mod validate;

pub use error::SpecError;
pub use resources::{cores_to_pcpu_percent, parse_cpu_cores, parse_memory_bytes};
pub use types::{
    IngressBackend, IngressSpec, IngressSpecBody, IngressTls, JailSpec, JailTemplate, Metadata,
    NetworkSpec, RestartPolicy, ResourcesSpec, ServiceSpec, ServiceSpecBody, Spec,
    TemplateNetworkSpec, VolumeMount,
};
pub use validate::{
    validate_address, validate_bridge, validate_email, validate_host, validate_image, validate_mount_path,
    validate_name, validate_transition, validate_volumes,
};

pub fn parse_and_validate(yaml: &str) -> Result<JailSpec, SpecError> {
    let spec: JailSpec = serde_yaml::from_str(yaml).map_err(|e| SpecError::Yaml(e.to_string()))?;
    validate::validate_name(&spec.metadata.name)?;
    validate::validate_image(&spec.spec.image)?;
    validate::validate_bridge(&spec.spec.network.bridge)?;
    validate::validate_address(&spec.spec.network.address)?;
    resources::parse_cpu_cores(&spec.spec.resources.cpu)?;
    resources::parse_memory_bytes(&spec.spec.resources.memory)?;
    validate::validate_volumes(&spec.spec.volumes)?;
    Ok(spec)
}

pub fn parse_and_validate_service(yaml: &str) -> Result<ServiceSpec, SpecError> {
    let spec: ServiceSpec = serde_yaml::from_str(yaml).map_err(|e| SpecError::Yaml(e.to_string()))?;
    validate::validate_name(&spec.metadata.name)?;
    validate::validate_image(&spec.spec.template.image)?;
    validate::validate_bridge(&spec.spec.template.network.bridge)?;
    resources::parse_cpu_cores(&spec.spec.template.resources.cpu)?;
    resources::parse_memory_bytes(&spec.spec.template.resources.memory)?;
    validate::validate_volumes(&spec.spec.template.volumes)?;
    if spec.spec.port == 0 {
        return Err(SpecError::InvalidPort(0));
    }
    Ok(spec)
}

pub fn parse_and_validate_ingress(yaml: &str) -> Result<types::IngressSpec, SpecError> {
    let spec: types::IngressSpec = serde_yaml::from_str(yaml).map_err(|e| SpecError::Yaml(e.to_string()))?;
    validate::validate_name(&spec.metadata.name)?;
    validate::validate_host(&spec.spec.host)?;
    validate::validate_email(&spec.spec.tls.email)?;
    if spec.spec.backend.port == 0 {
        return Err(SpecError::InvalidPort(0));
    }
    Ok(spec)
}

/// Reads just the `kind` field out of a YAML document, without requiring the
/// rest of it to parse as any particular spec type — used by `keelctl` to
/// decide whether to parse the rest as a `JailSpec`, a `ServiceSpec`, or an `IngressSpec`.
pub fn sniff_kind(yaml: &str) -> Result<String, SpecError> {
    #[derive(serde::Deserialize)]
    struct KindOnly {
        kind: String,
    }
    let sniff: KindOnly = serde_yaml::from_str(yaml).map_err(|e| SpecError::Yaml(e.to_string()))?;
    Ok(sniff.kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_SERVICE_YAML: &str = r#"
apiVersion: keel/v1
kind: Service
metadata:
  name: web
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
"#;

    #[test]
    fn parse_and_validate_service_accepts_a_well_formed_service() {
        let spec = parse_and_validate_service(VALID_SERVICE_YAML).unwrap();
        assert_eq!(spec.metadata.name, "web");
        assert_eq!(spec.spec.replicas, 2);
    }

    #[test]
    fn parse_and_validate_service_rejects_an_invalid_name() {
        let yaml = VALID_SERVICE_YAML.replace("name: web", "name: Invalid_Name");
        assert!(matches!(parse_and_validate_service(&yaml), Err(SpecError::InvalidName(_))));
    }

    #[test]
    fn parse_and_validate_service_rejects_invalid_resources() {
        let yaml = VALID_SERVICE_YAML.replace("cpu: \"1\"", "cpu: \"0\"");
        assert!(matches!(parse_and_validate_service(&yaml), Err(SpecError::InvalidCpu(_))));
    }

    #[test]
    fn parse_and_validate_service_accepts_the_port_field() {
        let spec = parse_and_validate_service(VALID_SERVICE_YAML).unwrap();
        assert_eq!(spec.spec.port, 8080);
    }

    #[test]
    fn parse_and_validate_service_rejects_port_zero() {
        let yaml = VALID_SERVICE_YAML.replace("port: 8080", "port: 0");
        assert!(matches!(parse_and_validate_service(&yaml), Err(SpecError::InvalidPort(0))));
    }

    #[test]
    fn sniff_kind_reads_jail() {
        let yaml = "apiVersion: keel/v1\nkind: Jail\nmetadata:\n  name: web-1\n";
        assert_eq!(sniff_kind(yaml).unwrap(), "Jail");
    }

    #[test]
    fn sniff_kind_reads_service_without_needing_the_rest_of_the_document_to_parse_as_a_jail() {
        assert_eq!(sniff_kind(VALID_SERVICE_YAML).unwrap(), "Service");
    }

    #[test]
    fn sniff_kind_on_malformed_yaml_is_an_error() {
        assert!(sniff_kind("not: valid: yaml: [").is_err());
    }

    #[test]
    fn parse_and_validate_service_rejects_a_malformed_template_volume() {
        let yaml = VALID_SERVICE_YAML.replace(
            "    restartPolicy: Always\n",
            "    restartPolicy: Always\n    volumes:\n      - name: Invalid_Name\n        mountPath: /data\n        size: 1G\n",
        );
        assert!(matches!(parse_and_validate_service(&yaml), Err(SpecError::InvalidName(_))));
    }

    const VALID_INGRESS_YAML: &str = r#"
apiVersion: keel/v1
kind: Ingress
metadata:
  name: blog
spec:
  host: example.com
  backend:
    service: hugo-site
    port: 8080
  tls:
    email: admin@example.com
"#;

    #[test]
    fn parse_and_validate_ingress_accepts_a_well_formed_ingress() {
        let spec = parse_and_validate_ingress(VALID_INGRESS_YAML).unwrap();
        assert_eq!(spec.metadata.name, "blog");
        assert_eq!(spec.spec.host, "example.com");
        assert_eq!(spec.spec.backend.service, "hugo-site");
        assert_eq!(spec.spec.backend.port, 8080);
        assert_eq!(spec.spec.tls.email, "admin@example.com");
    }

    #[test]
    fn parse_and_validate_ingress_rejects_an_invalid_name() {
        let yaml = VALID_INGRESS_YAML.replace("name: blog", "name: Invalid_Name");
        assert!(matches!(parse_and_validate_ingress(&yaml), Err(SpecError::InvalidName(_))));
    }

    #[test]
    fn parse_and_validate_ingress_rejects_a_malformed_host() {
        let yaml = VALID_INGRESS_YAML.replace("host: example.com", "host: not a host!");
        assert!(matches!(parse_and_validate_ingress(&yaml), Err(SpecError::InvalidHost(_))));
    }

    #[test]
    fn parse_and_validate_ingress_rejects_a_host_with_no_dot() {
        let yaml = VALID_INGRESS_YAML.replace("host: example.com", "host: localhost");
        assert!(matches!(parse_and_validate_ingress(&yaml), Err(SpecError::InvalidHost(_))));
    }

    #[test]
    fn parse_and_validate_ingress_rejects_port_zero() {
        let yaml = VALID_INGRESS_YAML.replace("port: 8080", "port: 0");
        assert!(matches!(parse_and_validate_ingress(&yaml), Err(SpecError::InvalidPort(0))));
    }

    #[test]
    fn parse_and_validate_ingress_rejects_a_malformed_email() {
        let yaml = VALID_INGRESS_YAML.replace("email: admin@example.com", "email: not-an-email");
        assert!(matches!(parse_and_validate_ingress(&yaml), Err(SpecError::InvalidEmail(_))));
    }

    #[test]
    fn sniff_kind_reads_ingress() {
        assert_eq!(sniff_kind(VALID_INGRESS_YAML).unwrap(), "Ingress");
    }

    const VALID_JAIL_YAML: &str = r#"
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

    #[test]
    fn parse_and_validate_accepts_a_well_formed_jail() {
        assert!(parse_and_validate(VALID_JAIL_YAML).is_ok());
    }

    #[test]
    fn parse_and_validate_rejects_an_image_outside_the_base_namespace() {
        let yaml = VALID_JAIL_YAML.replace("image: base/14.2-web", "image: jails/other-jail");
        assert!(matches!(parse_and_validate(&yaml), Err(SpecError::InvalidImage(_))));
    }

    #[test]
    fn parse_and_validate_rejects_a_bridge_outside_the_keel_naming_convention() {
        let yaml = VALID_JAIL_YAML.replace("bridge: keel0", "bridge: lo0");
        assert!(matches!(parse_and_validate(&yaml), Err(SpecError::InvalidBridge(_))));
    }

    #[test]
    fn parse_and_validate_service_rejects_an_image_outside_the_base_namespace() {
        let yaml = VALID_SERVICE_YAML.replace("image: base/14.2-web", "image: volumes/web-data");
        assert!(matches!(parse_and_validate_service(&yaml), Err(SpecError::InvalidImage(_))));
    }

    #[test]
    fn parse_and_validate_service_rejects_a_bridge_outside_the_keel_naming_convention() {
        let yaml = VALID_SERVICE_YAML.replace("bridge: keel0", "bridge: em0");
        assert!(matches!(parse_and_validate_service(&yaml), Err(SpecError::InvalidBridge(_))));
    }
}
