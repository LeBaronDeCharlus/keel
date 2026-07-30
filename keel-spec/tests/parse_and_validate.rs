use keel_spec::{parse_and_validate, RestartPolicy, SpecError};

const VALID_YAML: &str = r#"
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
fn parses_and_validates_the_design_spec_example() {
    let spec = parse_and_validate(VALID_YAML).expect("valid spec should parse");
    assert_eq!(spec.metadata.name, "web-1");
    assert_eq!(spec.spec.restart_policy, RestartPolicy::Always);
}

#[test]
fn rejects_an_invalid_name() {
    let yaml = VALID_YAML.replace("name: web-1", "name: Invalid_Name");
    assert!(matches!(
        parse_and_validate(&yaml),
        Err(SpecError::InvalidName(_))
    ));
}

#[test]
fn rejects_a_malformed_address() {
    let yaml = VALID_YAML.replace("address: 10.0.0.5/24", "address: not-an-address");
    assert!(matches!(
        parse_and_validate(&yaml),
        Err(SpecError::InvalidAddress(_, _))
    ));
}

#[test]
fn rejects_missing_required_fields() {
    let yaml = "apiVersion: keel/v1\nkind: Jail\n"; // missing metadata and spec entirely
    assert!(matches!(parse_and_validate(yaml), Err(SpecError::Yaml(_))));
}
