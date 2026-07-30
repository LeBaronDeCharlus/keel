use ipnet::Ipv4Net;
use keel_controlplane::placements::Placements;
use keel_controlplane::registry::Registry;
use keel_controlplane::worker;
use std::net::TcpListener;
use std::path::PathBuf;

struct Config {
    addr: String,
    cluster_cidr: Option<Ipv4Net>,
    service_cidr: Option<Ipv4Net>,
    tls_ca_file: Option<PathBuf>,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    tls_crl_file: Option<PathBuf>,
    state_dir: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:7620".to_string(),
            cluster_cidr: None,
            service_cidr: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_crl_file: None,
            state_dir: PathBuf::from("/var/db/keel-controlplane"),
        }
    }
}

fn parse_args() -> Config {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from(args: impl Iterator<Item = String>) -> Config {
    let mut config = Config::default();
    let mut args = args;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| panic!("missing value for {flag}"));
        match flag.as_str() {
            "--addr" => config.addr = value,
            "--cluster-cidr" => {
                config.cluster_cidr = Some(
                    value
                        .parse()
                        .unwrap_or_else(|e| panic!("invalid --cluster-cidr '{value}': {e}")),
                )
            }
            "--service-cidr" => {
                config.service_cidr = Some(
                    value
                        .parse()
                        .unwrap_or_else(|e| panic!("invalid --service-cidr '{value}': {e}")),
                )
            }
            "--tls-ca-file" => config.tls_ca_file = Some(PathBuf::from(value)),
            "--tls-cert-file" => config.tls_cert_file = Some(PathBuf::from(value)),
            "--tls-key-file" => config.tls_key_file = Some(PathBuf::from(value)),
            "--tls-crl-file" => config.tls_crl_file = Some(PathBuf::from(value)),
            "--state-dir" => config.state_dir = PathBuf::from(value),
            other => panic!("unknown flag: {other}"),
        }
    }
    if config.cluster_cidr.is_none()
        || config.service_cidr.is_none()
        || config.tls_ca_file.is_none()
        || config.tls_cert_file.is_none()
        || config.tls_key_file.is_none()
        || config.tls_crl_file.is_none()
    {
        panic!("--cluster-cidr, --service-cidr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required");
    }
    if let Some(cidr) = config.cluster_cidr {
        assert!(
            cidr.prefix_len() <= 24,
            "--cluster-cidr prefix length {} must be <= 24",
            cidr.prefix_len()
        );
    }
    config
}

fn main() {
    let config = parse_args();
    let cluster_cidr = config
        .cluster_cidr
        .expect("validated as required in parse_args_from");
    let service_cidr = config
        .service_cidr
        .expect("validated as required in parse_args_from");
    let ca_file = config
        .tls_ca_file
        .expect("validated as required in parse_args_from");
    let cert_file = config
        .tls_cert_file
        .expect("validated as required in parse_args_from");
    let key_file = config
        .tls_key_file
        .expect("validated as required in parse_args_from");
    let crl_file = config
        .tls_crl_file
        .expect("validated as required in parse_args_from");

    let reloading_tls = keel_controlplane::tls::ReloadingTls::spawn(
        cert_file,
        key_file,
        ca_file,
        crl_file,
        std::time::Duration::from_secs(30),
    )
    .unwrap_or_else(|e| panic!("failed to load TLS configuration: {e}"));

    eprintln!(
        "keel-controlplane: starting (addr={}, state_dir={})",
        config.addr,
        config.state_dir.display()
    );

    let placements: Placements =
        keel_controlplane::store::load_or_default(&config.state_dir.join("placements.yaml"));
    let used_addresses: keel_controlplane::addresses::UsedAddresses =
        keel_controlplane::store::load_or_default(&config.state_dir.join("used_addresses.yaml"));
    let standbys: keel_controlplane::Standbys =
        keel_controlplane::store::load_or_default(&config.state_dir.join("standbys.yaml"));
    let pending_fences: keel_controlplane::PendingFences =
        keel_controlplane::store::load_or_default(&config.state_dir.join("pending_fences.yaml"));
    let cordoned: keel_controlplane::Cordoned =
        keel_controlplane::store::load_or_default(&config.state_dir.join("cordoned.yaml"));
    let services = keel_controlplane::Services::load(&config.state_dir, service_cidr);

    let (_worker_handle, commands) = worker::spawn(
        Registry::new(cluster_cidr),
        placements,
        services,
        used_addresses,
        standbys,
        pending_fences,
        cordoned,
        config.state_dir.clone(),
    );

    let listener = TcpListener::bind(&config.addr).expect("failed to bind TCP listener");
    keel_controlplane::http::run(listener, commands, reloading_tls);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(strs: &[&str]) -> impl Iterator<Item = String> {
        strs.iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn parses_the_tls_flags() {
        let config = parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/16",
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
        assert_eq!(config.cluster_cidr, Some("10.0.0.0/16".parse().unwrap()));
        assert_eq!(config.tls_ca_file, Some(PathBuf::from("/etc/keel/ca.crt")));
        assert_eq!(
            config.tls_cert_file,
            Some(PathBuf::from("/etc/keel/controlplane.crt"))
        );
        assert_eq!(
            config.tls_key_file,
            Some(PathBuf::from("/etc/keel/controlplane.key"))
        );
        assert_eq!(
            config.tls_crl_file,
            Some(PathBuf::from("/etc/keel/crl.pem"))
        );
    }

    #[test]
    #[should_panic(
        expected = "--tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required"
    )]
    fn missing_any_tls_flag_panics() {
        parse_args_from(args(&["--tls-ca-file", "/etc/keel/ca.crt"]));
    }

    #[test]
    fn parses_the_cluster_cidr_flag() {
        let config = parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/16",
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
        assert_eq!(config.cluster_cidr, Some("10.0.0.0/16".parse().unwrap()));
    }

    #[test]
    #[should_panic(
        expected = "--cluster-cidr, --service-cidr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required"
    )]
    fn missing_cluster_cidr_panics() {
        parse_args_from(args(&[
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    #[should_panic(expected = "invalid --cluster-cidr")]
    fn malformed_cluster_cidr_panics_with_a_clear_message() {
        parse_args_from(args(&[
            "--cluster-cidr",
            "not-a-cidr",
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    #[should_panic(expected = "must be <= 24")]
    fn cluster_cidr_prefix_larger_than_24_panics() {
        parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/28",
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    fn parses_the_service_cidr_flag() {
        let config = parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/16",
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
        assert_eq!(config.service_cidr, Some("10.0.250.0/24".parse().unwrap()));
    }

    #[test]
    #[should_panic(expected = "--service-cidr")]
    fn missing_service_cidr_panics() {
        parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/16",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    #[should_panic(expected = "invalid --service-cidr")]
    fn malformed_service_cidr_panics_with_a_clear_message() {
        parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/16",
            "--service-cidr",
            "not-a-cidr",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    fn parses_the_state_dir_flag() {
        let config = parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/16",
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
            "--state-dir",
            "/custom/state/dir",
        ]));
        assert_eq!(config.state_dir, PathBuf::from("/custom/state/dir"));
    }

    #[test]
    fn state_dir_defaults_when_not_given() {
        let config = parse_args_from(args(&[
            "--cluster-cidr",
            "10.0.0.0/16",
            "--service-cidr",
            "10.0.250.0/24",
            "--tls-ca-file",
            "/etc/keel/ca.crt",
            "--tls-cert-file",
            "/etc/keel/controlplane.crt",
            "--tls-key-file",
            "/etc/keel/controlplane.key",
            "--tls-crl-file",
            "/etc/keel/crl.pem",
        ]));
        assert_eq!(config.state_dir, PathBuf::from("/var/db/keel-controlplane"));
    }
}
