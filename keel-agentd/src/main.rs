use keel_agentd::pf::PfController;
use keel_agentd::worker::{self, Command};
use keel_agentd::Reconciler;
use keel_jail::ProcessJailRuntime;
use keel_net::ProcessNetManager;
use keel_zfs::CliZfsManager;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

struct Config {
    pool: String,
    state_dir: PathBuf,
    socket: PathBuf,
    node_id: Option<String>,
    control_plane_addr: Option<String>,
    advertise_addr: Option<String>,
    replicate_addr: Option<String>,
    tls_ca_file: Option<PathBuf>,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    tls_crl_file: Option<PathBuf>,
    public_iface: Option<String>,
    dns_ovh_config: Option<PathBuf>,
    acme_directory_url: Option<String>,
    acme_account_key_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pool: "zroot".to_string(),
            state_dir: PathBuf::from("/var/db/keel"),
            socket: PathBuf::from("/var/run/keel-agentd.sock"),
            node_id: None,
            control_plane_addr: None,
            advertise_addr: None,
            replicate_addr: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_crl_file: None,
            public_iface: None,
            dns_ovh_config: None,
            acme_directory_url: None,
            acme_account_key_file: None,
        }
    }
}

#[derive(serde::Deserialize)]
struct OvhConfig {
    app_key: String,
    app_secret: String,
    consumer_key: String,
    zone: String,
}

fn load_ovh_config(path: &std::path::Path) -> OvhConfig {
    let content = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read OVH config at {}: {e}", path.display()));
    toml::from_str(&content).unwrap_or_else(|e| panic!("failed to parse OVH config at {}: {e}", path.display()))
}

fn parse_args() -> Config {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from(args: impl Iterator<Item = String>) -> Config {
    let mut config = Config::default();
    let mut args = args;
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| panic!("missing value for {flag}"));
        match flag.as_str() {
            "--pool" => config.pool = value,
            "--state-dir" => config.state_dir = PathBuf::from(value),
            "--socket" => config.socket = PathBuf::from(value),
            "--node-id" => config.node_id = Some(value),
            "--control-plane-addr" => config.control_plane_addr = Some(value),
            "--advertise-addr" => config.advertise_addr = Some(value),
            "--replicate-addr" => config.replicate_addr = Some(value),
            "--tls-ca-file" => config.tls_ca_file = Some(PathBuf::from(value)),
            "--tls-cert-file" => config.tls_cert_file = Some(PathBuf::from(value)),
            "--tls-key-file" => config.tls_key_file = Some(PathBuf::from(value)),
            "--tls-crl-file" => config.tls_crl_file = Some(PathBuf::from(value)),
            "--public-iface" => config.public_iface = Some(value),
            "--dns-ovh-config" => config.dns_ovh_config = Some(PathBuf::from(value)),
            "--acme-directory-url" => config.acme_directory_url = Some(value),
            "--acme-account-key-file" => config.acme_account_key_file = Some(PathBuf::from(value)),
            other => panic!("unknown flag: {other}"),
        }
    }
    if config.control_plane_addr.is_some()
        && (config.node_id.is_none()
            || config.advertise_addr.is_none()
            || config.replicate_addr.is_none()
            || config.tls_ca_file.is_none()
            || config.tls_cert_file.is_none()
            || config.tls_key_file.is_none()
            || config.tls_crl_file.is_none())
    {
        panic!(
            "--node-id, --advertise-addr, --replicate-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required when --control-plane-addr is set"
        );
    }
    config
}

fn main() {
    let config = parse_args();

    let zfs = CliZfsManager::new();
    // Created here (rather than after the reconciler, as before) so the same
    // shared slot can be handed to the reconciler below -- it must be the
    // identical instance that `registration::spawn`'s heartbeat loop and
    // `http::run`/`run_tls` populate/read, not an independent one, or the
    // reconciler's ingress-config regeneration would never see a real VIP.
    let service_vips = keel_agentd::ServiceVipSlot::new();
    let (acme, dns): (Box<dyn keel_ingress::AcmeClient + Send>, Box<dyn keel_ingress::DnsProvider + Send>) =
        match (&config.dns_ovh_config, &config.acme_directory_url, &config.acme_account_key_file) {
            (Some(ovh_config_path), Some(directory_url), Some(account_key_file)) => {
                let ovh_config = load_ovh_config(ovh_config_path);
                let dns = keel_ingress::OvhDnsProvider::new(ovh_config.app_key, ovh_config.app_secret, ovh_config.consumer_key, ovh_config.zone);
                let acme = keel_ingress::InstantAcmeClient::new(directory_url.clone(), account_key_file.clone())
                    .expect("failed to initialize the real ACME client");
                (Box::new(acme), Box::new(dns))
            }
            (None, None, None) => (Box::new(keel_ingress::FakeAcmeClient::new()), Box::new(keel_ingress::FakeDnsProvider::new())),
            _ => panic!("--dns-ovh-config, --acme-directory-url, and --acme-account-key-file must all be set together, or none of them"),
        };
    let reconciler = Reconciler::new(
        ProcessJailRuntime::new(),
        zfs.clone(),
        ProcessNetManager::new(),
        keel_jail::CliMountManager::new(),
        config.pool.clone(),
        config.state_dir.clone(),
        acme,
        dns,
        Box::new(keel_agentd::nginx::JexecNginxController::new(config.pool.clone())),
        service_vips.clone(),
    )
    .expect("failed to initialize reconciler from on-disk state");

    eprintln!(
        "keel-agentd: starting (pool={}, state_dir={}, socket={})",
        config.pool,
        config.state_dir.display(),
        config.socket.display()
    );

    let (_worker_handle, commands) = worker::spawn(reconciler, zfs.clone(), config.pool.clone());
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    commands
        .send(Command::ResumeReplicationLoops(resume_tx))
        .expect("worker command channel closed before startup completed");
    resume_rx.recv().expect("worker dropped without replying to ResumeReplicationLoops");
    let pod_cidr_slot = keel_agentd::PodCidrSlot::new();
    let replica_targets = keel_agentd::ReplicaTargetRegistry::load(config.state_dir.clone())
        .expect("failed to load replica-target state");

    if let (
        Some(node_id),
        Some(control_plane_addr),
        Some(advertise_addr),
        Some(replicate_addr),
        Some(ca_file),
        Some(cert_file),
        Some(key_file),
        Some(crl_file),
    ) = (
        config.node_id.clone(),
        config.control_plane_addr.clone(),
        config.advertise_addr.clone(),
        config.replicate_addr.clone(),
        config.tls_ca_file.clone(),
        config.tls_cert_file.clone(),
        config.tls_key_file.clone(),
        config.tls_crl_file.clone(),
    ) {
        let (capacity_cpu, capacity_memory) = keel_agentd::capacity::detect()
            .unwrap_or_else(|e| panic!("failed to detect node capacity via sysctl: {e}"));
        let reloading_tls = keel_agentd::tls::ReloadingTls::spawn(
            cert_file,
            key_file,
            ca_file,
            crl_file,
            Duration::from_secs(30),
        )
        .unwrap_or_else(|e| panic!("failed to load TLS configuration: {e}"));
        eprintln!(
            "keel-agentd: registering with control plane at {control_plane_addr} as node '{node_id}' ({advertise_addr}), capacity {capacity_cpu} cores / {capacity_memory} bytes"
        );
        keel_agentd::registration::spawn(
            node_id,
            advertise_addr.clone(),
            replicate_addr.clone(),
            control_plane_addr,
            Duration::from_secs(5),
            config.state_dir.clone(),
            capacity_cpu,
            capacity_memory,
            std::sync::Arc::clone(&reloading_tls),
            commands.clone(),
            pod_cidr_slot.clone(),
            service_vips.clone(),
        );

        eprintln!("keel-agentd: serving jails API over TLS on {advertise_addr}");
        let tcp_listener = TcpListener::bind(&advertise_addr)
            .unwrap_or_else(|e| panic!("failed to bind jails-API TCP listener on {advertise_addr}: {e}"));
        let tcp_commands = commands.clone();
        let tcp_pod_cidr_slot = pod_cidr_slot.clone();
        let tcp_service_vips = service_vips.clone();
        let tcp_replica_targets = replica_targets.clone();
        thread::spawn(move || {
            keel_agentd::http::run_tls(tcp_listener, tcp_commands, reloading_tls, tcp_pod_cidr_slot, tcp_service_vips, tcp_replica_targets)
        });

        eprintln!("keel-agentd: serving replication listener on {replicate_addr}");
        let replicate_listener = TcpListener::bind(&replicate_addr)
            .unwrap_or_else(|e| panic!("failed to bind replication TCP listener on {replicate_addr}: {e}"));
        let replicate_zfs = zfs.clone();
        let replicate_pool = config.pool.clone();
        let replicate_targets = replica_targets.clone();
        thread::spawn(move || keel_agentd::replication::run(replicate_listener, replicate_zfs, replicate_pool, replicate_targets));
    }

    let timer_commands = commands.clone();
    thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(5));
        if timer_commands.send(Command::Tick).is_err() {
            break;
        }
    });

    // Applied once at startup with its own retry loop, entirely separate
    // from `Reconciler::reconcile`'s per-Ingress loop: pf redirect rules are
    // a host-level, once-per-node concern, not something to re-derive per
    // Ingress. A failure here must not block per-Ingress cert/config
    // reconciliation, so it retries independently and only logs on error.
    if let Some(public_iface) = config.public_iface.clone() {
        thread::spawn(move || {
            let pf = keel_agentd::pf::PfctlController::new();
            let mut backoff = keel_agentd::backoff::BackoffState::new();
            loop {
                let now = std::time::Instant::now();
                if backoff.can_retry(now) {
                    backoff.record_attempt(now);
                    if let Err(e) = pf.ensure_redirect_rules(&public_iface, keel_agentd::record::INGRESS_JAIL_BRIDGE_ADDR) {
                        eprintln!("keel-agentd: failed to apply pf ingress redirect rules: {e}");
                    }
                }
                std::thread::sleep(Duration::from_secs(5));
            }
        });
    }

    if config.socket.exists() {
        std::fs::remove_file(&config.socket).expect("failed to remove stale socket file");
    }
    let listener = UnixListener::bind(&config.socket).expect("failed to bind Unix socket");
    std::fs::set_permissions(&config.socket, std::fs::Permissions::from_mode(0o600))
        .expect("failed to set socket permissions");

    keel_agentd::http::run(listener, commands, pod_cidr_slot, service_vips, replica_targets);
}

#[cfg(test)]
mod ovh_config_tests {
    use super::*;

    #[test]
    fn load_ovh_config_parses_a_well_formed_file() {
        let dir = std::env::temp_dir().join("keel-agentd-ovh-config-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dns-ovh.toml");
        std::fs::write(&path, "app_key = \"ak\"\napp_secret = \"as\"\nconsumer_key = \"ck\"\nzone = \"example.com\"\n").unwrap();
        let config = load_ovh_config(&path);
        assert_eq!(config.app_key, "ak");
        assert_eq!(config.zone, "example.com");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(strs: &[&str]) -> impl Iterator<Item = String> {
        strs.iter().map(|s| s.to_string()).collect::<Vec<_>>().into_iter()
    }

    #[test]
    fn defaults_have_no_control_plane_configuration() {
        let config = parse_args_from(args(&["--pool", "zroot"]));
        assert_eq!(config.node_id, None);
        assert_eq!(config.control_plane_addr, None);
        assert_eq!(config.advertise_addr, None);
        assert_eq!(config.replicate_addr, None);
        assert_eq!(config.tls_ca_file, None);
        assert_eq!(config.tls_cert_file, None);
        assert_eq!(config.tls_key_file, None);
        assert_eq!(config.tls_crl_file, None);
        assert_eq!(config.public_iface, None);
        assert_eq!(config.dns_ovh_config, None);
        assert_eq!(config.acme_directory_url, None);
        assert_eq!(config.acme_account_key_file, None);
    }

    #[test]
    fn parses_public_iface_flag() {
        let config = parse_args_from(args(&["--public-iface", "em0"]));
        assert_eq!(config.public_iface, Some("em0".to_string()));
    }

    #[test]
    fn parses_acme_and_dns_flags() {
        let config = parse_args_from(args(&[
            "--dns-ovh-config", "/usr/local/etc/keel/dns-ovh.toml",
            "--acme-directory-url", "https://acme.example.com/directory",
            "--acme-account-key-file", "/var/db/keel/acme-account.key",
        ]));
        assert_eq!(config.dns_ovh_config, Some(PathBuf::from("/usr/local/etc/keel/dns-ovh.toml")));
        assert_eq!(config.acme_directory_url, Some("https://acme.example.com/directory".to_string()));
        assert_eq!(config.acme_account_key_file, Some(PathBuf::from("/var/db/keel/acme-account.key")));
    }

    #[test]
    fn parses_all_eight_control_plane_flags() {
        let config = parse_args_from(args(&[
            "--node-id", "node-2",
            "--control-plane-addr", "192.168.64.2:7620",
            "--advertise-addr", "192.168.64.2",
            "--replicate-addr", "192.168.64.2:7622",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/node-2.crt",
            "--tls-key-file", "/etc/keel/node-2.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
        ]));
        assert_eq!(config.node_id, Some("node-2".to_string()));
        assert_eq!(config.control_plane_addr, Some("192.168.64.2:7620".to_string()));
        assert_eq!(config.advertise_addr, Some("192.168.64.2".to_string()));
        assert_eq!(config.replicate_addr, Some("192.168.64.2:7622".to_string()));
        assert_eq!(config.tls_ca_file, Some(PathBuf::from("/etc/keel/ca.crt")));
        assert_eq!(config.tls_cert_file, Some(PathBuf::from("/etc/keel/node-2.crt")));
        assert_eq!(config.tls_key_file, Some(PathBuf::from("/etc/keel/node-2.key")));
        assert_eq!(config.tls_crl_file, Some(PathBuf::from("/etc/keel/crl.pem")));
    }

    #[test]
    #[should_panic(expected = "--node-id, --advertise-addr, --replicate-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required when --control-plane-addr is set")]
    fn control_plane_addr_without_node_id_panics() {
        parse_args_from(args(&[
            "--control-plane-addr", "192.168.64.2:7620",
            "--advertise-addr", "192.168.64.2",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/node-2.crt",
            "--tls-key-file", "/etc/keel/node-2.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    #[should_panic(expected = "--node-id, --advertise-addr, --replicate-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required when --control-plane-addr is set")]
    fn control_plane_addr_without_advertise_addr_panics() {
        parse_args_from(args(&[
            "--control-plane-addr", "192.168.64.2:7620",
            "--node-id", "node-2",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/node-2.crt",
            "--tls-key-file", "/etc/keel/node-2.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    #[should_panic(expected = "--node-id, --advertise-addr, --replicate-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required when --control-plane-addr is set")]
    fn control_plane_addr_without_replicate_addr_panics() {
        parse_args_from(args(&[
            "--control-plane-addr", "192.168.64.2:7620",
            "--node-id", "node-2",
            "--advertise-addr", "192.168.64.2",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/node-2.crt",
            "--tls-key-file", "/etc/keel/node-2.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
        ]));
    }

    #[test]
    #[should_panic(expected = "--node-id, --advertise-addr, --replicate-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required when --control-plane-addr is set")]
    fn control_plane_addr_without_tls_crl_file_panics() {
        parse_args_from(args(&[
            "--control-plane-addr", "192.168.64.2:7620",
            "--node-id", "node-2",
            "--advertise-addr", "192.168.64.2",
            "--replicate-addr", "192.168.64.2:7622",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/node-2.crt",
            "--tls-key-file", "/etc/keel/node-2.key",
        ]));
    }
}
