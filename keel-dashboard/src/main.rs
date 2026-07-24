use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

struct Config {
    control_plane_addr: Option<String>,
    tls_ca_file: Option<PathBuf>,
    tls_cert_file: Option<PathBuf>,
    tls_key_file: Option<PathBuf>,
    tls_crl_file: Option<PathBuf>,
    listen_addr: String,
    dashboard_tls_cert_file: Option<PathBuf>,
    dashboard_tls_key_file: Option<PathBuf>,
    poll_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            control_plane_addr: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_crl_file: None,
            listen_addr: "0.0.0.0:8443".to_string(),
            dashboard_tls_cert_file: None,
            dashboard_tls_key_file: None,
            poll_interval_secs: 5,
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
        let value = args.next().unwrap_or_else(|| panic!("missing value for {flag}"));
        match flag.as_str() {
            "--control-plane-addr" => config.control_plane_addr = Some(value),
            "--tls-ca-file" => config.tls_ca_file = Some(PathBuf::from(value)),
            "--tls-cert-file" => config.tls_cert_file = Some(PathBuf::from(value)),
            "--tls-key-file" => config.tls_key_file = Some(PathBuf::from(value)),
            "--tls-crl-file" => config.tls_crl_file = Some(PathBuf::from(value)),
            "--listen-addr" => config.listen_addr = value,
            "--dashboard-tls-cert-file" => config.dashboard_tls_cert_file = Some(PathBuf::from(value)),
            "--dashboard-tls-key-file" => config.dashboard_tls_key_file = Some(PathBuf::from(value)),
            "--poll-interval-secs" => {
                config.poll_interval_secs =
                    value.parse().unwrap_or_else(|e| panic!("invalid --poll-interval-secs '{value}': {e}"))
            }
            other => panic!("unknown flag: {other}"),
        }
    }
    if config.control_plane_addr.is_none()
        || config.tls_ca_file.is_none()
        || config.tls_cert_file.is_none()
        || config.tls_key_file.is_none()
        || config.tls_crl_file.is_none()
    {
        panic!("--control-plane-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required");
    }
    if config.dashboard_tls_cert_file.is_none() || config.dashboard_tls_key_file.is_none() {
        panic!("--dashboard-tls-cert-file and --dashboard-tls-key-file are required");
    }
    config
}

fn main() {
    let config = parse_args();
    let control_plane_addr = config.control_plane_addr.expect("validated as required in parse_args_from");
    let tls_ca_file = config.tls_ca_file.expect("validated as required in parse_args_from");
    let tls_cert_file = config.tls_cert_file.expect("validated as required in parse_args_from");
    let tls_key_file = config.tls_key_file.expect("validated as required in parse_args_from");
    let tls_crl_file = config.tls_crl_file.expect("validated as required in parse_args_from");
    let dashboard_tls_cert_file = config.dashboard_tls_cert_file.expect("validated as required in parse_args_from");
    let dashboard_tls_key_file = config.dashboard_tls_key_file.expect("validated as required in parse_args_from");

    let client_config = Arc::new(
        keel_dashboard::tls::load_client_config(&tls_cert_file, &tls_key_file, &tls_ca_file, &tls_crl_file)
            .unwrap_or_else(|e| panic!("failed to load control-plane TLS client config: {e}")),
    );
    let client: Box<dyn keel_dashboard::control_plane_client::ControlPlaneClient> =
        Box::new(keel_dashboard::control_plane_client::TlsControlPlaneClient::new(control_plane_addr, client_config));
    let snapshot = keel_dashboard::poller::spawn(client, Duration::from_secs(config.poll_interval_secs));

    let server_config = Arc::new(
        keel_dashboard::tls::load_browser_server_config(&dashboard_tls_cert_file, &dashboard_tls_key_file)
            .unwrap_or_else(|e| panic!("failed to load dashboard TLS server config: {e}")),
    );

    eprintln!("keel-dashboard: starting (listen_addr={})", config.listen_addr);
    let listener = std::net::TcpListener::bind(&config.listen_addr).expect("failed to bind TCP listener");
    keel_dashboard::http::run(listener, server_config, snapshot);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(strs: &[&str]) -> impl Iterator<Item = String> {
        strs.iter().map(|s| s.to_string()).collect::<Vec<_>>().into_iter()
    }

    fn full_args() -> Vec<&'static str> {
        vec![
            "--control-plane-addr", "10.0.0.1:7620",
            "--tls-ca-file", "/etc/keel/ca.crt",
            "--tls-cert-file", "/etc/keel/dashboard.crt",
            "--tls-key-file", "/etc/keel/dashboard.key",
            "--tls-crl-file", "/etc/keel/crl.pem",
            "--dashboard-tls-cert-file", "/etc/keel/dashboard-browser.crt",
            "--dashboard-tls-key-file", "/etc/keel/dashboard-browser.key",
        ]
    }

    #[test]
    fn parses_all_required_flags_and_applies_defaults() {
        let config = parse_args_from(args(&full_args()));
        assert_eq!(config.control_plane_addr, Some("10.0.0.1:7620".to_string()));
        assert_eq!(config.listen_addr, "0.0.0.0:8443");
        assert_eq!(config.poll_interval_secs, 5);
    }

    #[test]
    fn parses_a_custom_poll_interval() {
        let mut full = full_args();
        full.extend(["--poll-interval-secs", "10"]);
        let config = parse_args_from(args(&full));
        assert_eq!(config.poll_interval_secs, 10);
    }

    #[test]
    #[should_panic(expected = "--control-plane-addr, --tls-ca-file, --tls-cert-file, --tls-key-file, and --tls-crl-file are all required")]
    fn missing_control_plane_tls_flag_panics() {
        parse_args_from(args(&["--tls-ca-file", "/etc/keel/ca.crt"]));
    }

    #[test]
    #[should_panic(expected = "--dashboard-tls-cert-file and --dashboard-tls-key-file are required")]
    fn missing_dashboard_tls_flag_panics() {
        let mut partial: Vec<&str> = full_args().into_iter().take(10).collect();
        partial.retain(|f| *f != "--dashboard-tls-cert-file" && *f != "/etc/keel/dashboard-browser.crt");
        parse_args_from(args(&partial));
    }
}
