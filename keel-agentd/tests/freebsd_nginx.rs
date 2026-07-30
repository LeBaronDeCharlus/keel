#![cfg(target_os = "freebsd")]

// Run as root on the dev VM, with a `keel-ingress` jail already running
// nginx (see this milestone's Task 19 for how that jail comes to exist).
// Usage: cargo test -p keel-agentd --test freebsd_nginx -- --ignored --test-threads=1

use keel_agentd::nginx::{JexecNginxController, NginxController};

#[test]
#[ignore]
fn write_test_and_reload_round_trip_against_a_real_running_nginx_jail() {
    let controller = JexecNginxController::new("zroot".to_string());
    let config = "user www; worker_processes 1;\nevents { worker_connections 1024; }\nhttp {\n    server { listen 80; return 200 'ok'; }\n}\n";
    controller.write_config("keel-ingress", config).unwrap();
    controller.test_config("keel-ingress").unwrap();
    controller.reload("keel-ingress").unwrap();
}

#[test]
#[ignore]
fn test_config_fails_on_a_deliberately_malformed_config() {
    let controller = JexecNginxController::new("zroot".to_string());
    controller
        .write_config("keel-ingress", "this is not valid nginx config {{{")
        .unwrap();
    assert!(controller.test_config("keel-ingress").is_err());
}
