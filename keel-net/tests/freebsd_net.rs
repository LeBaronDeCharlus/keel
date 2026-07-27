#![cfg(target_os = "freebsd")]

use keel_jail::{JailRuntime, ProcessJailRuntime};
use keel_net::{NetManager, ProcessNetManager};
use std::path::Path;
use std::process::Command;

// Run as root on the FreeBSD VM: `sudo cargo test -p keel-net --test freebsd_net`

fn destroy_interface_if_exists(name: &str) {
    let _ = Command::new("ifconfig").args([name, "destroy"]).output();
}

fn destroy_route_if_exists(subnet: &str) {
    let _ = Command::new("route")
        .args(["delete", "-net", subnet])
        .output();
}

fn make_test_jail(name: &str) -> ProcessJailRuntime {
    let jails = ProcessJailRuntime::new();
    let _ = jails.destroy(name);
    let rootfs = Path::new("/tmp").join(format!("{name}-rootfs"));
    let bin_dir = rootfs.join("sbin");
    std::fs::create_dir_all(&bin_dir).unwrap();
    std::fs::copy("/rescue/ifconfig", bin_dir.join("ifconfig"))
        .expect("copy /rescue/ifconfig into test rootfs");
    std::fs::copy("/rescue/route", bin_dir.join("route"))
        .expect("copy /rescue/route into test rootfs");
    jails
        .create(name, &rootfs, true)
        .expect("create should succeed"); // vnet: true
    jails
}

#[test]
fn ensure_bridge_exists_creates_and_is_idempotent() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br0";
    destroy_interface_if_exists(bridge);

    net.ensure_bridge_exists(bridge)
        .expect("first call should create the bridge");
    let check = Command::new("ifconfig")
        .arg(bridge)
        .output()
        .expect("ifconfig should run");
    assert!(
        check.status.success(),
        "bridge should exist after ensure_bridge_exists"
    );

    net.ensure_bridge_exists(bridge)
        .expect("second call should be a no-op success");

    destroy_interface_if_exists(bridge);
}

#[test]
fn attach_jail_wires_up_epair_and_configures_address() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br1";
    let jail_name = "keel-net-test-attach";
    let epair_base = "epair50";

    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
    let jails = make_test_jail(jail_name);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");
    net.attach_jail(jail_name, bridge, epair_base, "10.99.0.5/24")
        .expect("attach_jail should succeed");

    let inside = Command::new("jexec")
        .args([jail_name, "/sbin/ifconfig", &format!("{epair_base}b")])
        .output()
        .expect("jexec ifconfig should run");
    let inside_output = String::from_utf8_lossy(&inside.stdout);
    assert!(
        inside_output.contains("10.99.0.5"),
        "expected configured address in: {inside_output}"
    );

    jails
        .destroy(jail_name)
        .expect("cleanup destroy should succeed");
    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
}

#[test]
fn attach_jail_tolerates_retry_after_epair_already_created() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br2";
    let jail_name = "keel-net-test-retry";
    let epair_base = "epair51";

    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
    let jails = make_test_jail(jail_name);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");
    net.attach_jail(jail_name, bridge, epair_base, "10.99.0.6/24")
        .expect("first attach_jail should succeed");

    // Simulate a retry after an interrupted prior attempt: calling
    // attach_jail again for the same epair_base (now fully wired into the
    // jail) must not error.
    net.attach_jail(jail_name, bridge, epair_base, "10.99.0.6/24")
        .expect("retried attach_jail should tolerate already-attached state");

    jails
        .destroy(jail_name)
        .expect("cleanup destroy should succeed");
    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
}

#[test]
fn detach_jail_removes_epair_and_is_idempotent() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br3";
    let jail_name = "keel-net-test-detach";
    let epair_base = "epair52";

    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
    let jails = make_test_jail(jail_name);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");
    net.attach_jail(jail_name, bridge, epair_base, "10.99.0.7/24")
        .expect("attach_jail should succeed");

    net.detach_jail(epair_base)
        .expect("detach_jail should succeed");

    let check = Command::new("ifconfig")
        .arg(format!("{epair_base}a"))
        .output()
        .expect("ifconfig should run");
    assert!(
        !check.status.success(),
        "epair should no longer exist on the host after detach"
    );

    // Idempotent: detaching an already-detached epair must not error.
    net.detach_jail(epair_base)
        .expect("second detach_jail call should be a no-op success");

    jails
        .destroy(jail_name)
        .expect("cleanup destroy should succeed");
    destroy_interface_if_exists(bridge);
}

#[test]
fn attach_jail_returns_not_found_for_missing_bridge() {
    let net = ProcessNetManager::new();
    let jail_name = "keel-net-test-missing-bridge";
    let epair_base = "epair60";

    destroy_interface_if_exists(&format!("{epair_base}a"));
    let jails = make_test_jail(jail_name);

    let result = net.attach_jail(
        jail_name,
        "keel-nonexistent-bridge",
        epair_base,
        "10.99.0.9/24",
    );
    assert!(
        matches!(result, Err(keel_net::NetError::NotFound(_))),
        "expected NotFound, got {result:?}"
    );

    // Confirm no epair was created, since the bridge check happens before anything else.
    let check = Command::new("ifconfig")
        .arg(format!("{epair_base}a"))
        .output()
        .expect("ifconfig should run");
    assert!(
        !check.status.success(),
        "no epair should have been created when the bridge check fails first"
    );

    jails
        .destroy(jail_name)
        .expect("cleanup destroy should succeed");
}

#[test]
fn detach_before_destroy_works_while_jail_is_still_running() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br4";
    let jail_name = "keel-net-test-detach-order";
    let epair_base = "epair53";

    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
    let jails = make_test_jail(jail_name);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");
    net.attach_jail(jail_name, bridge, epair_base, "10.99.0.8/24")
        .expect("attach_jail should succeed");

    // Detach while the jail is still running, matching the Reconciliation
    // Loop's stated order (detach network, then destroy the jail).
    net.detach_jail(epair_base)
        .expect("detach_jail should succeed on a running jail");
    assert_eq!(
        jails.is_running(jail_name).unwrap(),
        false,
        "no command was ever started in this jail"
    );

    jails
        .destroy(jail_name)
        .expect("destroy after detach should still succeed");
    destroy_interface_if_exists(bridge);
}

#[test]
fn attach_jail_assigns_bridge_gateway_and_jail_default_route() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br5";
    let jail_name = "keel-net-test-gateway";
    let epair_base = "epair54";

    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
    let jails = make_test_jail(jail_name);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");
    net.attach_jail(jail_name, bridge, epair_base, "10.99.20.5/24")
        .expect("attach_jail should succeed");

    let bridge_check = Command::new("ifconfig")
        .arg(bridge)
        .output()
        .expect("ifconfig should run");
    let bridge_output = String::from_utf8_lossy(&bridge_check.stdout);
    assert!(
        bridge_output.contains("10.99.20.1"),
        "expected bridge gateway address in: {bridge_output}"
    );

    let route_check = Command::new("jexec")
        .args([jail_name, "/sbin/route", "-n", "get", "default"])
        .output()
        .expect("jexec route should run");
    let route_output = String::from_utf8_lossy(&route_check.stdout);
    assert!(
        route_output.contains("10.99.20.1"),
        "expected default route via bridge gateway in: {route_output}"
    );

    jails
        .destroy(jail_name)
        .expect("cleanup destroy should succeed");
    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
}

#[test]
fn attach_jail_supports_two_jails_on_the_same_bridge() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br6";
    let jail_a = "keel-net-test-multi-a";
    let jail_b = "keel-net-test-multi-b";
    let epair_a = "epair55";
    let epair_b = "epair56";

    destroy_interface_if_exists(&format!("{epair_a}a"));
    destroy_interface_if_exists(&format!("{epair_b}a"));
    destroy_interface_if_exists(bridge);
    let jails_a = make_test_jail(jail_a);
    let jails_b = make_test_jail(jail_b);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");

    // Both jails live in the same /24, so bridge_gateway() computes the
    // identical gateway address for both. The second attach_jail call's
    // "ifconfig <bridge> inet <gateway>" must tolerate FreeBSD's
    // "File exists" error instead of hard-failing, since the gateway was
    // already assigned by the first call. This is the multi-jail-per-node
    // regression this fix addresses.
    net.attach_jail(jail_a, bridge, epair_a, "10.99.23.5/24")
        .expect("first jail's attach_jail should succeed");
    net.attach_jail(jail_b, bridge, epair_b, "10.99.23.6/24")
        .expect("second jail's attach_jail on the same bridge should also succeed");

    jails_a
        .destroy(jail_a)
        .expect("cleanup destroy should succeed");
    jails_b
        .destroy(jail_b)
        .expect("cleanup destroy should succeed");
    destroy_interface_if_exists(&format!("{epair_a}a"));
    destroy_interface_if_exists(&format!("{epair_b}a"));
    destroy_interface_if_exists(bridge);
}

#[test]
fn attach_jail_supports_two_distinct_subnets_gateways_coexisting_on_one_bridge() {
    // Reproduces a real bug found during Milestone 21 VM verification:
    // `attach_jail`'s gateway address used to be set with a plain
    // `ifconfig <bridge> inet <addr>` (no `alias`), which *replaces* the
    // bridge's primary address rather than adding a second one alongside
    // it. Every milestone before 21 only ever put jails from the SAME
    // subnet (one node's own pod-CIDR) on a given bridge, so this was
    // never exercised - Milestone 21 was the first to put a second,
    // unrelated subnet (the singleton ingress jail's fixed `10.0.0.0/24`)
    // on the same bridge as the node's ordinary jails, and attaching the
    // second subnet's jail silently evicted the first subnet's gateway,
    // breaking every already-running jail in that first subnet.
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br8";
    let jail_a = "keel-net-test-subnet-a";
    let jail_b = "keel-net-test-subnet-b";
    let epair_a = "epair58";
    let epair_b = "epair59";

    destroy_interface_if_exists(&format!("{epair_a}a"));
    destroy_interface_if_exists(&format!("{epair_b}a"));
    destroy_interface_if_exists(bridge);
    let jails_a = make_test_jail(jail_a);
    let jails_b = make_test_jail(jail_b);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");

    // Two completely distinct /24 subnets on the same bridge - not two
    // jails sharing one subnet, which `attach_jail_supports_two_jails_
    // on_the_same_bridge` above already covers.
    net.attach_jail(jail_a, bridge, epair_a, "10.99.30.5/24")
        .expect("first subnet's attach_jail should succeed");
    net.attach_jail(jail_b, bridge, epair_b, "10.99.31.5/24")
        .expect("second subnet's attach_jail should succeed");

    let bridge_check = Command::new("ifconfig")
        .arg(bridge)
        .output()
        .expect("ifconfig should run");
    let bridge_output = String::from_utf8_lossy(&bridge_check.stdout);
    assert!(
        bridge_output.contains("10.99.30.1"),
        "expected the first subnet's gateway to still be present after attaching the second subnet's jail: {bridge_output}"
    );
    assert!(
        bridge_output.contains("10.99.31.1"),
        "expected the second subnet's gateway to be present: {bridge_output}"
    );

    let route_a = Command::new("jexec")
        .args([jail_a, "/sbin/route", "-n", "get", "default"])
        .output()
        .expect("jexec route should run");
    assert!(
        String::from_utf8_lossy(&route_a.stdout).contains("10.99.30.1"),
        "first jail's default route should still point at its own gateway"
    );

    jails_a
        .destroy(jail_a)
        .expect("cleanup destroy should succeed");
    jails_b
        .destroy(jail_b)
        .expect("cleanup destroy should succeed");
    destroy_interface_if_exists(&format!("{epair_a}a"));
    destroy_interface_if_exists(&format!("{epair_b}a"));
    destroy_interface_if_exists(bridge);
}

#[test]
fn attach_jail_tolerates_restart_with_same_address() {
    let net = ProcessNetManager::new();
    let bridge = "keel-test-br7";
    let jail_name = "keel-net-test-restart";
    let epair_base = "epair57";

    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
    let jails = make_test_jail(jail_name);

    net.ensure_bridge_exists(bridge)
        .expect("bridge should be created");
    net.attach_jail(jail_name, bridge, epair_base, "10.99.22.5/24")
        .expect("first attach_jail should succeed");

    // Simulate a jail restart: attach_jail is re-run for the same jail with
    // the identical bridge/epair/address. The bridge gateway assignment and
    // jail default route steps must tolerate FreeBSD's "already configured"
    // errors here, matching the epair/addm steps' existing idempotency.
    net.attach_jail(jail_name, bridge, epair_base, "10.99.22.5/24")
        .expect("attach_jail after a simulated restart should succeed");

    jails
        .destroy(jail_name)
        .expect("cleanup destroy should succeed");
    destroy_interface_if_exists(&format!("{epair_base}a"));
    destroy_interface_if_exists(bridge);
}

#[test]
fn add_route_then_remove_route_round_trips_through_the_kernel_table() {
    let net = ProcessNetManager::new();
    let subnet = "10.99.9.0/24";
    destroy_route_if_exists(subnet);

    net.add_route(subnet, "127.0.0.1")
        .expect("add_route should succeed");
    let check = Command::new("netstat")
        .args(["-rn", "-f", "inet"])
        .output()
        .expect("netstat should run");
    let table = String::from_utf8_lossy(&check.stdout);
    assert!(
        table.contains("10.99.9"),
        "expected the route to appear in the kernel table: {table}"
    );

    net.remove_route(subnet)
        .expect("remove_route should succeed");
    let check = Command::new("netstat")
        .args(["-rn", "-f", "inet"])
        .output()
        .expect("netstat should run");
    let table = String::from_utf8_lossy(&check.stdout);
    assert!(
        !table.contains("10.99.9"),
        "expected the route to be gone from the kernel table: {table}"
    );
}

#[test]
fn add_route_and_remove_route_are_idempotent_against_the_real_kernel() {
    let net = ProcessNetManager::new();
    let subnet = "10.99.10.0/24";
    destroy_route_if_exists(subnet);

    net.add_route(subnet, "127.0.0.1")
        .expect("first add_route should succeed");
    net.add_route(subnet, "127.0.0.1")
        .expect("second add_route should tolerate the duplicate");

    net.remove_route(subnet)
        .expect("first remove_route should succeed");
    net.remove_route(subnet)
        .expect("second remove_route should tolerate the missing route");
}
