#![cfg(target_os = "freebsd")]

use keel_jail::{JailRuntime, ProcessJailRuntime};
use keel_zfs::{CliZfsManager, ZfsManager};
use std::path::Path;
use std::{thread, time::Duration};

// Run as root on the FreeBSD VM: `sudo cargo test -p keel-jail --test freebsd_lifecycle`
// (jail(8)/jls(8)/jexec(8) require root privileges).
//
// Requires a `zroot/keel/base/test` dataset to exist (same prerequisite as
// keel-zfs's own freebsd_zfs test), and, for the reap-on-destroy test below
// specifically, that dataset must contain a real, runnable `/bin/sh` (a
// dynamically linked binary needs its shared libs and `/libexec/ld-elf.so.1`
// alongside it too) — a minimal userland, not just an empty dataset.

#[test]
fn create_destroy_and_is_running_lifecycle() {
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-lifecycle";
    let rootfs = Path::new("/tmp/keel-test-lifecycle-rootfs");
    std::fs::create_dir_all(rootfs).unwrap();

    let _ = runtime.destroy(name);

    runtime
        .create(name, rootfs, false)
        .expect("create should succeed");
    assert_eq!(
        runtime.is_running(name).unwrap(),
        false,
        "no command started yet"
    );

    runtime.destroy(name).expect("destroy should succeed");
    assert_eq!(
        runtime.is_running(name).unwrap(),
        false,
        "destroyed jail is not running"
    );
}

#[test]
fn start_command_makes_is_running_true() {
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-start-command";
    let rootfs = Path::new("/tmp/keel-test-start-command-rootfs");
    let bin_dir = rootfs.join("bin");
    std::fs::create_dir_all(&bin_dir).unwrap();
    std::fs::copy("/rescue/sh", bin_dir.join("sh")).expect("copy /rescue/sh into test rootfs");
    std::fs::copy("/rescue/sleep", bin_dir.join("sleep"))
        .expect("copy /rescue/sleep into test rootfs");

    let _ = runtime.destroy(name);
    runtime
        .create(name, rootfs, false)
        .expect("create should succeed");

    runtime
        .start_command(
            name,
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "sleep 30".to_string(),
            ],
        )
        .expect("start_command should succeed");

    // Give jexec a moment to actually fork/exec before checking.
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        runtime.is_running(name).unwrap(),
        true,
        "sleep 30 should still be running"
    );

    runtime.destroy(name).expect("destroy should succeed");
}

#[test]
fn set_and_remove_resource_limits() {
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-resource-limits";
    let rootfs = Path::new("/tmp/keel-test-resource-limits-rootfs");
    std::fs::create_dir_all(rootfs).unwrap();

    let _ = runtime.remove_resource_limits(name);
    let _ = runtime.destroy(name);
    runtime
        .create(name, rootfs, false)
        .expect("create should succeed");

    runtime
        .set_resource_limits(name, 200, 512 * 1024 * 1024)
        .expect("set_resource_limits should succeed");

    let output = std::process::Command::new("rctl")
        .arg(format!("jail:{name}"))
        .output()
        .expect("rctl should run");
    let rules = String::from_utf8_lossy(&output.stdout);
    assert!(
        rules.contains("pcpu:deny=200"),
        "expected pcpu rule in: {rules}"
    );
    assert!(
        rules.contains("vmemoryuse:deny=536870912"),
        "expected vmemoryuse rule in: {rules}"
    );

    runtime
        .remove_resource_limits(name)
        .expect("remove_resource_limits should succeed");
    let output = std::process::Command::new("rctl")
        .arg(format!("jail:{name}"))
        .output()
        .expect("rctl should run");
    assert!(
        String::from_utf8_lossy(&output.stdout).trim().is_empty(),
        "rules should be gone after removal"
    );

    runtime.destroy(name).expect("destroy should succeed");
}

#[test]
fn remove_resource_limits_on_jail_with_no_limits_set_is_a_no_op_success() {
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-no-limits-set";
    let rootfs = Path::new("/tmp/keel-test-no-limits-set-rootfs");
    std::fs::create_dir_all(rootfs).unwrap();

    let _ = runtime.destroy(name);
    runtime
        .create(name, rootfs, false)
        .expect("create should succeed");

    runtime
        .remove_resource_limits(name)
        .expect("removing limits that were never set should succeed, not error");

    runtime.destroy(name).expect("destroy should succeed");
}

#[test]
fn destroy_on_a_never_created_jail_returns_not_found() {
    // Reproduces a real bug found during Milestone 8 VM verification:
    // `destroy` always mapped a failing `jail -r` to `JailError::
    // CommandFailed`, never `JailError::NotFound`, so `Reconciler::
    // delete`'s documented tolerance for "a record that was applied but
    // never got as far as being provisioned" (added in Milestone 4) never
    // actually engaged against the real jail runtime, only against
    // `FakeJailRuntime`'s test double, whose `destroy` already returns
    // `NotFound` directly.
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-destroy-never-created";

    let _ = runtime.destroy(name);

    match runtime.destroy(name) {
        Err(keel_jail::JailError::NotFound(n)) => assert_eq!(n, name),
        other => panic!("expected NotFound for a jail that was never created, got: {other:?}"),
    }
}

#[test]
fn create_mounts_devfs_so_dev_null_exists_inside_the_jail_and_destroy_unmounts_it() {
    // Reproduces a real bug found during Milestone 21 VM verification:
    // `create` never mounted `devfs`, so a jail's own `/dev` was an empty
    // directory. Every prior real-VM test happened to only run
    // statically-linked `/rescue/*` binaries or shell builtins, none of
    // which touch `/dev` from inside the jail, so this went undetected
    // until a real dynamically-linked workload (a Python interpreter)
    // needed `/dev/null` for its own dynamic linker and failed outright.
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-devfs";
    let rootfs = Path::new("/tmp/keel-test-devfs-rootfs");
    std::fs::create_dir_all(rootfs.join("dev")).unwrap();

    let _ = runtime.destroy(name);
    runtime
        .create(name, rootfs, false)
        .expect("create should succeed");

    assert!(
        rootfs.join("dev/null").exists(),
        "expected /dev/null to exist inside the jail's rootfs after create"
    );

    runtime.destroy(name).expect("destroy should succeed");

    let mount_output = std::process::Command::new("mount")
        .output()
        .expect("mount should run");
    let mount_table = String::from_utf8_lossy(&mount_output.stdout);
    assert!(
        !mount_table.contains(&*rootfs.join("dev").to_string_lossy()),
        "devfs should be unmounted from the jail's rootfs after destroy, but mount(8) still shows it:\n{mount_table}"
    );
}

#[test]
fn jail_exists_distinguishes_created_from_never_existed() {
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-jail-exists";
    let rootfs = Path::new("/tmp/keel-test-jail-exists-rootfs");
    std::fs::create_dir_all(rootfs).unwrap();

    let _ = runtime.destroy(name);
    assert_eq!(
        runtime.jail_exists(name).unwrap(),
        false,
        "should not exist before create"
    );

    runtime
        .create(name, rootfs, false)
        .expect("create should succeed");
    assert_eq!(
        runtime.jail_exists(name).unwrap(),
        true,
        "should exist after create"
    );

    runtime.destroy(name).expect("destroy should succeed");
    assert_eq!(
        runtime.jail_exists(name).unwrap(),
        false,
        "should not exist after destroy"
    );
}

#[test]
fn destroy_reaps_the_spawned_command_so_its_dataset_can_be_destroyed_immediately() {
    // Reproduces a real bug found during Milestone 5 VM verification:
    // `destroy` used to only run `jail -r` without reaping the process
    // `start_command` had spawned into it, leaving a zombie that held a
    // reference into the jail's rootfs mount — a caller's immediately
    // following `zfs destroy` of that dataset then failed with "device
    // busy" (this is exactly the sequence `keel-agentd`'s `Reconciler::
    // delete` runs: detach network, destroy jail, destroy dataset).
    let jails = ProcessJailRuntime::new();
    let zfs = CliZfsManager::new();
    let name = "keel-test-destroy-reaps-child";
    let dataset = "zroot/keel/jails/keel-test-destroy-reaps-child";

    let _ = jails.destroy(name);
    let _ = zfs.destroy_dataset(dataset);

    zfs.clone_from_base("zroot/keel/base/test", dataset)
        .expect("clone_from_base should succeed");
    let rootfs = Path::new("/").join(dataset);
    jails
        .create(name, &rootfs, false)
        .expect("create should succeed");
    // `:` is a shell builtin (no-op) so this only needs `/bin/sh` inside
    // the test base image, not any other binary.
    jails
        .start_command(
            name,
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "while true; do :; done".to_string(),
            ],
        )
        .expect("start_command should succeed");
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        jails.is_running(name).unwrap(),
        true,
        "the spawned command should be running"
    );

    jails.destroy(name).expect("destroy should succeed");

    // No sleep/retry here: this must work on the very next call, since
    // that's exactly how `Reconciler::delete` chains `jails.destroy` then
    // `zfs.destroy_dataset` with nothing in between.
    zfs.destroy_dataset(dataset)
        .expect("dataset should be destroyable immediately after destroy reaps its child");
}

#[test]
fn start_command_does_not_leak_stdio_into_the_jailed_process() {
    // Reproduces a real bug found during Milestone 6 VM verification: a
    // long-running jailed process that inherited keel-agentd's own
    // stdout/stderr held a supervisor's (daemon(8) -S's) relay pipe open
    // indefinitely, so the supervisor never saw EOF and never noticed
    // keel-agentd itself had died, silently breaking restart-on-crash for
    // any jail with an active command. `start_command` must give the
    // jailed process its own, disconnected stdio (here, /dev/null) rather
    // than inheriting the caller's.
    let runtime = ProcessJailRuntime::new();
    let name = "keel-test-stdio-isolation";
    let rootfs = Path::new("/tmp/keel-test-stdio-isolation-rootfs");
    let bin_dir = rootfs.join("bin");
    std::fs::create_dir_all(&bin_dir).unwrap();
    std::fs::copy("/rescue/sh", bin_dir.join("sh")).expect("copy /rescue/sh into test rootfs");

    let _ = runtime.destroy(name);
    runtime
        .create(name, rootfs, false)
        .expect("create should succeed");

    // `:` is a shell builtin (no-op), needing no binary beyond `/bin/sh`
    // itself — same pattern as `destroy_reaps_the_spawned_command...`
    // above, avoiding any dependence on which other utilities happen to be
    // reachable inside the jail's minimal rootfs.
    runtime
        .start_command(
            name,
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "while true; do :; done".to_string(),
            ],
        )
        .expect("start_command should succeed");
    thread::sleep(Duration::from_millis(200));

    let jid_output = std::process::Command::new("jls")
        .args(["-j", name, "jid"])
        .output()
        .expect("jls should run");
    let jid = String::from_utf8_lossy(&jid_output.stdout)
        .trim()
        .to_string();
    assert!(!jid.is_empty(), "expected the jail to have a jid");

    let pid_output = std::process::Command::new("ps")
        .args(["-J", &jid, "-o", "pid="])
        .output()
        .expect("ps should run");
    let pid = String::from_utf8_lossy(&pid_output.stdout)
        .trim()
        .to_string();
    assert!(
        !pid.is_empty(),
        "expected to find a process running inside the jail"
    );

    let procstat_output = std::process::Command::new("procstat")
        .args(["-f", &pid])
        .output()
        .expect("procstat should run");
    let procstat_text = String::from_utf8_lossy(&procstat_output.stdout);
    for line in procstat_text.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let (Some(fd), Some(kind)) = (fields.get(2), fields.get(3)) else {
            continue;
        };
        if matches!(*fd, "0" | "1" | "2") {
            assert_ne!(
                *kind, "p",
                "fd {fd} of the jailed process must not be a pipe (would mean it inherited \
                 keel-agentd's own stdio): {line}\nfull procstat output:\n{procstat_text}"
            );
        }
    }

    runtime.destroy(name).expect("destroy should succeed");
}
