#![cfg(target_os = "freebsd")]

use keel_jail::{CliMountManager, MountManager};
use std::path::Path;

// Run as root on the FreeBSD VM: `sudo cargo test -p keel-jail --test freebsd_mount`
// (mount(8)/umount(8) require root privileges).
//
// Requires a `zroot/keel/volumes` dataset to exist (same one-time bootstrap
// keel-zfs's freebsd_zfs test documents), used here purely as a real
// directory to nullfs-mount from — this test never calls into keel-zfs
// itself, it only needs a real source path that already exists.

#[test]
fn ensure_mount_point_creates_missing_parent_directories() {
    let mounts = CliMountManager::new();
    let target = Path::new("/tmp/keel-mount-test-ensure-mount-point/nested/data");
    let _ = std::fs::remove_dir_all("/tmp/keel-mount-test-ensure-mount-point");

    mounts.ensure_mount_point(target).expect("ensure_mount_point should succeed");
    assert!(target.is_dir());
}

#[test]
fn mount_nullfs_then_is_mounted_then_unmount_round_trips_through_the_real_kernel() {
    let mounts = CliMountManager::new();
    let source = Path::new("/zroot/keel/volumes");
    let target = Path::new("/tmp/keel-mount-test-round-trip");
    let _ = std::process::Command::new("umount").arg(target).output();
    std::fs::create_dir_all(target).unwrap();

    assert_eq!(mounts.is_mounted(target).unwrap(), false, "must not be mounted before mount_nullfs");

    mounts.mount_nullfs(source, target).expect("mount_nullfs should succeed");
    assert_eq!(mounts.is_mounted(target).unwrap(), true);

    mounts.unmount(target).expect("unmount should succeed");
    assert_eq!(mounts.is_mounted(target).unwrap(), false);
}

#[test]
fn mount_nullfs_called_twice_in_a_row_does_not_stack_a_duplicate_mount() {
    // Real mount(8) doesn't check is_mounted first and silently stacks a
    // second nullfs mount rather than erroring - reproduced here by calling
    // mount_nullfs twice and confirming only one mount(8) entry exists for
    // this target afterward (via `mount -p`'s own listing), not by relying
    // on unmount() alone (a stacked mount would still "successfully"
    // unmount once, leaving the first mount silently in place underneath).
    let mounts = CliMountManager::new();
    let source = Path::new("/zroot/keel/volumes");
    let target = Path::new("/tmp/keel-mount-test-no-duplicate-stacking");
    let _ = std::process::Command::new("umount").arg(target).output();
    let _ = std::process::Command::new("umount").arg(target).output();
    std::fs::create_dir_all(target).unwrap();

    mounts.mount_nullfs(source, target).expect("first mount_nullfs should succeed");
    mounts.mount_nullfs(source, target).expect("second mount_nullfs should succeed, not stack a duplicate");

    let output = std::process::Command::new("mount").arg("-p").output().unwrap();
    let target_str = target.to_string_lossy();
    let entries = String::from_utf8_lossy(&output.stdout).lines().filter(|line| line.contains(target_str.as_ref())).count();
    assert_eq!(entries, 1, "expected exactly one mount(8) entry for {target_str}, got {entries}");

    // Single real unmount must be enough - if this leaves the target still
    // mounted, a duplicate was stacked underneath after all.
    mounts.unmount(target).expect("unmount should succeed");
    assert_eq!(mounts.is_mounted(target).unwrap(), false, "one unmount must fully clear a mount_nullfs that was called twice");
}

#[test]
fn unmount_on_a_never_mounted_target_returns_not_mounted() {
    let mounts = CliMountManager::new();
    let target = Path::new("/tmp/keel-mount-test-never-mounted");
    std::fs::create_dir_all(target).unwrap();

    match mounts.unmount(target) {
        Err(keel_jail::MountError::NotMounted(p)) => assert_eq!(p, target),
        other => panic!("expected NotMounted for a target that was never mounted, got: {other:?}"),
    }
}
