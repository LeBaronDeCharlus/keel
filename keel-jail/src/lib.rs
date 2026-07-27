pub mod error;
pub mod fake;
pub mod mount_cli;
pub mod mount_error;
pub mod mount_fake;
pub mod process;

pub use error::JailError;
pub use fake::FakeJailRuntime;
pub use mount_cli::CliMountManager;
pub use mount_error::MountError;
pub use mount_fake::FakeMountManager;
pub use process::ProcessJailRuntime;

use std::path::Path;

pub trait MountManager {
    /// Creates `target` (and any missing parent directories) if it doesn't
    /// already exist. Routed through this trait rather than a raw
    /// `std::fs::create_dir_all` call so a `FakeMountManager`-backed test
    /// never touches the real filesystem at an absolute rootfs path that
    /// doesn't exist outside a real ZFS-backed node.
    fn ensure_mount_point(&self, target: &Path) -> Result<(), MountError>;
    fn mount_nullfs(&self, source: &Path, target: &Path) -> Result<(), MountError>;
    fn unmount(&self, target: &Path) -> Result<(), MountError>;
    fn is_mounted(&self, target: &Path) -> Result<bool, MountError>;
}

pub trait JailRuntime {
    /// Creates a persistent, empty jail with no command running yet
    /// (uses `jail -c ... persist`).
    fn create(&self, name: &str, rootfs: &Path, vnet: bool) -> Result<(), JailError>;

    /// Checks only whether the jail itself exists — not whether a command
    /// is running inside it. Needed because `is_running` collapses "jail
    /// doesn't exist" and "jail exists but its process exited" into the
    /// same `false`; callers that need to distinguish "provision from
    /// scratch" from "just restart the command" need this method instead.
    fn jail_exists(&self, name: &str) -> Result<bool, JailError>;

    /// Non-blocking: spawns the command and returns immediately. A launch
    /// failure *inside* the jail (bad command, missing binary) is NOT
    /// reported by this method's `Ok` return — callers must re-check
    /// `is_running` afterward to confirm the process actually started and
    /// stayed up.
    fn start_command(&self, name: &str, command: &[String]) -> Result<(), JailError>;
    fn destroy(&self, name: &str) -> Result<(), JailError>;

    /// Means "the jail exists and has at least one non-zombie process
    /// running in it" — not merely "the jail exists".
    fn is_running(&self, name: &str) -> Result<bool, JailError>;

    /// `pcpu_percent` is cores × 100 (so 2 cores = `200`, not `2`). The two
    /// rctl rules (pcpu, vmemoryuse) are not applied atomically — if the
    /// second fails, the first remains in effect until
    /// `remove_resource_limits` is called.
    fn set_resource_limits(
        &self,
        name: &str,
        pcpu_percent: u32,
        memory_bytes: u64,
    ) -> Result<(), JailError>;
    fn remove_resource_limits(&self, name: &str) -> Result<(), JailError>;
}
