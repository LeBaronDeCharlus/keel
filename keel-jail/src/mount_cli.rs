use crate::MountError;
use crate::MountManager;
use std::path::Path;
use std::process::{Command, Output};

pub struct CliMountManager;

impl CliMountManager {
    pub fn new() -> Self {
        Self
    }

    fn run(program: &str, args: &[&str]) -> Result<Output, MountError> {
        Command::new(program)
            .args(args)
            .output()
            .map_err(|e| MountError::Spawn(program.to_string(), e))
    }

    fn run_checked(program: &str, args: &[&str]) -> Result<(), MountError> {
        let output = Self::run(program, args)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(MountError::CommandFailed(
                format!("{program} {}", args.join(" ")),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }
}

impl Default for CliMountManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MountManager for CliMountManager {
    fn ensure_mount_point(&self, target: &Path) -> Result<(), MountError> {
        std::fs::create_dir_all(target)?;
        Ok(())
    }

    fn mount_nullfs(&self, source: &Path, target: &Path) -> Result<(), MountError> {
        // Real mount(8) doesn't check this itself: naming an already-mounted
        // target doesn't error, it silently stacks a second nullfs mount on
        // top. `FakeMountManager` (a `HashSet`) is safe to call repeatedly
        // by construction, so this check is what makes the real
        // implementation honor that same contract, rather than leaving it
        // to every call site to remember to guard with `is_mounted` first.
        if self.is_mounted(target)? {
            return Ok(());
        }
        Self::run_checked("mount", &["-t", "nullfs", &source.to_string_lossy(), &target.to_string_lossy()])
    }

    fn unmount(&self, target: &Path) -> Result<(), MountError> {
        let output = Self::run("umount", &[&target.to_string_lossy()])?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Verified directly against a real FreeBSD 15.1 VM: `umount` prints
        // `umount: <path>: not a file system root directory` and exits
        // non-zero both for a path that was never a mount point and for one
        // that already got unmounted (the exact tolerance `Reconciler::
        // delete` needs for a volume it never got as far as mounting, or
        // one it already unmounted on a prior, partially-completed delete).
        if stderr.contains("not a file system root directory") {
            return Err(MountError::NotMounted(target.to_path_buf()));
        }
        Err(MountError::CommandFailed(
            format!("umount {}", target.display()),
            output.status,
            stderr.into_owned(),
        ))
    }

    fn is_mounted(&self, target: &Path) -> Result<bool, MountError> {
        let output = Self::run("mount", &["-p"])?;
        if !output.status.success() {
            return Err(MountError::CommandFailed(
                "mount -p".to_string(),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        let target_str = target.to_string_lossy();
        // Verified directly against a real FreeBSD 15.1 VM: `mount -p`'s
        // columns (`device mountpoint fstype options dump pass`) are
        // tab-separated but padded with a variable number of tabs for
        // alignment, not exactly one tab per field, so splitting on a
        // literal single `\t` and indexing shifts fields on any
        // short-enough entry (e.g. every nullfs mount this project creates).
        // Splitting on any run of whitespace instead is safe here since
        // none of these fields ever contain embedded whitespace themselves.
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.split_whitespace().nth(1) == Some(target_str.as_ref())))
    }
}
