use crate::ZfsError;
use crate::ZfsManager;
use std::io::{Read, Write};
use std::process::{Command, Output, Stdio};

#[derive(Clone)]
pub struct CliZfsManager;

impl CliZfsManager {
    pub fn new() -> Self {
        Self
    }

    fn run(args: &[&str]) -> Result<Output, ZfsError> {
        Command::new("zfs")
            .args(args)
            .output()
            .map_err(|e| ZfsError::Spawn("zfs".to_string(), e))
    }

    fn run_checked(args: &[&str]) -> Result<(), ZfsError> {
        let output = Self::run(args)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(ZfsError::CommandFailed(
                format!("zfs {}", args.join(" ")),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }
}

impl Default for CliZfsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ZfsManager for CliZfsManager {
    fn dataset_exists(&self, dataset: &str) -> Result<bool, ZfsError> {
        let output = Self::run(&["list", "-H", "-o", "name", dataset])?;
        if output.status.success() {
            return Ok(true);
        }
        if output.status.code() == Some(1) {
            return Ok(false);
        }
        Err(ZfsError::CommandFailed(
            format!("zfs list -H -o name {dataset}"),
            output.status,
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ))
    }

    fn create_volume(&self, dataset: &str, quota: &str) -> Result<(), ZfsError> {
        if self.dataset_exists(dataset)? {
            return Ok(());
        }
        Self::run_checked(&["create", "-o", &format!("quota={quota}"), dataset])
    }

    fn destroy_dataset(&self, dataset: &str) -> Result<(), ZfsError> {
        // Immediately after a jail using this dataset as its rootfs is torn
        // down (`jail -r`), the kernel can take a brief moment to release
        // the mount's last references even though `jail -r` and the
        // process's own reaping have both already completed — `zfs
        // destroy` fails with "dataset is busy" in that narrow window.
        // Reproduced directly against the real VM during Milestone 5
        // verification (the busy state reliably clears within well under
        // a second). Retry briefly rather than failing a caller (like
        // `Reconciler::delete`) that chains this right after destroying
        // the owning jail.
        let mut last_err = None;
        let mut last_was_busy = false;
        for _ in 0..10 {
            match Self::run_checked(&["destroy", dataset]) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // `zfs destroy` on a dataset that doesn't exist prints
                    // `cannot open '<dataset>': dataset does not exist` and
                    // exits 1 (verified directly on the real VM) — the same
                    // condition `Reconciler::delete` already tolerates from
                    // `FakeZfsManager` (which returns `NotFound` directly),
                    // for the real case of deleting a record whose
                    // provisioning failed before this dataset was ever
                    // cloned. `keel-jail::ProcessJailRuntime::destroy` had
                    // the identical gap for `jail -r`, fixed in Milestone 8.
                    if matches!(&e, ZfsError::CommandFailed(_, _, stderr) if stderr.contains("dataset does not exist"))
                    {
                        return Err(ZfsError::NotFound(dataset.to_string()));
                    }
                    let is_busy =
                        matches!(&e, ZfsError::CommandFailed(_, _, stderr) if stderr.contains("dataset is busy"));
                    last_was_busy = is_busy;
                    last_err = Some(e);
                    if !is_busy {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
        if last_was_busy {
            return Err(ZfsError::Busy(dataset.to_string()));
        }
        Err(last_err.unwrap())
    }

    fn snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), ZfsError> {
        Self::run_checked(&["snapshot", &format!("{dataset}@{snapshot}")])
    }

    fn destroy_snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), ZfsError> {
        Self::run_checked(&["destroy", &format!("{dataset}@{snapshot}")])
    }

    fn send_snapshot(&self, dataset: &str, snapshot: &str, base: Option<&str>, out: &mut dyn Write) -> Result<(), ZfsError> {
        let target = format!("{dataset}@{snapshot}");
        let base_arg = base.map(|b| format!("{dataset}@{b}"));
        let mut args: Vec<&str> = vec!["send"];
        if let Some(b) = &base_arg {
            args.push("-i");
            args.push(b);
        }
        args.push(&target);

        let mut child = Command::new("zfs")
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ZfsError::Spawn("zfs".to_string(), e))?;
        let mut stdout = child.stdout.take().expect("stdout was piped");
        let copy_result = std::io::copy(&mut stdout, out);
        drop(stdout);
        let status = child.wait().map_err(|e| ZfsError::Spawn("zfs".to_string(), e))?;
        let mut stderr = String::new();
        if let Some(mut s) = child.stderr.take() {
            let _ = s.read_to_string(&mut stderr);
        }
        if !status.success() {
            return Err(ZfsError::CommandFailed(format!("zfs {}", args.join(" ")), status, stderr));
        }
        copy_result.map_err(|e| ZfsError::Spawn("zfs send".to_string(), e))?;
        Ok(())
    }

    fn receive_snapshot(&self, dataset: &str, input: &mut dyn Read) -> Result<(), ZfsError> {
        let mut child = Command::new("zfs")
            .args(["receive", dataset])
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ZfsError::Spawn("zfs".to_string(), e))?;
        let mut stdin = child.stdin.take().expect("stdin was piped");
        let copy_result = std::io::copy(input, &mut stdin);
        drop(stdin);
        let status = child.wait().map_err(|e| ZfsError::Spawn("zfs".to_string(), e))?;
        let mut stderr = String::new();
        if let Some(mut s) = child.stderr.take() {
            let _ = s.read_to_string(&mut stderr);
        }
        if !status.success() {
            return Err(ZfsError::CommandFailed(format!("zfs receive {dataset}"), status, stderr));
        }
        copy_result.map_err(|e| ZfsError::Spawn("zfs receive".to_string(), e))?;
        Ok(())
    }

    fn clone_from_base(&self, base_dataset: &str, target_dataset: &str) -> Result<(), ZfsError> {
        let snapshot = format!("{base_dataset}@keel");
        if !self.dataset_exists(&snapshot)? {
            if let Err(e) = Self::run_checked(&["snapshot", &snapshot]) {
                // Lost a race with a concurrent caller cloning the same base:
                // if the snapshot exists now anyway, proceed; otherwise this
                // was a real failure (e.g. the base dataset doesn't exist).
                if !self.dataset_exists(&snapshot)? {
                    return Err(e);
                }
            }
        }
        Self::run_checked(&["clone", &snapshot, target_dataset])
    }

    fn list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError> {
        let output = Self::run(&["list", "-H", "-o", "name", "-r", parent])?;
        if !output.status.success() {
            if output.status.code() == Some(1) {
                return Ok(Vec::new());
            }
            return Err(ZfsError::CommandFailed(
                format!("zfs list -H -o name -r {parent}"),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let prefix = format!("{parent}/");
        let mut children: Vec<String> = stdout
            .lines()
            .map(|line| line.trim().to_string())
            .filter(|name| name.starts_with(&prefix) && !name[prefix.len()..].contains('/'))
            .collect();
        children.sort();
        Ok(children)
    }
}
