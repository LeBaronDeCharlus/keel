use crate::JailError;
use crate::JailRuntime;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Mutex;

pub struct ProcessJailRuntime {
    children: Mutex<Vec<(String, Child)>>,
}

impl ProcessJailRuntime {
    pub fn new() -> Self {
        Self {
            children: Mutex::new(Vec::new()),
        }
    }

    fn run(program: &str, args: &[&str]) -> Result<Output, JailError> {
        Command::new(program)
            .args(args)
            .output()
            .map_err(|e| JailError::Spawn(program.to_string(), e))
    }

    fn run_checked(program: &str, args: &[&str]) -> Result<(), JailError> {
        let output = Self::run(program, args)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(JailError::CommandFailed(
                format!("{program} {}", args.join(" ")),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    fn reap_finished_children(&self) {
        let mut children = self.children.lock().unwrap();
        children.retain_mut(|(_, child)| !matches!(child.try_wait(), Ok(Some(_))));
    }

    // Unlike `zfs list`, `jls` returns exit code 1 both when the jail
    // doesn't exist and on a usage error, so we can't distinguish them
    // by exit code. Since our own arguments here are fixed and known
    // to be valid, a usage error would indicate a code bug, not a
    // runtime condition — treating any failure as "doesn't exist" is
    // an accepted, deliberate trade-off, not an oversight.
    fn jid_of(&self, name: &str) -> Result<Option<String>, JailError> {
        let jls = Self::run("jls", &["-j", name, "jid"])?;
        if !jls.status.success() {
            return Ok(None);
        }
        let jid = String::from_utf8_lossy(&jls.stdout).trim().to_string();
        if jid.is_empty() {
            return Ok(None);
        }
        Ok(Some(jid))
    }
}

impl Default for ProcessJailRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl JailRuntime for ProcessJailRuntime {
    fn create(&self, name: &str, rootfs: &Path, vnet: bool) -> Result<(), JailError> {
        // `mount.devfs` below requires `<rootfs>/dev` to already exist as a
        // directory; a real jail's rootfs (cloned from a base image via
        // ZFS) already has one, but this makes `create` robust regardless
        // of what the rootfs happened to contain beforehand, rather than
        // relying on every caller to have created it.
        std::fs::create_dir_all(rootfs.join("dev"))
            .map_err(|e| JailError::Spawn("mkdir devfs mountpoint".to_string(), e))?;

        let path_arg = format!("path={}", rootfs.display());
        let name_arg = format!("name={name}");
        let mut args: Vec<&str> = vec!["-c", &name_arg, &path_arg];
        if vnet {
            args.push("vnet");
        }
        // Without this, the jail's own `/dev` is an empty directory: no
        // `/dev/null`, `/dev/random`, etc. Every previous real-VM test
        // happened to only ever run statically-linked `/rescue/*` binaries
        // or shell builtins, none of which touch `/dev` from inside the
        // jail, so this went undetected until a real dynamically-linked
        // workload (Milestone 21's Python test backend) needed
        // `/dev/null` for its own dynamic linker and failed outright.
        // `devfs_ruleset=4` is FreeBSD's standard `devfsrules_jail` set
        // (`/etc/defaults/devfs.rules`): null/zero/random/urandom/crypto,
        // pty/tty, fd/stdin/stdout/stderr, fuse, zfs — deliberately not
        // ruleset 0 (unrestricted), which would expose the host's raw
        // disk devices to the jail.
        args.push("mount.devfs");
        args.push("devfs_ruleset=4");
        args.push("persist");
        Self::run_checked("jail", &args)
    }

    fn destroy(&self, name: &str) -> Result<(), JailError> {
        // Must be captured before `jail -r` below: once the jail is gone,
        // `jls` can no longer report its `path`. `jail -r` does not itself
        // unmount `devfs` from `<path>/dev` (confirmed directly against a
        // real jail during Milestone 21 VM verification: the mount was
        // still present in `mount(8)`'s output after removal), so this is
        // undone explicitly below rather than assumed automatic.
        let devfs_path = Self::run("jls", &["-j", name, "path"])
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|p| !p.is_empty())
            .map(|p| Path::new(&p).join("dev"));

        let output = Self::run("jail", &["-r", name])?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // `jail -r` on a jail that doesn't exist prints
            // `jail: "<name>" not found` and exits 1 — the same condition
            // `Reconciler::delete` already tolerates from `FakeJailRuntime`
            // (which returns `NotFound` directly), for the real case of
            // deleting a record that was applied but never got as far as
            // being provisioned. Matching this specific stderr text (the
            // same idiom `remove_resource_limits` below already uses for
            // `rctl`'s "No such process") is what actually makes that
            // tolerance engage against the real jail runtime; without it,
            // this case surfaced as a plain `CommandFailed` instead,
            // reproduced end-to-end during Milestone 8 VM verification.
            if stderr.contains("not found") {
                return Err(JailError::NotFound(name.to_string()));
            }
            return Err(JailError::CommandFailed(
                format!("jail -r {name}"),
                output.status,
                stderr.into_owned(),
            ));
        }

        // Best-effort: an already-unmounted `/dev` (e.g. a jail created
        // before this fix, or destroy called twice) is not an error here,
        // matching every other idempotent-on-absence cleanup step in this
        // project (`remove_resource_limits`, `detach_jail`, ...).
        if let Some(devfs_path) = devfs_path {
            let _ = Self::run("umount", &[devfs_path.to_string_lossy().as_ref()]);
        }

        // `jail -r` kills every process in the jail and blocks until
        // removal completes, but the kernel doesn't fully release a killed
        // child until its parent (us, since `start_command` spawned it via
        // `Command::spawn`) reaps it. Left unreaped, the zombie still holds
        // a reference into the jail's rootfs mount, which then fails a
        // caller's immediately-following `zfs destroy` of that dataset with
        // "device busy" — reproduced end-to-end against a real ZFS-backed
        // jail during Milestone 5 VM verification. A non-blocking
        // `try_wait` right after `jail -r` returns isn't reliable enough
        // under load (observed intermittently in the full workspace test
        // run), so this does a blocking `wait` instead — bounded and safe
        // because `jail -r` already guarantees these specific processes
        // are dead. Only this jail's own children are touched; any other
        // jail's children are left for `start_command`'s ordinary
        // non-blocking sweep, so destroying one jail never blocks on an
        // unrelated jail's still-running process.
        let mine = {
            let mut children = self.children.lock().unwrap();
            let all = std::mem::take(&mut *children);
            let (mine, others): (Vec<_>, Vec<_>) = all
                .into_iter()
                .partition(|(child_name, _)| child_name == name);
            *children = others;
            mine
        };
        for (_, mut child) in mine {
            let _ = child.wait();
        }
        Ok(())
    }

    fn jail_exists(&self, name: &str) -> Result<bool, JailError> {
        Ok(self.jid_of(name)?.is_some())
    }

    fn is_running(&self, name: &str) -> Result<bool, JailError> {
        let jid = match self.jid_of(name)? {
            Some(jid) => jid,
            None => return Ok(false),
        };
        let ps = Self::run("ps", &["-J", &jid, "-o", "state="])?;
        let has_live_process = String::from_utf8_lossy(&ps.stdout).lines().any(|state| {
            let state = state.trim();
            !state.is_empty() && !state.starts_with('Z')
        });
        Ok(has_live_process)
    }

    fn start_command(&self, name: &str, command: &[String]) -> Result<(), JailError> {
        self.reap_finished_children();
        let mut cmd = Command::new("jexec");
        cmd.arg(name);
        cmd.args(command);
        // The jailed process must never inherit this process's own stdio.
        // Under a supervisor like `daemon(8) -S`, keel-agentd's own
        // stdout/stderr are the write end of a pipe daemon(8) reads to
        // relay output to syslog — and relies on reaching EOF to detect
        // that keel-agentd itself has exited and needs restarting. A
        // long-running jailed process that inherited those fds (Rust's
        // `Command` inherits stdio by default) holds that pipe open
        // indefinitely even after keel-agentd dies, so the supervisor never
        // sees EOF and never restarts it. Reproduced directly on the real
        // VM during Milestone 6 verification: daemon(8) -r silently never
        // restarted a killed keel-agentd whenever a jail with a
        // long-running command was active.
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
        let child = cmd
            .spawn()
            .map_err(|e| JailError::Spawn("jexec".to_string(), e))?;
        self.children
            .lock()
            .unwrap()
            .push((name.to_string(), child));
        Ok(())
    }

    fn set_resource_limits(
        &self,
        name: &str,
        pcpu_percent: u32,
        memory_bytes: u64,
    ) -> Result<(), JailError> {
        Self::run_checked(
            "rctl",
            &["-a", &format!("jail:{name}:pcpu:deny={pcpu_percent}")],
        )?;
        Self::run_checked(
            "rctl",
            &["-a", &format!("jail:{name}:vmemoryuse:deny={memory_bytes}")],
        )
    }

    fn remove_resource_limits(&self, name: &str) -> Result<(), JailError> {
        let output = Self::run("rctl", &["-r", &format!("jail:{name}")])?;
        if output.status.success() {
            return Ok(());
        }
        if String::from_utf8_lossy(&output.stderr).contains("No such process") {
            // Nothing to remove — already in the desired state, not an error.
            return Ok(());
        }
        Err(JailError::CommandFailed(
            format!("rctl -r jail:{name}"),
            output.status,
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ))
    }
}
