use crate::JailError;
use crate::JailRuntime;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct FakeJail {
    #[allow(dead_code)]
    rootfs: PathBuf,
    #[allow(dead_code)]
    vnet: bool,
    running: bool,
    pcpu_percent: Option<u32>,
    memory_bytes: Option<u64>,
    fail_start: bool,
}

/// `Arc`-backed (not a bare `Mutex`) so a clone shares the same underlying
/// jails rather than starting a fresh, empty runtime -- needed by tests that
/// simulate a daemon restart, where the same real jails must still be there
/// afterward.
#[derive(Default, Clone)]
pub struct FakeJailRuntime {
    jails: Arc<Mutex<HashMap<String, FakeJail>>>,
}

impl FakeJailRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test helper: simulate the jailed command exiting on its own,
    /// without destroying the jail — the same observable state a real
    /// crashed process leaves behind (jail still exists, is_running goes
    /// false).
    pub fn mark_exited(&self, name: &str) {
        if let Some(jail) = self.jails.lock().unwrap().get_mut(name) {
            jail.running = false;
        }
    }

    /// Test helper: forces `start_command` for `name` to fail with
    /// `JailError::NotFound` until called again with `fail: false`.
    /// Exercises restart-failure handling that this in-memory fake can't
    /// otherwise produce (e.g. a transient `jexec` failure against a
    /// real, already-existing jail).
    pub fn fail_start_command(&self, name: &str, fail: bool) {
        if let Some(jail) = self.jails.lock().unwrap().get_mut(name) {
            jail.fail_start = fail;
        }
    }
}

impl JailRuntime for FakeJailRuntime {
    fn create(&self, name: &str, rootfs: &Path, vnet: bool) -> Result<(), JailError> {
        self.jails.lock().unwrap().insert(
            name.to_string(),
            FakeJail {
                rootfs: rootfs.to_path_buf(),
                vnet,
                running: false,
                pcpu_percent: None,
                memory_bytes: None,
                fail_start: false,
            },
        );
        Ok(())
    }

    fn jail_exists(&self, name: &str) -> Result<bool, JailError> {
        Ok(self.jails.lock().unwrap().contains_key(name))
    }

    fn start_command(&self, name: &str, _command: &[String]) -> Result<(), JailError> {
        let mut jails = self.jails.lock().unwrap();
        let jail = jails
            .get_mut(name)
            .ok_or_else(|| JailError::NotFound(name.to_string()))?;
        if jail.fail_start {
            return Err(JailError::NotFound(format!(
                "{name} (start_command failure injected for testing)"
            )));
        }
        jail.running = true;
        Ok(())
    }

    fn destroy(&self, name: &str) -> Result<(), JailError> {
        self.jails
            .lock()
            .unwrap()
            .remove(name)
            .ok_or_else(|| JailError::NotFound(name.to_string()))?;
        Ok(())
    }

    fn is_running(&self, name: &str) -> Result<bool, JailError> {
        Ok(self
            .jails
            .lock()
            .unwrap()
            .get(name)
            .map(|j| j.running)
            .unwrap_or(false))
    }

    fn set_resource_limits(
        &self,
        name: &str,
        pcpu_percent: u32,
        memory_bytes: u64,
    ) -> Result<(), JailError> {
        let mut jails = self.jails.lock().unwrap();
        let jail = jails
            .get_mut(name)
            .ok_or_else(|| JailError::NotFound(name.to_string()))?;
        jail.pcpu_percent = Some(pcpu_percent);
        jail.memory_bytes = Some(memory_bytes);
        Ok(())
    }

    fn remove_resource_limits(&self, name: &str) -> Result<(), JailError> {
        let mut jails = self.jails.lock().unwrap();
        let jail = jails
            .get_mut(name)
            .ok_or_else(|| JailError::NotFound(name.to_string()))?;
        jail.pcpu_percent = None;
        jail.memory_bytes = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_is_running_is_false_until_start_command() {
        let runtime = FakeJailRuntime::new();
        runtime
            .create("test-1", Path::new("/tmp/rootfs"), false)
            .unwrap();
        assert!(!runtime.is_running("test-1").unwrap());
    }

    #[test]
    fn start_command_makes_is_running_true() {
        let runtime = FakeJailRuntime::new();
        runtime
            .create("test-1", Path::new("/tmp/rootfs"), false)
            .unwrap();
        runtime
            .start_command("test-1", &["/bin/sh".to_string()])
            .unwrap();
        assert!(runtime.is_running("test-1").unwrap());
    }

    #[test]
    fn destroy_removes_the_jail() {
        let runtime = FakeJailRuntime::new();
        runtime
            .create("test-1", Path::new("/tmp/rootfs"), false)
            .unwrap();
        runtime.destroy("test-1").unwrap();
        assert!(!runtime.is_running("test-1").unwrap());
    }

    #[test]
    fn operations_on_unknown_jail_return_not_found() {
        let runtime = FakeJailRuntime::new();
        assert!(matches!(
            runtime.start_command("missing", &[]),
            Err(JailError::NotFound(_))
        ));
        assert!(matches!(
            runtime.destroy("missing"),
            Err(JailError::NotFound(_))
        ));
        assert!(matches!(
            runtime.set_resource_limits("missing", 100, 1024),
            Err(JailError::NotFound(_))
        ));
        assert!(matches!(
            runtime.remove_resource_limits("missing"),
            Err(JailError::NotFound(_))
        ));
    }

    #[test]
    fn set_and_remove_resource_limits_do_not_error_on_known_jail() {
        let runtime = FakeJailRuntime::new();
        runtime
            .create("test-1", Path::new("/tmp/rootfs"), false)
            .unwrap();
        runtime
            .set_resource_limits("test-1", 200, 512 * 1024 * 1024)
            .unwrap();
        runtime.remove_resource_limits("test-1").unwrap();
    }

    #[test]
    fn mark_exited_makes_is_running_false_without_destroying() {
        let runtime = FakeJailRuntime::new();
        runtime
            .create("test-1", Path::new("/tmp/rootfs"), false)
            .unwrap();
        runtime
            .start_command("test-1", &["/bin/sh".to_string()])
            .unwrap();
        assert!(runtime.is_running("test-1").unwrap());

        runtime.mark_exited("test-1");
        assert!(!runtime.is_running("test-1").unwrap());

        // The jail itself still exists — start_command should work again
        // (simulating a restart), not error as NotFound.
        runtime
            .start_command("test-1", &["/bin/sh".to_string()])
            .unwrap();
        assert!(runtime.is_running("test-1").unwrap());
    }

    #[test]
    fn jail_exists_is_false_before_create_and_true_after() {
        let runtime = FakeJailRuntime::new();
        assert!(!runtime.jail_exists("test-1").unwrap());
        runtime
            .create("test-1", Path::new("/tmp/rootfs"), false)
            .unwrap();
        assert!(runtime.jail_exists("test-1").unwrap());
    }

    #[test]
    fn jail_exists_is_false_after_destroy() {
        let runtime = FakeJailRuntime::new();
        runtime
            .create("test-1", Path::new("/tmp/rootfs"), false)
            .unwrap();
        runtime.destroy("test-1").unwrap();
        assert!(!runtime.jail_exists("test-1").unwrap());
    }
}
