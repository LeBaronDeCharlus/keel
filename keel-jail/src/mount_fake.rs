use crate::MountError;
use crate::MountManager;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// `Arc`-backed so a clone shares the same underlying mount table, needed by
/// tests that simulate a daemon restart against the same real mount state.
#[derive(Default, Clone)]
pub struct FakeMountManager {
    mounted: Arc<Mutex<HashSet<PathBuf>>>,
}

impl FakeMountManager {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MountManager for FakeMountManager {
    fn ensure_mount_point(&self, _target: &Path) -> Result<(), MountError> {
        Ok(())
    }

    fn mount_nullfs(&self, _source: &Path, target: &Path) -> Result<(), MountError> {
        self.mounted.lock().unwrap().insert(target.to_path_buf());
        Ok(())
    }

    fn unmount(&self, target: &Path) -> Result<(), MountError> {
        if self.mounted.lock().unwrap().remove(target) {
            Ok(())
        } else {
            Err(MountError::NotMounted(target.to_path_buf()))
        }
    }

    fn is_mounted(&self, target: &Path) -> Result<bool, MountError> {
        Ok(self.mounted.lock().unwrap().contains(target))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_mounted_is_false_until_mount_nullfs() {
        let mounts = FakeMountManager::new();
        let target = Path::new("/zroot/keel/jails/web-1/data");
        assert_eq!(mounts.is_mounted(target).unwrap(), false);
        mounts.mount_nullfs(Path::new("/zroot/keel/volumes/web-data"), target).unwrap();
        assert_eq!(mounts.is_mounted(target).unwrap(), true);
    }

    #[test]
    fn unmount_makes_is_mounted_false() {
        let mounts = FakeMountManager::new();
        let target = Path::new("/zroot/keel/jails/web-1/data");
        mounts.mount_nullfs(Path::new("/zroot/keel/volumes/web-data"), target).unwrap();
        mounts.unmount(target).unwrap();
        assert_eq!(mounts.is_mounted(target).unwrap(), false);
    }

    #[test]
    fn unmount_on_a_never_mounted_target_returns_not_mounted() {
        let mounts = FakeMountManager::new();
        let target = Path::new("/zroot/keel/jails/web-1/data");
        assert!(matches!(mounts.unmount(target), Err(MountError::NotMounted(_))));
    }
}
