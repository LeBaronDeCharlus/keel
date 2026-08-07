use crate::ZfsError;
use crate::ZfsManager;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
pub struct FakeZfsManager {
    datasets: Arc<Mutex<HashSet<String>>>,
    snapshots: Arc<Mutex<HashSet<String>>>,
    busy: Arc<Mutex<HashSet<String>>>,
    quotas: Arc<Mutex<HashMap<String, String>>>,
}

impl FakeZfsManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test helper: seed a base dataset as if it already existed on the pool.
    pub fn seed_dataset(&self, dataset: &str) {
        self.datasets.lock().unwrap().insert(dataset.to_string());
    }

    /// Test helper: makes `destroy_dataset` return `ZfsError::Busy` for
    /// this dataset instead of removing it — simulates a volume still
    /// nullfs-mounted by a running jail, since this in-memory fake has no
    /// real mount awareness of its own.
    pub fn mark_busy(&self, dataset: &str) {
        self.busy.lock().unwrap().insert(dataset.to_string());
    }

    /// Test helper: the real-world counterpart to `mark_busy` -- simulates
    /// whatever was holding the dataset busy (e.g. a jail's nullfs mount)
    /// releasing it, so a retried `destroy_dataset` can succeed.
    pub fn unmark_busy(&self, dataset: &str) {
        self.busy.lock().unwrap().remove(dataset);
    }

    /// Test helper: the quota currently recorded for `dataset`, whether it
    /// came from `create_volume`'s create-time quota or a later
    /// `set_quota`. `None` for a dataset that has never had one -- notably
    /// one created by `receive_snapshot`, which (like real ZFS without
    /// `-p`) carries no properties across.
    pub fn quota_of(&self, dataset: &str) -> Option<String> {
        self.quotas.lock().unwrap().get(dataset).cloned()
    }
}

impl ZfsManager for FakeZfsManager {
    fn dataset_exists(&self, dataset: &str) -> Result<bool, ZfsError> {
        Ok(self.datasets.lock().unwrap().contains(dataset))
    }

    fn clone_from_base(&self, base_dataset: &str, target_dataset: &str) -> Result<(), ZfsError> {
        let datasets = self.datasets.lock().unwrap();
        if !datasets.contains(base_dataset) {
            return Err(ZfsError::NotFound(base_dataset.to_string()));
        }
        drop(datasets);
        self.datasets
            .lock()
            .unwrap()
            .insert(target_dataset.to_string());
        Ok(())
    }

    fn create_volume(&self, dataset: &str, quota: &str) -> Result<(), ZfsError> {
        if !self.datasets.lock().unwrap().insert(dataset.to_string()) {
            // Already existed: like the real implementation, this is a no-op
            // and the quota is deliberately *not* re-applied.
            return Ok(());
        }
        self.quotas
            .lock()
            .unwrap()
            .insert(dataset.to_string(), quota.to_string());
        Ok(())
    }

    fn set_quota(&self, dataset: &str, quota: &str) -> Result<(), ZfsError> {
        if !self.datasets.lock().unwrap().contains(dataset) {
            return Err(ZfsError::NotFound(dataset.to_string()));
        }
        self.quotas
            .lock()
            .unwrap()
            .insert(dataset.to_string(), quota.to_string());
        Ok(())
    }

    fn destroy_dataset(&self, dataset: &str) -> Result<(), ZfsError> {
        if self.busy.lock().unwrap().contains(dataset) {
            return Err(ZfsError::Busy(dataset.to_string()));
        }
        if self.datasets.lock().unwrap().remove(dataset) {
            self.quotas.lock().unwrap().remove(dataset);
            Ok(())
        } else {
            Err(ZfsError::NotFound(dataset.to_string()))
        }
    }

    fn snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), ZfsError> {
        if !self.datasets.lock().unwrap().contains(dataset) {
            return Err(ZfsError::NotFound(dataset.to_string()));
        }
        self.snapshots
            .lock()
            .unwrap()
            .insert(format!("{dataset}@{snapshot}"));
        Ok(())
    }

    fn destroy_snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), ZfsError> {
        let key = format!("{dataset}@{snapshot}");
        if self.snapshots.lock().unwrap().remove(&key) {
            Ok(())
        } else {
            Err(ZfsError::NotFound(key))
        }
    }

    fn send_snapshot(
        &self,
        dataset: &str,
        snapshot: &str,
        base: Option<&str>,
        out: &mut dyn Write,
    ) -> Result<(), ZfsError> {
        let key = format!("{dataset}@{snapshot}");
        if !self.snapshots.lock().unwrap().contains(&key) {
            return Err(ZfsError::NotFound(key));
        }
        if let Some(base_snapshot) = base {
            let base_key = format!("{dataset}@{base_snapshot}");
            if !self.snapshots.lock().unwrap().contains(&base_key) {
                return Err(ZfsError::NotFound(base_key));
            }
        }
        let marker = format!(
            "keel-zfs-fake-send:{dataset}@{snapshot}:base={}\n",
            base.unwrap_or("none")
        );
        out.write_all(marker.as_bytes())
            .map_err(|e| ZfsError::Spawn("fake zfs send".to_string(), e))
    }

    fn receive_snapshot(&self, dataset: &str, input: &mut dyn Read) -> Result<(), ZfsError> {
        let mut buf = String::new();
        input
            .read_to_string(&mut buf)
            .map_err(|e| ZfsError::Spawn("fake zfs receive".to_string(), e))?;
        let marker = buf.strip_prefix("keel-zfs-fake-send:");
        let parsed = marker.and_then(|rest| {
            let rest = rest.strip_suffix('\n').unwrap_or(rest);
            let (sent, base_part) = rest.split_once(":base=")?;
            let (_sender_dataset, snapshot) = sent.split_once('@')?;
            let base = if base_part == "none" {
                None
            } else {
                Some(base_part.to_string())
            };
            Some((snapshot.to_string(), base))
        });
        let Some((snapshot, base)) = parsed else {
            return Err(ZfsError::CommandFailed(
                "zfs receive (fake)".to_string(),
                std::process::ExitStatus::from_raw(256),
                "malformed stream".to_string(),
            ));
        };
        if let Some(b) = &base {
            let base_key = format!("{dataset}@{b}");
            if !self.snapshots.lock().unwrap().contains(&base_key) {
                return Err(ZfsError::CommandFailed(
                    "zfs receive (fake)".to_string(),
                    std::process::ExitStatus::from_raw(256),
                    format!("cannot receive incremental stream: base snapshot {base_key} does not exist"),
                ));
            }
        }
        self.datasets.lock().unwrap().insert(dataset.to_string());
        self.snapshots
            .lock()
            .unwrap()
            .insert(format!("{dataset}@{snapshot}"));
        Ok(())
    }

    fn list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError> {
        let prefix = format!("{parent}/");
        let mut children: Vec<String> = self
            .datasets
            .lock()
            .unwrap()
            .iter()
            .filter(|name| name.starts_with(&prefix) && !name[prefix.len()..].contains('/'))
            .cloned()
            .collect();
        children.sort();
        Ok(children)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_exists_is_false_until_seeded() {
        let zfs = FakeZfsManager::new();
        assert!(!zfs.dataset_exists("zroot/keel/base/test").unwrap());
        zfs.seed_dataset("zroot/keel/base/test");
        assert!(zfs.dataset_exists("zroot/keel/base/test").unwrap());
    }

    #[test]
    fn clone_from_base_requires_existing_base() {
        let zfs = FakeZfsManager::new();
        assert!(matches!(
            zfs.clone_from_base("zroot/keel/base/test", "zroot/keel/jails/web-1"),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn clone_from_base_creates_target_dataset() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/base/test");
        zfs.clone_from_base("zroot/keel/base/test", "zroot/keel/jails/web-1")
            .unwrap();
        assert!(zfs.dataset_exists("zroot/keel/jails/web-1").unwrap());
    }

    #[test]
    fn destroy_dataset_removes_it() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/jails/web-1");
        zfs.destroy_dataset("zroot/keel/jails/web-1").unwrap();
        assert!(!zfs.dataset_exists("zroot/keel/jails/web-1").unwrap());
    }

    #[test]
    fn destroy_dataset_on_unknown_dataset_returns_not_found() {
        let zfs = FakeZfsManager::new();
        assert!(matches!(
            zfs.destroy_dataset("zroot/keel/jails/missing"),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn create_volume_creates_the_dataset() {
        let zfs = FakeZfsManager::new();
        zfs.create_volume("zroot/keel/volumes/web-data", "1G")
            .unwrap();
        assert!(zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
    }

    #[test]
    fn create_volume_is_idempotent_on_an_already_existing_dataset() {
        let zfs = FakeZfsManager::new();
        zfs.create_volume("zroot/keel/volumes/web-data", "1G")
            .unwrap();
        zfs.create_volume("zroot/keel/volumes/web-data", "1G")
            .unwrap();
        assert!(zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
    }

    #[test]
    fn create_volume_records_its_quota_and_does_not_re_apply_it_on_a_second_call() {
        let zfs = FakeZfsManager::new();
        zfs.create_volume("zroot/keel/volumes/web-data", "1G")
            .unwrap();
        assert_eq!(
            zfs.quota_of("zroot/keel/volumes/web-data"),
            Some("1G".to_string())
        );
        zfs.create_volume("zroot/keel/volumes/web-data", "5G")
            .unwrap();
        assert_eq!(
            zfs.quota_of("zroot/keel/volumes/web-data"),
            Some("1G".to_string()),
            "create_volume is idempotent and must not re-apply a quota to an existing dataset"
        );
    }

    #[test]
    fn set_quota_applies_a_quota_to_an_existing_dataset() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        assert_eq!(zfs.quota_of("zroot/keel/volumes/web-data"), None);
        zfs.set_quota("zroot/keel/volumes/web-data", "2G").unwrap();
        assert_eq!(
            zfs.quota_of("zroot/keel/volumes/web-data"),
            Some("2G".to_string())
        );
    }

    #[test]
    fn set_quota_on_an_unknown_dataset_returns_not_found() {
        let zfs = FakeZfsManager::new();
        assert!(matches!(
            zfs.set_quota("zroot/keel/volumes/missing", "1G"),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn receive_snapshot_leaves_the_received_dataset_with_no_quota() {
        // Mirrors real `zfs send`/`receive` without `-p`: properties, quota
        // included, do not travel with the stream. This is exactly why
        // restore has to re-apply the declared size itself.
        let zfs = FakeZfsManager::new();
        zfs.create_volume("zroot/keel/volumes/web-data", "1G")
            .unwrap();
        zfs.snapshot("zroot/keel/volumes/web-data", "backup-1")
            .unwrap();
        let mut stream = Vec::new();
        zfs.send_snapshot("zroot/keel/volumes/web-data", "backup-1", None, &mut stream)
            .unwrap();

        let target = FakeZfsManager::new();
        target
            .receive_snapshot("zroot/keel/volumes/web-data", &mut stream.as_slice())
            .unwrap();
        assert_eq!(target.quota_of("zroot/keel/volumes/web-data"), None);
    }

    #[test]
    fn destroy_dataset_on_a_busy_dataset_returns_busy_and_leaves_it_present() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.mark_busy("zroot/keel/volumes/web-data");
        assert!(matches!(
            zfs.destroy_dataset("zroot/keel/volumes/web-data"),
            Err(ZfsError::Busy(_))
        ));
        assert!(zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
    }

    #[test]
    fn unmark_busy_lets_a_previously_busy_dataset_be_destroyed() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.mark_busy("zroot/keel/volumes/web-data");
        assert!(matches!(
            zfs.destroy_dataset("zroot/keel/volumes/web-data"),
            Err(ZfsError::Busy(_))
        ));

        zfs.unmark_busy("zroot/keel/volumes/web-data");
        zfs.destroy_dataset("zroot/keel/volumes/web-data").unwrap();
        assert!(!zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
    }

    #[test]
    fn snapshot_requires_an_existing_dataset() {
        let zfs = FakeZfsManager::new();
        assert!(matches!(
            zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-1"),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn send_snapshot_requires_the_snapshot_to_exist() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        let mut out = Vec::new();
        assert!(matches!(
            zfs.send_snapshot("zroot/keel/volumes/web-data", "keel-repl-1", None, &mut out),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn send_snapshot_full_then_receive_snapshot_creates_the_target_dataset() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();

        let mut stream = Vec::new();
        zfs.send_snapshot(
            "zroot/keel/volumes/web-data",
            "keel-repl-1",
            None,
            &mut stream,
        )
        .unwrap();

        let target = FakeZfsManager::new();
        target
            .receive_snapshot("zroot/keel/volumes/web-0-data", &mut stream.as_slice())
            .unwrap();
        assert!(target
            .dataset_exists("zroot/keel/volumes/web-0-data")
            .unwrap());
    }

    #[test]
    fn send_snapshot_incremental_requires_the_base_snapshot_to_exist() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-2")
            .unwrap();

        let mut out = Vec::new();
        assert!(matches!(
            zfs.send_snapshot(
                "zroot/keel/volumes/web-data",
                "keel-repl-2",
                Some("keel-repl-1"),
                &mut out
            ),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn send_snapshot_incremental_succeeds_once_the_base_exists() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();
        zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-2")
            .unwrap();

        let mut out = Vec::new();
        zfs.send_snapshot(
            "zroot/keel/volumes/web-data",
            "keel-repl-2",
            Some("keel-repl-1"),
            &mut out,
        )
        .unwrap();
        assert!(
            !out.is_empty(),
            "expected the fake to still write a synthetic byte marker for an incremental send"
        );
    }

    #[test]
    fn receive_snapshot_on_a_malformed_stream_fails_without_creating_the_dataset() {
        let zfs = FakeZfsManager::new();
        let mut garbage: &[u8] = b"not a real send stream";
        assert!(matches!(
            zfs.receive_snapshot("zroot/keel/volumes/web-0-data", &mut garbage),
            Err(ZfsError::CommandFailed(_, _, _))
        ));
        assert!(!zfs.dataset_exists("zroot/keel/volumes/web-0-data").unwrap());
    }

    #[test]
    fn receive_snapshot_incremental_rejects_when_base_was_never_received_on_target() {
        let source = FakeZfsManager::new();
        source.seed_dataset("zroot/keel/volumes/web-data");
        source
            .snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();
        source
            .snapshot("zroot/keel/volumes/web-data", "keel-repl-2")
            .unwrap();

        let mut stream = Vec::new();
        source
            .send_snapshot(
                "zroot/keel/volumes/web-data",
                "keel-repl-2",
                Some("keel-repl-1"),
                &mut stream,
            )
            .unwrap();

        // Target never received keel-repl-1, so an incremental receive based on it must fail,
        // mirroring real `zfs receive -i` refusing a stream whose base doesn't match.
        let target = FakeZfsManager::new();
        assert!(matches!(
            target.receive_snapshot("zroot/keel/volumes/web-0-data", &mut stream.as_slice()),
            Err(ZfsError::CommandFailed(_, _, _))
        ));
        assert!(!target
            .dataset_exists("zroot/keel/volumes/web-0-data")
            .unwrap());
    }

    #[test]
    fn receive_snapshot_incremental_succeeds_once_the_base_was_received_first() {
        let source = FakeZfsManager::new();
        source.seed_dataset("zroot/keel/volumes/web-data");
        source
            .snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();
        source
            .snapshot("zroot/keel/volumes/web-data", "keel-repl-2")
            .unwrap();

        let mut full_stream = Vec::new();
        source
            .send_snapshot(
                "zroot/keel/volumes/web-data",
                "keel-repl-1",
                None,
                &mut full_stream,
            )
            .unwrap();
        let mut incremental_stream = Vec::new();
        source
            .send_snapshot(
                "zroot/keel/volumes/web-data",
                "keel-repl-2",
                Some("keel-repl-1"),
                &mut incremental_stream,
            )
            .unwrap();

        let target = FakeZfsManager::new();
        target
            .receive_snapshot("zroot/keel/volumes/web-0-data", &mut full_stream.as_slice())
            .unwrap();
        target
            .receive_snapshot(
                "zroot/keel/volumes/web-0-data",
                &mut incremental_stream.as_slice(),
            )
            .unwrap();
        assert!(target
            .dataset_exists("zroot/keel/volumes/web-0-data")
            .unwrap());
    }

    #[test]
    fn destroy_snapshot_removes_an_existing_snapshot() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();
        zfs.destroy_snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();

        let mut out = Vec::new();
        assert!(matches!(
            zfs.send_snapshot("zroot/keel/volumes/web-data", "keel-repl-1", None, &mut out),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn destroy_snapshot_then_send_using_it_as_a_base_fails() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();
        zfs.snapshot("zroot/keel/volumes/web-data", "keel-repl-2")
            .unwrap();
        zfs.destroy_snapshot("zroot/keel/volumes/web-data", "keel-repl-1")
            .unwrap();

        let mut out = Vec::new();
        assert!(matches!(
            zfs.send_snapshot(
                "zroot/keel/volumes/web-data",
                "keel-repl-2",
                Some("keel-repl-1"),
                &mut out
            ),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn destroy_snapshot_on_an_unknown_snapshot_returns_not_found() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        assert!(matches!(
            zfs.destroy_snapshot("zroot/keel/volumes/web-data", "keel-repl-1"),
            Err(ZfsError::NotFound(_))
        ));
    }

    #[test]
    fn clone_shares_the_same_underlying_state() {
        let zfs = FakeZfsManager::new();
        let clone = zfs.clone();
        clone.seed_dataset("zroot/keel/volumes/shared");
        assert!(
            zfs.dataset_exists("zroot/keel/volumes/shared").unwrap(),
            "expected a clone's mutation to be visible through the original handle"
        );
    }

    #[test]
    fn list_child_datasets_returns_only_immediate_children_sorted() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.seed_dataset("zroot/keel/volumes/db-data");
        zfs.seed_dataset("zroot/keel/jails/web-1");
        assert_eq!(
            zfs.list_child_datasets("zroot/keel/volumes").unwrap(),
            vec![
                "zroot/keel/volumes/db-data".to_string(),
                "zroot/keel/volumes/web-data".to_string()
            ]
        );
    }

    #[test]
    fn list_child_datasets_excludes_grandchildren() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.seed_dataset("zroot/keel/volumes/web-data/nested");
        assert_eq!(
            zfs.list_child_datasets("zroot/keel/volumes").unwrap(),
            vec!["zroot/keel/volumes/web-data".to_string()]
        );
    }

    #[test]
    fn list_child_datasets_on_an_unseeded_parent_is_empty() {
        let zfs = FakeZfsManager::new();
        assert_eq!(
            zfs.list_child_datasets("zroot/keel/volumes").unwrap(),
            Vec::<String>::new()
        );
    }
}
