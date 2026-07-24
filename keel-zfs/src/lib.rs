pub mod cli;
pub mod error;
pub mod fake;

pub use cli::CliZfsManager;
pub use error::ZfsError;
pub use fake::FakeZfsManager;

use std::io::{Read, Write};

pub trait ZfsManager {
    fn dataset_exists(&self, dataset: &str) -> Result<bool, ZfsError>;

    /// The `target_dataset`'s parent must already exist — this method does
    /// not create parent datasets (no `-p`).
    fn clone_from_base(&self, base_dataset: &str, target_dataset: &str) -> Result<(), ZfsError>;

    /// A plain, independent dataset with a hard quota and no base image —
    /// distinct from `clone_from_base`, which always clones from a shared
    /// base snapshot. Idempotent: a dataset that already exists is left
    /// untouched (its quota is not re-applied). Like `clone_from_base`,
    /// this does not create `dataset`'s parent (no `-p`).
    fn create_volume(&self, dataset: &str, quota: &str) -> Result<(), ZfsError>;

    fn destroy_dataset(&self, dataset: &str) -> Result<(), ZfsError>;

    fn snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), ZfsError>;

    /// Destroys `dataset@snapshot`. Used to prune the previous incremental
    /// base once a new one has been confirmed, keeping exactly one snapshot
    /// per replicated volume at steady state.
    fn destroy_snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), ZfsError>;

    /// Streams a `zfs send` (full if `base` is `None`, incremental `-i <base>`
    /// otherwise) of `dataset@snapshot` into `out`.
    fn send_snapshot(&self, dataset: &str, snapshot: &str, base: Option<&str>, out: &mut dyn Write) -> Result<(), ZfsError>;

    /// Streams `input` into `zfs receive <dataset>`, creating or advancing
    /// `dataset` from the received stream.
    fn receive_snapshot(&self, dataset: &str, input: &mut dyn Read) -> Result<(), ZfsError>;

    /// Immediate children of `parent` only (not the whole subtree), sorted
    /// for deterministic output. `Ok(vec![])` if `parent` itself doesn't
    /// exist yet, mirroring `dataset_exists`'s tolerant handling of ZFS's
    /// "no such dataset" exit code rather than treating it as an error.
    fn list_child_datasets(&self, parent: &str) -> Result<Vec<String>, ZfsError>;
}
