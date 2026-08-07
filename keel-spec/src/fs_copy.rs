use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;

/// Recursively copies every entry under `src` into `dst` (creating `dst`
/// and any missing parent directories as needed), skipping any entry
/// directly under `src` whose name matches one in `skip_names`. Used by
/// backup to copy a component's `state_dir` into `backups/<id>/...`
/// without also copying that same `state_dir`'s own `backups/`
/// subdirectory into itself, and by restore to copy a backup's saved tree
/// back onto an already-wiped live `state_dir`.
pub fn copy_dir_recursive(src: &Path, dst: &Path, skip_names: &[&str]) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if skip_names.iter().any(|s| name == OsStr::new(s)) {
            continue;
        }
        let dst_path = dst.join(&name);
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &dst_path, skip_names)?;
        } else {
            fs::copy(entry.path(), &dst_path)?;
        }
    }
    Ok(())
}

/// Removes every entry directly under `dir`, except any name in
/// `skip_names`, recursively. Used by restore to clear a live `state_dir`
/// of its current contents (without touching its own `backups/`
/// subdirectory) before copying a backup's saved tree over it, so a
/// record that existed live but not in the backup doesn't survive the
/// restore.
pub fn wipe_dir_contents(dir: &Path, skip_names: &[&str]) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if skip_names.iter().any(|s| name == OsStr::new(s)) {
            continue;
        }
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn fresh_dir(name: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "keel-spec-fs-copy-test-{name}-{}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn copy_dir_recursive_copies_nested_files_and_subdirs() {
        let src = fresh_dir("copy_src");
        let dst = fresh_dir("copy_dst");
        fs::create_dir_all(src.join("sub")).unwrap();
        fs::write(src.join("top.yaml"), "top").unwrap();
        fs::write(src.join("sub").join("nested.yaml"), "nested").unwrap();

        copy_dir_recursive(&src, &dst, &[]).unwrap();

        assert_eq!(fs::read_to_string(dst.join("top.yaml")).unwrap(), "top");
        assert_eq!(
            fs::read_to_string(dst.join("sub").join("nested.yaml")).unwrap(),
            "nested"
        );
    }

    #[test]
    fn copy_dir_recursive_skips_named_top_level_entries() {
        let src = fresh_dir("copy_skip_src");
        let dst = fresh_dir("copy_skip_dst");
        fs::create_dir_all(src.join("backups")).unwrap();
        fs::write(src.join("backups").join("old.yaml"), "old").unwrap();
        fs::write(src.join("placements.yaml"), "keep").unwrap();

        copy_dir_recursive(&src, &dst, &["backups"]).unwrap();

        assert!(!dst.join("backups").exists());
        assert_eq!(
            fs::read_to_string(dst.join("placements.yaml")).unwrap(),
            "keep"
        );
    }

    #[test]
    fn wipe_dir_contents_removes_files_and_subdirs_but_skips_named_entries() {
        let dir = fresh_dir("wipe");
        fs::create_dir_all(dir.join("backups")).unwrap();
        fs::write(dir.join("backups").join("keep.yaml"), "keep").unwrap();
        fs::create_dir_all(dir.join("replica-targets")).unwrap();
        fs::write(dir.join("replica-targets").join("r.yaml"), "r").unwrap();
        fs::write(dir.join("placements.yaml"), "gone").unwrap();

        wipe_dir_contents(&dir, &["backups"]).unwrap();

        assert!(dir.join("backups").join("keep.yaml").exists());
        assert!(!dir.join("replica-targets").exists());
        assert!(!dir.join("placements.yaml").exists());
    }
}
