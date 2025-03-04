use crate::transaction::TransactionId;
use anyhow::Result;
use std::path::{Path, PathBuf};
use tracing::{error, warn};

pub const TEMP_DIR: &str = "/tmp/iluvatar";

/// Return an absolute path to a file in the temp dir
/// Takes a tail file name an extension
pub fn temp_file_pth(with_tail: &str, with_extension: &str) -> String {
    format!("{}/{}.{}", TEMP_DIR, with_tail, with_extension)
}

pub fn container_path(container_id: &str) -> PathBuf {
    PathBuf::from(TEMP_DIR).join(container_id)
}
pub fn make_paths(pth: &Path, tid: &TransactionId) -> Result<()> {
    match std::fs::create_dir_all(pth) {
        Ok(_) => Ok(()),
        Err(e) => crate::bail_error!(tid=tid, error=%e, "Failed to make paths"),
    }
}

/// Create a temp file and return the path to it
pub fn temp_file(with_tail: &str, with_extension: &str) -> std::io::Result<String> {
    let pth = temp_file_pth(with_tail, with_extension);
    touch(&pth)?;
    Ok(pth)
}

/// A simple implementation of `% touch path` (ignores existing files)
pub fn touch<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    match std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Tries to remove the specified directory
/// Swallows any failure
pub fn try_remove_pth<P: AsRef<Path>>(path: P, tid: &TransactionId) {
    let pth: &Path = path.as_ref();
    if pth.is_file() {
        match std::fs::remove_file(pth) {
            Ok(_) => {},
            Err(_) => warn!(tid=tid, path=%pth.display(), "Unable to remove file"),
        };
    } else if pth.is_dir() {
        match std::fs::remove_dir(pth) {
            Ok(_) => {},
            Err(_) => warn!(tid=tid, path=%pth.display(), "Unable to remove directory"),
        };
    } else {
        error!(tid=tid, path=%pth.display(), "Unknown path type to delete")
    }
}

/// Make sure the temp dir to use exists
pub fn ensure_dir<P: AsRef<Path>>(dir: P) -> Result<()> {
    match std::fs::create_dir_all(&dir) {
        Ok(_) => Ok(()),
        Err(e) => anyhow::bail!("Failed to create dir '{:?}' because '{}'", dir.as_ref().to_str(), e),
    }
}

/// Make sure the temp dir to use exists
pub fn ensure_temp_dir() -> Result<()> {
    let bf = PathBuf::new();
    let bf = bf.join(TEMP_DIR);
    ensure_dir(&bf)
}
