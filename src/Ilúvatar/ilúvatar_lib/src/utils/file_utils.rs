use std::path::PathBuf;

use anyhow::Result;
use tracing::{warn, error};

use crate::transaction::TransactionId;

pub const TEMP_DIR: &str = "/tmp/ilúvatar";

/// Return an absolute path to a file in the temp dir
/// Takes a tail file name an extension
pub fn temp_file_pth(with_tail: &str, with_extension: &str) -> String {
  format!("{}/{}.{}", TEMP_DIR, with_tail, with_extension)
}

/// Create a temp file and return the path to it
pub fn temp_file(with_tail: &str, with_extension: &str) -> std::io::Result<String> {
  let pth = temp_file_pth(with_tail, with_extension);
  touch(&pth)?;
  Ok(pth)
}

/// A simple implementation of `% touch path` (ignores existing files)
pub fn touch(path: &String) -> std::io::Result<()> {
  match std::fs::OpenOptions::new().create(true).write(true).open(path) {
      Ok(_) => Ok(()),
      Err(e) => Err(e),
  }
}

/// Tries to remove the specified directory
/// Swallows any failure
pub fn try_remove_pth(pth: &String, tid: &TransactionId) {
  let path = std::path::Path::new(&pth);
  if path.is_file() {
    match std::fs::remove_file(pth) {
      Ok(_) => {},
      Err(_) => warn!("[{}] Unable to remove file {}", tid, pth),
    };  
  } else if path.is_dir() {
    match std::fs::remove_dir(pth) {
      Ok(_) => {},
      Err(_) => warn!("[{}] Unable to remove directory {}", tid, pth),
    };
  } else {
    error!("[{}] Unknown path type to delete {}", tid, pth)
  }
}

/// Make sure the temp dir to use exists
pub fn ensure_dir(dir: &PathBuf) -> Result<()> {
  match std::fs::create_dir_all(dir) {
    Ok(_) => Ok(()),
    Err(e) => anyhow::bail!("Failed to create temp dir: {}", e),
  }
}

/// Make sure the temp dir to use exists
pub fn ensure_temp_dir() -> Result<()> {
  let bf = PathBuf::new();
  let bf = bf.join(TEMP_DIR);
  ensure_dir(&bf)
}
