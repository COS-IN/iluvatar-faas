use anyhow::Result;
use log::warn;

use crate::transaction::TransactionId;

pub const TEMP_DIR: &str = "/tmp/ilúvatar_worker";

/// Return an absolute path to a file in the temp dir
/// Takes a tail file name an extension
pub fn temp_file_pth(with_tail: &str, with_extension: &str) -> String {
  format!("{}/{}.{}", TEMP_DIR, with_tail, with_extension)
}

pub fn temp_file(with_tail: &str, with_extension: &str) -> std::io::Result<String> {
  let pth = temp_file_pth(with_tail, with_extension);
  touch(&pth)?;
  Ok(pth)
}

// A simple implementation of `% touch path` (ignores existing files)
fn touch(path: &String) -> std::io::Result<()> {
  match std::fs::OpenOptions::new().create(true).write(true).open(path) {
      Ok(_) => Ok(()),
      Err(e) => Err(e),
  }
}

/// Tries to remove the specified directory
/// Swallows any failure
pub fn try_remove_pth(pth: &String, tid: &TransactionId) {
  match std::fs::remove_dir(pth) {
    Ok(_) => {},
    Err(_) => warn!("[{}] Unable to remove directory {}", tid, pth),
  };
}

/// Make sure the temp dir to use exists
pub fn ensure_temp_dir() -> Result<()> {
  std::fs::create_dir_all(TEMP_DIR)?;
  Ok(())
}