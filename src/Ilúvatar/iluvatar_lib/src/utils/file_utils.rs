use anyhow::Result;

pub const TEMP_DIR: &str = "/tmp/ilúvatar_worker";

pub fn temp_file(with_tail: &str, with_extension: &str) -> Result<String> {
  let ret = format!("{}/{}.{}", TEMP_DIR, with_tail, with_extension);
  touch(&ret)?;
  Ok(ret)
}

// A simple implementation of `% touch path` (ignores existing files)
fn touch(path: &String) -> std::io::Result<()> {
  match std::fs::OpenOptions::new().create(true).write(true).open(path) {
      Ok(_) => Ok(()),
      Err(e) => Err(e),
  }
}

pub fn ensure_temp_dir() -> Result<()> {
  std::fs::create_dir_all(TEMP_DIR)?;
  Ok(())
}