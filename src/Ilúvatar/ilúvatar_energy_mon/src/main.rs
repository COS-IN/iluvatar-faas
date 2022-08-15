use iluvatar_library::utils::config::get_val;

use crate::read::read_log;

pub mod config;
pub mod read;
pub mod structs;

fn main() -> anyhow::Result<()> {
  let args = config::parse();
  let log_file: String = get_val("file", &args)?;

  let read_len = read_log(&log_file)?;
  println!("{}: {}", log_file, read_len);

  Ok(())
}
