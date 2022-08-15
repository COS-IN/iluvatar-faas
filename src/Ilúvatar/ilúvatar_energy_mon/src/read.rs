use anyhow::Result;
use std::fs::File;
use std::io::{Seek, SeekFrom, BufRead, BufReader};

use crate::structs::Span;

/// Read the log file from the beginning, returning the new position ended on
pub fn read_log(file: &String) -> Result<u64> {
  Ok(seeking(file, 0)?)
}

/// Read the log file from a certain point, returning the new position ended on
pub fn seeking(file: &String, start_pos: u64) -> Result<u64> {
  let mut f = File::open(file)?;
  f.seek(SeekFrom::Start(start_pos))?;
  let mut buffed = BufReader::new(f);

  let mut buff = String::new();
  while buffed.read_line(&mut buff)? != 0 {
    if buff.contains("span") {
      match serde_json::from_str::<Span>(&buff) {
        Ok(span) => println!("{}::{} -> {}", span.target, span.span.name, span.span.tid),
        Err(e) => {
          println!("len:{} \n '{}'", buff.len(), buff);
          anyhow::bail!(e)
        },
      }
    }
    buff.clear();
  }
  Ok(buffed.stream_position()?)
}
