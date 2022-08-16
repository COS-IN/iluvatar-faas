use anyhow::Result;
use time::OffsetDateTime;
use std::collections::{HashSet, HashMap};
use std::fs::File;
use std::io::{Seek, SeekFrom, BufRead, BufReader};

use crate::structs::Span;

pub struct LogMonitor {
  stream_pos: u64,
  buffered_reader: BufReader<File>,
  outstanding_spans: HashMap<String, OffsetDateTime>,
  total_spans: u64,
  pub functions: HashSet<String>,
}
impl LogMonitor {
  pub fn new(file_pth: &String) -> Result<Self> {
    let mut f = File::open(file_pth)?;
    f.seek(SeekFrom::Start(0))?;
    Ok(LogMonitor {
      stream_pos: 0,
      buffered_reader: BufReader::new(f),
      outstanding_spans: HashMap::new(),
      total_spans: 0,
      functions: HashSet::new(),
    })
  }

  /// Read the katest changes in the log file
  pub fn read_log(&mut self) -> Result<()> {
    let mut buff = String::new();
    while self.buffered_reader.read_line(&mut buff)? != 0 {
      if buff.contains("span") {
        match serde_json::from_str::<Span>(&buff) {
          Ok(span) => {
            match span.fields.message.as_str() {
              "new" => (),
              "exit" => {
                if span.name == "iluvatar_worker_library::services::containers::containerd::containerdstructs::ContainerdContainer::invoke" {
                  self.remove_transaction(&span.uuid, span.timestamp)
                }
              },
              "close" => (),
              "enter" =>  {
                if span.name == "iluvatar_worker_library::services::containers::containerd::containerdstructs::ContainerdContainer::invoke" {
                  self.add_transaction(span.uuid.clone(), span.timestamp);
                }
                self.functions.insert(span.name);
              },
              _ => (),
            }
          },
          Err(e) => {
            println!("len:{}; Span was missing something! \n '{}'", buff.len(), buff);
            anyhow::bail!(e)
          },
        }
      }
      buff.clear();
    }
    println!("tids: {}; tot: {}", self.outstanding_spans.len(), self.total_spans);
    self.stream_pos = self.buffered_reader.stream_position()?;
    Ok(())
  }

  fn remove_transaction(&mut self, id: &String, stamp: OffsetDateTime) {
    let found_stamp = self.outstanding_spans.remove(id);
    match found_stamp {
      Some(s) => println!("{} {}", id, stamp.unix_timestamp_nanos() - s.unix_timestamp_nanos()),
      None => println!("Tried to remove {} that wasn't found", id),
    }
  }
  fn add_transaction(&mut self, id: String, stamp: OffsetDateTime) {
    self.outstanding_spans.insert(id, stamp);
    self.total_spans += 1;
  }
}
