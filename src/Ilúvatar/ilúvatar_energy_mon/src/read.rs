use anyhow::Result;
use std::collections::{HashSet, HashMap};
use std::fs::File;
use std::io::{Seek, SeekFrom, BufRead, BufReader};

use crate::structs::Span;

const REGISTER_API_ID: &str = "iluvatar_worker_library::worker_api::ilúvatar_worker::register";
const INVOKE_API_ID: &str = "iluvatar_worker_library::worker_api::ilúvatar_worker::invoke";
const INVOKE_ID: &str = "iluvatar_worker_library::services::containers::containerd::containerdstructs::ContainerdContainer::invoke";

pub struct LogMonitor {
  stream_pos: u64,
  buffered_reader: BufReader<File>,
  invocation_spans: HashMap<String, Span>,
  invocation_durations: HashMap<String, i128>,
  worker_spans: HashMap<String, Span>,
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
      invocation_spans: HashMap::new(),
      invocation_durations: HashMap::new(),
      total_spans: 0,
      functions: HashSet::new(),
      worker_spans: HashMap::new(),
    })
  }

  /// Read the katest changes in the log file
  pub fn read_log(&mut self) -> Result<(HashMap<String, i128>, i128)> {
    let mut overhead_ns = 0;
    let mut timing_data = HashMap::new();
    let mut buff = String::new();
    while self.buffered_reader.read_line(&mut buff)? != 0 {
      if buff.contains("span") {
        match serde_json::from_str::<Span>(&buff) {
          Ok(span) => {
            match span.fields.get("message").unwrap() {
              crate::structs::Field::String(s) => match s.as_str() {
                "enter" => (),
                "exit" => (),
                "close" => {
                  match span.name.as_str() {
                    INVOKE_ID => {self.remove_invoke_transaction(&span.uuid, &span);},
                    REGISTER_API_ID => {
                      overhead_ns += self.remove_worker_transaction(&span.uuid, &span, &mut timing_data);
                    },
                    INVOKE_API_ID => {
                      overhead_ns += self.remove_worker_transaction(&span.uuid, &span, &mut timing_data);
                    },
                    // TODO: account for background work done here
                    _ => (),
                  };
                },
                "new" =>  {
                  self.functions.insert(span.name.clone());
                  self.total_spans += 1;
                  match span.name.as_str() {
                    INVOKE_ID => {self.invocation_spans.insert(span.uuid.clone(), span);},
                    REGISTER_API_ID => {self.worker_spans.insert(span.uuid.clone(), span);},
                    INVOKE_API_ID => {self.worker_spans.insert(span.uuid.clone(), span);},
                    // TODO: account for background work done here
                    _ => (),
                  }
                },
                _ => (),
              },
              crate::structs::Field::Number(_) => todo!(),
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
    println!("tids: {}; tot: {}", self.invocation_spans.len(), self.total_spans);
    self.stream_pos = self.buffered_reader.stream_position()?;
    Ok( (timing_data,overhead_ns) )
  }

  fn remove_invoke_transaction(&mut self, id: &String, span: &Span) {
    let found_stamp = self.invocation_spans.remove(id);
    match found_stamp {
      Some(s) => {
        let time_ns = span.timestamp.unix_timestamp_nanos() - s.timestamp.unix_timestamp_nanos();
        self.invocation_durations.insert(span.span.tid.clone(), time_ns);
      },
      None => println!("Tried to remove {} that wasn't found", id),
    }
  }
  fn remove_worker_transaction(&mut self, id: &String, span: &Span, timing_data: &mut HashMap<String, i128>) -> i128 {
    let found_stamp = self.worker_spans.remove(id);
    match found_stamp {
      Some(s) => {
        let time_ns = span.timestamp.unix_timestamp_nanos() - s.timestamp.unix_timestamp_nanos();
        match self.invocation_durations.get(&span.span.tid) {
          Some(invoke_ns) => {
            let overhead = time_ns - invoke_ns;
            timing_data.insert(span.span.tid.clone(), *invoke_ns);
            overhead
          },
          None => time_ns,
        }
      },
      None => {
        println!("Tried to remove {} that wasn't found", id);
        0
      },
    }
  }
}
