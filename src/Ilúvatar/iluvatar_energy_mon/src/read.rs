use anyhow::Result;
use iluvatar_library::energy::rapl::RAPL;
use iluvatar_library::transaction::TransactionId;
// use iluvatar_library::utils::config::get_val;
use crate::structs::Span;
use clap::ArgMatches;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use tracing::{error, info};

const REGISTER_API_ID: &str = "iluvatar_worker_library::worker_api::iluvatar_worker::register";
const INVOKE_API_ID: &str = "iluvatar_worker_library::worker_api::iluvatar_worker::invoke";
const INVOKE_ID: &str =
    "iluvatar_worker_library::services::containers::containerd::containerdstructs::ContainerdContainer::invoke";

pub fn analyze_logs(_matches: &ArgMatches, _submatches: &ArgMatches) -> Result<()> {
    // TODO: re-implement this
    let tid: &TransactionId = &iluvatar_library::transaction::ENERGY_MONITOR_TID;
    // let log_file: String = get_val("logfile", submatches)?;
    let log_file = "".to_string();
    let mut monitor = LogMonitor::new(&log_file)?;
    let rapl = RAPL::new()?;
    let mut curr_rapl = rapl.record()?;

    let (function_data, overhead) = monitor.read_log()?;
    if !function_data.is_empty() {
        let tot_time = (function_data.values().sum::<u128>() + overhead) as f64;
        info!(
            "Overhead: {}; Total time: {}; Overhead share: {}",
            overhead,
            tot_time,
            overhead as f64 / tot_time
        );
        let mut shares = HashMap::new();
        for (k, v) in function_data.iter() {
            let share = *v as f64 / tot_time;
            shares.insert(k.clone(), share);
        }
        info!("{:?}", shares);

        let new_rapl = rapl.record()?;
        let (time, uj) = rapl.difference(&new_rapl, &curr_rapl, tid)?;
        info!("{} seconds; {} joules;", time / 1000000, uj / 1000000);
        curr_rapl = new_rapl;
    }
    let new_rapl = rapl.record()?;
    let (time, uj) = rapl.difference(&new_rapl, &curr_rapl, tid)?;
    info!("{} seconds; {} joules;", time / 1000000, uj / 1000000);
    Ok(())
}

pub struct LogMonitor {
    stream_pos: u64,
    buffered_reader: BufReader<File>,
    invocation_spans: HashMap<String, Span>,
    invocation_durations: HashMap<String, (String, u128)>,
    worker_spans: HashMap<String, Span>,
    total_spans: u64,
    pub functions: HashSet<String>,
}
impl LogMonitor {
    pub fn new(file_pth: &str) -> Result<Self> {
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
    pub fn read_log(&mut self) -> Result<(HashMap<String, u128>, u128)> {
        let mut overhead_ns = 0;
        let mut timing_data = HashMap::new();
        let mut buff = String::new();
        while self.buffered_reader.read_line(&mut buff)? != 0 {
            if buff.contains("span") {
                match serde_json::from_str::<Span>(&buff) {
                    Ok(span) => {
                        match span.fields.message.as_str() {
                            "enter" => (),
                            "exit" => (),
                            "close" => {
                                match span.name.as_str() {
                                    INVOKE_ID => {
                                        self.remove_invoke_transaction(&span.uuid, &span);
                                    },
                                    REGISTER_API_ID => {
                                        overhead_ns +=
                                            self.remove_worker_transaction(&span.uuid, &span, &mut timing_data);
                                    },
                                    INVOKE_API_ID => {
                                        overhead_ns +=
                                            self.remove_worker_transaction(&span.uuid, &span, &mut timing_data);
                                    },
                                    // TODO: account for background work done here
                                    _ => (),
                                };
                            },
                            "new" => {
                                self.functions.insert(span.name.clone());
                                self.total_spans += 1;
                                match span.name.as_str() {
                                    INVOKE_ID => {
                                        self.invocation_spans.insert(span.uuid.clone(), span);
                                    },
                                    REGISTER_API_ID => {
                                        self.worker_spans.insert(span.uuid.clone(), span);
                                    },
                                    INVOKE_API_ID => {
                                        self.worker_spans.insert(span.uuid.clone(), span);
                                    },
                                    // TODO: account for background work done here
                                    _ => (),
                                }
                            },
                            _ => (),
                        }
                    },
                    Err(e) => {
                        error!("len:{}; Span was missing something! \n '{}'", buff.len(), buff);
                        anyhow::bail!(e)
                    },
                }
            }
            buff.clear();
        }
        info!("tids: {}; tot: {}", self.invocation_spans.len(), self.total_spans);
        self.stream_pos = self.buffered_reader.stream_position()?;
        Ok((timing_data, overhead_ns))
    }

    fn remove_invoke_transaction(&mut self, id: &str, span: &Span) {
        let found_stamp = self.invocation_spans.remove(id);
        match found_stamp {
            Some(s) => {
                let time_ns = span.timestamp - s.timestamp;
                match s.span.fqdn() {
                    Some(f) => {
                        self.invocation_durations.insert(span.span.tid.clone(), (f, time_ns));
                    },
                    None => error!("Span didn't have a valid FQDN: {:?}", s),
                }
            },
            None => error!("Tried to remove {} that wasn't found", id),
        }
    }
    fn remove_worker_transaction(&mut self, id: &str, span: &Span, timing_data: &mut HashMap<String, u128>) -> u128 {
        let found_stamp = self.worker_spans.remove(id);
        match found_stamp {
            Some(s) => {
                let time_ns = span.timestamp - s.timestamp;
                match self.invocation_durations.get(&span.span.tid) {
                    Some((fqdn, invoke_ns)) => {
                        let overhead = time_ns - invoke_ns;
                        match timing_data.get_mut(fqdn) {
                            Some(v) => *v += *invoke_ns,
                            None => {
                                timing_data.insert(fqdn.clone(), *invoke_ns);
                            },
                        };
                        overhead
                    },
                    None => time_ns,
                }
            },
            None => {
                error!("Tried to remove {} that wasn't found", id);
                0
            },
        }
    }
}
