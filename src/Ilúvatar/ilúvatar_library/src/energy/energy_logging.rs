use std::{sync::{Arc, mpsc::{channel, Receiver}}, time::Duration, fs::File, io::Write, path::Path};
use crate::{transaction::{TransactionId, WORKER_ENERGY_LOGGER_TID}, energy::{rapl::RAPL, perf::start_perf_stat, EnergyConfig}, continuation::Continuation};
use time::OffsetDateTime;
use std::thread::JoinHandle;
use tracing::{debug, error};
use anyhow::Result;
use super::ipmi::IPMI;

pub type EnergyInjectableT = Arc<dyn Fn() -> String + Send + Sync>;

/// Struct that repeatedly checks energy usage from various sources
/// They are then stored in a timestamped log file
/// Optionall can inject additional information to be included in each line
pub struct EnergyLogger {
  config: Arc<EnergyConfig>,
  _worker_thread: JoinHandle<()>,
  rapl: Arc<RAPL>,
  ipmi: Option<IPMI>,
  _csv_modifiers: Vec<String>,
  _perf_child: Option<std::process::Child>,
  headers: Option<Vec<String>>,
  csv_injectables: Option<Vec<EnergyInjectableT>>,
  continuation: Arc<Continuation>
}

impl EnergyLogger {
  pub fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId, headers: Option<Vec<String>>, csv_injectables: Option<Vec<EnergyInjectableT>>, continuation: Arc<Continuation>) -> Result<Arc<Self>> {
    match &headers {
      Some(hs) => match &csv_injectables {
        Some(cvs) => {
          if cvs.len() != hs.len() {
            panic!("Supplied 'headers' and 'csv_injectables' did not have same length")
          }
        },
        None => panic!("Must supply both 'headers' and 'csv_injectables', one was 'None'"),
      },
      None => (),
    };

    let (tx, rx) = channel();
    let handle = EnergyLogger::launch_worker_thread(rx);
    
    let child = match config.enable_perf {
      true => {
        let perf_file = Path::new(&config.log_folder);
        let perf_file = perf_file.join("energy-perf.log");
        let perf_stat_duration_ms = match config.perf_stat_duration_ms {
          Some(t) => t,
          None => panic!("'--perf-stat-duration-ms' was not supplied even though perf monitoring was enabled"),
        };
        Some(start_perf_stat(&perf_file.to_str().unwrap(), tid, perf_stat_duration_ms)?)  
      },
      false => None
    };

    let ipmi = match config.enable_ipmi {
      true => Some(IPMI::new(
          config.ipmi_pass_file.as_ref().expect("'ipmi_pass_file was not present with ipmi enabled").clone(), 
          config.ipmi_ip_addr.as_ref().expect("'ipmi_ip_addr was not present with ipmi enabled").clone(), tid)?),
      false => None,
    };

    let i = Arc::new(EnergyLogger {
      config,
      _worker_thread: handle,
      rapl: Arc::new(RAPL::new()?),
      _csv_modifiers: vec![],
      _perf_child: child,
      headers,
      csv_injectables,
      ipmi,
      continuation
    });
    tx.send(i.clone())?;
    Ok(i)
  }

  fn launch_worker_thread(rx: Receiver<Arc<EnergyLogger>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
      let tid: &TransactionId = &WORKER_ENERGY_LOGGER_TID;
      let svc = match rx.recv() {
        Ok(svc) => svc,
        Err(e) => {
          error!(tid=%tid, error=%e, "energy monitor thread failed to receive service from channel!");
          return;
        },
      };

      let header = svc.gen_header();
      let mut file = match File::create(Path::new(&svc.config.log_folder).join("energy-function.log")) {
        Ok(f) => f,
        Err(e) => {
          error!(tid=%tid, error=%e, "Failed to create output file");
          return;
        }
      };
      match file.write_all(header.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
          error!(tid=%tid, error=%e, "Failed to write header of result");
          return;
        }
      };

      debug!(tid=%tid, "worker energy logger worker started");
      svc.continuation.thread_start(tid);
      while svc.continuation.check_continue() {
        svc.monitor_energy(tid, &file);
        std::thread::sleep(Duration::from_millis(svc.config.log_freq_ms));
      }
      svc.continuation.thread_exit(tid);
    })
  }

  /// Generate the header for our csv 
  fn gen_header(&self) -> String {
    let mut ret = String::from("timestamp");
    if self.config.enable_rapl {
      ret.push_str(",rapl_uj");
    }
    if self.config.enable_ipmi {
      ret.push_str(",ipmi");
    }
    match &self.headers {
      Some(v) => {
        for h in v {
          ret.push_str(",");
          ret.push_str(&h);
        }
      },
      None => (),
    }
    ret.push('\n');
    ret
  }

  /// Reads the different energy sources and writes the current staistics out to the csv file
  fn monitor_energy(&self, tid: &TransactionId, mut file: &File) {
    let now = OffsetDateTime::now_utc();
    let mut to_write = now.to_string();
    if self.config.enable_rapl {
      let rapl_uj = match self.instant_rapl() {
        Ok(uj) => uj,
        Err(e) => {
          error!(tid=%tid, error=%e, "Unable to read rapl value");
          return;
        },
      };
      to_write = format!("{},{}", to_write, rapl_uj);
    }
    if self.config.enable_ipmi {
      let ipmi_uj = match self.instant_impi(tid) {
        Ok(uj) => uj,
        Err(e) => {
          error!(tid=%tid, error=%e, "Unable to read ipmi value");
          return;
        },
      };
      to_write = format!("{},{}", to_write, ipmi_uj);
    }

    match &self.csv_injectables {
      Some(cvs) => {
        for cvs in cvs {
          to_write.push(',');
          to_write.push_str(cvs().as_str());
        }
      },
      None => (),
    }

    to_write.push('\n');
    match file.write_all(to_write.as_bytes()) {
      Ok(_) => (),
      Err(e) => {
        println!("Failed to write csv of result because {}", e);
      }
    };
  }

  /// The current RAPL energy usage value in uj
  fn instant_rapl(&self) -> Result<u128> {
    let reading = self.rapl.record()?;
    Ok(reading.start_uj)
  }
  /// The current IPMI energy usage value in watts
  fn instant_impi(&self, tid: &TransactionId) -> Result<u128> {
    match &self.ipmi {
      Some(i) => i.read(tid),
      None => anyhow::bail!("ipmi variable was non despite trying to query it!"),
    }
  }
}

impl Drop for EnergyLogger {
  fn drop(&mut self) {
    match self._perf_child.take() {
      Some(mut c) => match c.kill() {
        Ok(_) => (),
        Err(e) => error!(error=%e, "Failed to kill perf child!"),
      },
      None => (),
    }
  }
}
