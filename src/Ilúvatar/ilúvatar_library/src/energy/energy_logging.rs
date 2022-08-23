use std::{sync::{Arc, mpsc::{channel, Receiver}}, time::Duration, fs::File, io::Write, path::Path};
use crate::{transaction::{TransactionId, WORKER_ENERGY_LOGGER_TID}, energy::{rapl::RAPL, perf::start_perf_stat, EnergyConfig}};
use time::OffsetDateTime;
use std::thread::JoinHandle;
use tracing::{debug, error};
use anyhow::Result;

pub struct EnergyLogger {
  config: Arc<EnergyConfig>,
  _worker_thread: JoinHandle<()>,
  rapl: Arc<RAPL>,
  _csv_modifiers: Vec<String>,
  _perf_child: Option<std::process::Child>,
}

impl EnergyLogger {
  pub fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let (tx, rx) = channel();
    let handle = EnergyLogger::launch_worker_thread(rx);
    
    let child = match config.enable_perf {
      true => {
        let perf_file = Path::new(&config.log_folder);
        let perf_file = perf_file.join("perf.log");
        let perf_stat_duration_sec = match config.perf_stat_duration_sec {
          Some(t) => t,
          None => panic!("'perf_stat_duration_sec was not supplied even though perf monitoring was enabled"),
        };
        println!("starting perf");
        Some(start_perf_stat(&perf_file.to_str().unwrap(), tid, perf_stat_duration_sec)?)  
      },
      false => None
    };

    let i = Arc::new(EnergyLogger {
      config,
      _worker_thread: handle,
      rapl: Arc::new(RAPL::new()?),
      _csv_modifiers: vec![],
      _perf_child: child,
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
      loop {
        svc.monitor_energy(tid, &file);
        std::thread::sleep(Duration::from_millis(svc.config.log_freq_ms));
      }
    })
  }

  fn gen_header(&self) -> String {
    let mut ret = String::from("timestamp,");
    if self.config.enable_rapl { 
      ret.push_str("rapl,");
    }
    if self.config.enable_ipmi { 
      ret.push_str("ipmi,");
    }
    ret
  }

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
      let ipmi_uj = match self.instant_impi() {
        Ok(uj) => uj,
        Err(e) => {
          error!(tid=%tid, error=%e, "Unable to read ipmi value");
          return;
        },
      };
      to_write = format!("{},{}", to_write, ipmi_uj);
    }

    // TODO: enable letting injectable data into output
    // let funcs = self.invoker.get_running();
    // to_write = format!("{},{:?}\n", to_write, funcs);

    to_write.push('\n');
    match file.write_all(to_write.as_bytes()) {
      Ok(_) => (),
      Err(e) => {
        println!("Failed to write csv of result because {}", e);
      }
    };
  }

  fn instant_rapl(&self) -> Result<u128> {
    let reading = self.rapl.record()?;
    Ok(reading.start_uj)
  }
  fn instant_impi(&self) -> Result<u128> {
   todo!();
   // TODO: ipmi support!
  }
}
