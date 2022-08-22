use std::{sync::{Arc, mpsc::{channel, Receiver}}, time::Duration, fs::File, io::Write};
use iluvatar_library::{transaction::{TransactionId, WORKER_ENERGY_LOGGER_TID}, energy::rapl::RAPL};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tracing::{debug, error};
use crate::worker_api::worker_config::EnergyConfig;
use anyhow::Result;
use super::invocation::invoker::InvokerService;

pub struct EnergyLogger {
  config: Arc<EnergyConfig>,
  _worker_thread: JoinHandle<()>,
  invoker: Arc<InvokerService>,
  rapl: Arc<RAPL>,
}
impl EnergyLogger {
  pub fn boxed(config: Arc<EnergyConfig>, invoker: Arc<InvokerService>) -> Result<Arc<Self>> {
    let (tx, rx) = channel();
    let handle = EnergyLogger::launch_worker_thread(rx);

    let i = Arc::new(EnergyLogger {
      config,
      _worker_thread: handle,
      invoker,
      rapl: Arc::new(RAPL::new()?),
    });
    tx.send(i.clone())?;
    Ok(i)
  }

  fn launch_worker_thread(rx: Receiver<Arc<EnergyLogger>>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let tid: &TransactionId = &WORKER_ENERGY_LOGGER_TID;
      let svc = match rx.recv() {
        Ok(svc) => svc,
        Err(_) => {
          error!(tid=%tid, "energy monitor thread failed to receive service from channel!");
          return;
        },
      };
      let header = svc.gen_header();
      let mut file = match File::create(&svc.config.file) {
        Ok(f) => f,
        Err(e) => {
          error!("Failed to create output file because {}", e);
          return;
        }
      };
      match file.write_all(header.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
          error!("Failed to write header of result because {}", e);
          return;
        }
      };

      debug!(tid=%tid, "worker energy logger worker started");
      loop {
        svc.monitor_energy(tid, &file);
        tokio::time::sleep(Duration::from_millis(svc.config.log_freq_ms)).await;
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
    ret.push_str("functions\n");
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

    let funcs = self.invoker.get_running();

    to_write = format!("{},{:?}\n", to_write, funcs);
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
