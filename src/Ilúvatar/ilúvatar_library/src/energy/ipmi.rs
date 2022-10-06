use std::{path::Path, sync::{Arc, mpsc::{channel, Receiver}}, thread::JoinHandle, fs::File, io::Write, time::Duration};
use tracing::{trace, error, debug};
use crate::{utils::execute_cmd, transaction::{TransactionId, WORKER_ENERGY_LOGGER_TID}, logging::LocalTime};
use anyhow::Result;
use super::EnergyConfig;

pub struct IPMI {
  ipmi_pass_file: String,
  ipmi_ip_addr: String
}

impl IPMI {
  pub fn new(ipmi_pass_file: String, ipmi_ip_addr: String, tid: &TransactionId) -> anyhow::Result<Self> {
    let b = Path::new(&ipmi_pass_file);
    if ! b.exists() {
      anyhow::bail!("IPMI password file '{}' does not exist", ipmi_pass_file)
    }

    let i = IPMI {
      ipmi_pass_file,
      ipmi_ip_addr,
    };

    // test settings
    i.read(tid)?;
    Ok(i)
  }

  /// Get the instantaneous wattage usage of the system from ipmi
  pub fn read(&self, tid: &TransactionId) -> anyhow::Result<u128> {
    trace!(tid=%tid, "Reading from ipmi");
    let output = execute_cmd("/usr/bin/ipmitool", &vec!["-f", self.ipmi_pass_file.as_str(), "-I", "lanplus",
                                                       "-H", self.ipmi_ip_addr.as_str(), "-U", "ADMIN", "dcmi", "power", "reading"], None, tid)?;
    match output.status.code() {
      Some(0) => (),
      Some(code) => anyhow::bail!("Got an illegal exit code from command '{}', output: {:?}", code, output),
      None => anyhow::bail!("Got no exit code from command, output: {:?}", output),
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut split = stdout.split("\n");
    match split.nth(1) {
      Some(instant_line) => {
        let strs: Vec<&str> = instant_line.split(" ").filter(|str| str.len() > 0).collect();
        if strs.len() == 5 {
          let watts = strs[3];
          Ok(watts.parse::<u128>()?)
        } else {
          anyhow::bail!("Instantaneous wattage line was the incorrect size, was: '{:?}'", strs)
        }
      },
      None => {
        anyhow::bail!("Stdout was too short, got '{}'", stdout)
      },
    }
  }
}

pub struct IPMIMonitor {
  ipmi: IPMI,
  config: Arc<EnergyConfig>,
  _worker_thread: JoinHandle<()>,
  timer: LocalTime,
}
impl IPMIMonitor {
  pub fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let (tx, rx) = channel();
    let handle = IPMIMonitor::launch_worker_thread(rx);

    let i = IPMI::new(
      config.ipmi_pass_file.as_ref().expect("'ipmi_pass_file' was not present with ipmi enabled").clone(), 
      config.ipmi_ip_addr.as_ref().expect("'ipmi_ip_addr' was not present with ipmi enabled").clone(), tid)?;
    let r = Arc::new(IPMIMonitor {
      ipmi: i,
      _worker_thread: handle,
      config,
      timer: LocalTime::new(tid)?
    });
    tx.send(r.clone())?;
    Ok(r)
  }

  fn launch_worker_thread(rx: Receiver<Arc<IPMIMonitor>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
      let tid: &TransactionId = &WORKER_ENERGY_LOGGER_TID;
      let svc = match rx.recv() {
        Ok(svc) => svc,
        Err(e) => {
          error!(tid=%tid, error=%e, "IMPI energy monitor thread failed to receive service from channel!");
          return;
        },
      };

      let mut file = match File::create(Path::new(&svc.config.log_folder).join("energy-ipmi.log")) {
        Ok(f) => f,
        Err(e) => {
          error!(tid=%tid, error=%e, "Failed to create output file");
          return;
        }
      };
      match file.write_all("timestamp,ipmi\n".as_bytes()) {
        Ok(_) => (),
        Err(e) => {
          error!(tid=%tid, error=%e, "Failed to write header of result");
          return;
        }
      };

      debug!(tid=%tid, "worker IPMI energy logger worker started");
      crate::continuation::GLOB_CONT_CHECK.thread_start(tid);
      while crate::continuation::GLOB_CONT_CHECK.check_continue() {
        svc.monitor_energy(tid, &file);
        std::thread::sleep(Duration::from_millis(svc.config.ipmi_freq_ms));
      }
      crate::continuation::GLOB_CONT_CHECK.thread_exit(tid);
    })
  }

    /// Reads the different energy sources and writes the current staistics out to the csv file
    fn monitor_energy(&self, tid: &TransactionId, mut file: &File) {
      let ipmi_uj = match self.ipmi.read(tid) {
        Ok(uj) => uj,
        Err(e) => {
          error!(tid=%tid, error=%e, "Unable to read ipmi value");
          return;
        },
      };
      let t = match self.timer.now_str() {
        Ok(t) => t,
        Err(e) => {
          error!(error=%e, tid=%tid, "Failed to get time");
          return;
        },
      };
      let to_write = format!("{},{}\n", t, ipmi_uj);
      match file.write_all(to_write.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
          error!(error=%e, tid=%tid, "Failed to write csv result");
        }
      };
    }
}
