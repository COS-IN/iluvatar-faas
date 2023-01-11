use std::{path::Path, sync::Arc, thread::JoinHandle, fs::File, io::Write};
use tracing::{trace, error};
use crate::{utils::execute_cmd, transaction::{TransactionId, WORKER_ENERGY_LOGGER_TID}, logging::LocalTime, bail_error};
use anyhow::Result;
use super::EnergyConfig;
use parking_lot::RwLock;
use crate::threading::os_thread;

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
  _config: Arc<EnergyConfig>,
  _worker_thread: JoinHandle<()>,
  log_file: RwLock<File>,
  timer: LocalTime,
}
impl IPMIMonitor {
  pub fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let (handle, tx) = os_thread(config.ipmi_freq_ms, WORKER_ENERGY_LOGGER_TID.clone(), Arc::new(IPMIMonitor::monitor_energy))?;

    let i = IPMI::new(
      config.ipmi_pass_file.as_ref().expect("'ipmi_pass_file' was not present with ipmi enabled").clone(), 
      config.ipmi_ip_addr.as_ref().expect("'ipmi_ip_addr' was not present with ipmi enabled").clone(), tid)?;
    let r = Arc::new(IPMIMonitor {
      ipmi: i,
      _worker_thread: handle,
      _config: config.clone(),
      timer: LocalTime::new(tid)?,
      log_file: IPMIMonitor::open_log_file(&config, tid)?,
    });

    r.write_text("timestamp,ipmi\n".to_string(), tid);
    tx.send(r.clone())?;
    Ok(r)
  }

  /// Reads the different energy sources and writes the current staistics out to the csv file
  fn monitor_energy(&self, tid: &TransactionId) {
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
    self.write_text(to_write, tid);
  }

  fn open_log_file(config: &Arc<EnergyConfig>, tid: &TransactionId) -> Result<RwLock<File>> {
    match File::create(Path::new(&config.log_folder).join("energy-ipmi.log")) {
      Ok(f) => Ok(RwLock::new(f)),
      Err(e) => {
        bail_error!(tid=%tid, error=%e, "Failed to create IPMI output file")
      }
    }
  }

  fn write_text(&self, text: String, tid: &TransactionId) {
    let mut file = self.log_file.write();
    match file.write_all(text.as_bytes()) {
      Ok(_) => (),
      Err(e) => {
        error!(error=%e, tid=%tid, "Failed to write csv result to IPMI file");
      }
    };
  }
}
