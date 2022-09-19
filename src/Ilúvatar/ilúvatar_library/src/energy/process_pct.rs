use std::{path::Path, sync::{Arc, mpsc::{channel, Receiver}}, thread::JoinHandle, fs::File, io::Write, time::Duration};
use tracing::{error, debug};
use crate::{utils::execute_cmd, transaction::{TransactionId, ENERGY_LOGGER_PS_TID}, logging::LocalTime};
use anyhow::Result;
use super::EnergyConfig;

pub struct ProcessMonitor {
  pid: String,
  config: Arc<EnergyConfig>,
  _worker_thread: JoinHandle<()>,
  timer: LocalTime,
}
impl ProcessMonitor {
  pub fn boxed(config: Arc<EnergyConfig>) -> Result<Arc<Self>> {
    let (tx, rx) = channel();
    let handle = ProcessMonitor::launch_worker_thread(rx);

    let r = Arc::new(ProcessMonitor {
      pid: std::process::id().to_string(),
      _worker_thread: handle,
      config,
      timer: LocalTime::new()?
    });
    tx.send(r.clone())?;
    Ok(r)
  }

  fn launch_worker_thread(rx: Receiver<Arc<ProcessMonitor>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
      let tid: &TransactionId = &ENERGY_LOGGER_PS_TID;
      let svc = match rx.recv() {
        Ok(svc) => svc,
        Err(e) => {
          error!(tid=%tid, error=%e, "process monitor thread failed to receive service from channel!");
          return;
        },
      };

      let mut file = match File::create(Path::new(&svc.config.log_folder).join("process.log")) {
        Ok(f) => f,
        Err(e) => {
          error!(tid=%tid, error=%e, "Failed to create output file");
          return;
        }
      };
      match file.write_all("timestamp,cpu_pct,cpu_time\n".as_bytes()) {
        Ok(_) => (),
        Err(e) => {
          error!(tid=%tid, error=%e, "Failed to write header of result");
          return;
        }
      };

      debug!(tid=%tid, "worker process logger worker thread started");
      crate::continuation::GLOB_CONT_CHECK.thread_start(tid);
      while crate::continuation::GLOB_CONT_CHECK.check_continue() {
        svc.monitor_process(tid, &file);
        std::thread::sleep(Duration::from_millis(svc.config.process_freq_ms));
      }
      crate::continuation::GLOB_CONT_CHECK.thread_exit(tid);
    })
  }

    /// Reads the different energy sources and writes the current staistics out to the csv file
    fn monitor_process(&self, tid: &TransactionId, mut file: &File) {
      let (cpu_pct, cpu_time) = match execute_cmd("/usr/bin/ps", &vec!["-p", self.pid.as_str(), "-o", "%C %x"], None, tid) {
        Ok(out) => {
          let stdout = String::from_utf8_lossy(&out.stdout);
          let data = stdout.split("\n").collect::<Vec<&str>>()[1];
          let items = data.split_ascii_whitespace().filter(|str| str.len() > 0).collect::<Vec<&str>>();
          let cpu_pct = match items[0].parse::<f64>() {
            Ok(f) => f,
            Err(e) => {
              error!(error=%e, tid=%tid, "Failed to parse process cpu percentage");
              return;
            },
          };
          let cpu_time = items[1];
          (cpu_pct, cpu_time.to_string())
        },
        Err(e) => {
          error!(error=%e, tid=%tid, "Failed to call 'ps'");
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
      let to_write = format!("{},{},{}\n", t, cpu_pct, cpu_time);
      match file.write_all(to_write.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
          error!(error=%e, tid=%tid, "Failed to write csv result");
        }
      };
    }
}
