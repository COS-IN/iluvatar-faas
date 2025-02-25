use super::EnergyConfig;
use crate::clock::{get_global_clock, Clock};
use crate::{
    bail_error,
    threading::os_thread,
    transaction::{TransactionId, ENERGY_LOGGER_PS_TID},
    utils::execute_cmd_checked,
};
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::{fs::File, io::Write, path::Path, sync::Arc, thread::JoinHandle};
use tracing::error;

pub struct ProcessMonitor {
    pid: String,
    _config: Arc<EnergyConfig>,
    _worker_thread: JoinHandle<()>,
    timer: Clock,
    log_file: RwLock<File>,
}
impl ProcessMonitor {
    pub fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
        let ms = config
            .process_freq_ms
            .ok_or_else(|| anyhow!("'process_freq_ms' cannot be 0"))?;
        let (handle, tx) = os_thread(
            ms,
            ENERGY_LOGGER_PS_TID.clone(),
            Arc::new(ProcessMonitor::monitor_process),
        )?;

        let r = Arc::new(ProcessMonitor {
            pid: std::process::id().to_string(),
            _worker_thread: handle,
            _config: config.clone(),
            timer: get_global_clock(tid)?,
            log_file: ProcessMonitor::open_log_file(&config, tid)?,
        });
        r.write_text("timestamp,cpu_pct,cpu_time\n".to_string(), tid);
        tx.send(r.clone())?;
        Ok(r)
    }

    /// Reads the different energy sources and writes the current staistics out to the csv file
    fn monitor_process(&self, tid: &TransactionId) {
        let (cpu_pct, cpu_time) =
            match execute_cmd_checked("/usr/bin/ps", vec!["-p", self.pid.as_str(), "-o", "%C %x"], None, tid) {
                Ok(out) => {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let data = stdout.split('\n').collect::<Vec<&str>>();
                    if data.len() == 1 {
                        return;
                    }
                    let data = data[1];
                    let items = data
                        .split_ascii_whitespace()
                        .filter(|str| !str.is_empty())
                        .collect::<Vec<&str>>();
                    let cpu_pct = match items[0].parse::<f64>() {
                        Ok(f) => f,
                        Err(e) => {
                            error!(error=%e, tid=tid, "Failed to parse process cpu percentage");
                            return;
                        },
                    };
                    let cpu_time = items[1];
                    (cpu_pct, cpu_time.to_string())
                },
                Err(e) => {
                    error!(error=%e, tid=tid, "Failed to call 'ps'");
                    return;
                },
            };

        let t = match self.timer.now_str() {
            Ok(t) => t,
            Err(e) => {
                error!(error=%e, tid=tid, "Failed to get time");
                return;
            },
        };
        let to_write = format!("{},{},{}\n", t, cpu_pct, cpu_time);
        self.write_text(to_write, tid);
    }

    fn open_log_file(config: &Arc<EnergyConfig>, tid: &TransactionId) -> Result<RwLock<File>> {
        match File::create(Path::new(&config.log_folder).join("process.log")) {
            Ok(f) => Ok(RwLock::new(f)),
            Err(e) => {
                bail_error!(tid=tid, error=%e, "Failed to create process monitor output file")
            },
        }
    }

    fn write_text(&self, text: String, tid: &TransactionId) {
        let mut file = self.log_file.write();
        match file.write_all(text.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                error!(error=%e, tid=tid, "Failed to write csv result to process monitor file");
            },
        };
    }
}
