use std::sync::Arc;

use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::invoker::InvokerService;
use crate::transaction::TransactionId;
use crate::utils::execute_cmd;
use anyhow::Result;
use log::*;
use crate::rpc::StatusResponse;

#[derive(Debug)]
pub struct StatusService {
  container_manager: Arc<ContainerManager>, 
  invoker_service: Arc<InvokerService>
}

impl StatusService {
  pub fn boxed(cm: Arc<ContainerManager>, invoke: Arc<InvokerService>) -> Arc<Self> {
    Arc::new(StatusService { container_manager: cm, invoker_service: invoke })    
  }
  
  /// parse the string to an i64, used for getting info from vmstat
  fn parse(&self, string: &str, tid: &TransactionId) -> i64 {
    match string.parse::<i64>() {
      Ok(r) => r,
      Err(e) => {
        error!("[{}] error parsing {}: {}", tid, string, e);
        -1
      },
    }
  }

  /// Returns the status and load of the worker
  pub fn get_status(&self, tid: &TransactionId) -> Result<StatusResponse> {
    // TODO: put this on a background thread and store it, return that cached value
    // No blocking time this way
    let _free_cs = self.container_manager.free_cores();
    let free_mem = self.container_manager.free_memory();
    let out = execute_cmd("/usr/bin/vmstat", &vec!["1", "2", "-n", "-w"], None, tid)?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.split("\n").collect::<Vec<&str>>()[2].split(" ").collect();

    let us: i64 = self.parse(lines[55], tid);
    let sy: i64 = self.parse(lines[58], tid);
    let id: i64 = self.parse(lines[60], tid);
    let wa: i64 = self.parse(lines[63], tid);
    debug!("[{}] vmstat {} {} {} {}", tid, us, sy, id, wa);

    let out = execute_cmd("/usr/bin/uptime", &vec![], None, tid)?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.split(" ").collect::<Vec<&str>>();
    let min = lines[13];
    let min = &min[..min.len()-1];
    let minute_load_avg =  match min.parse::<f64>() {
        Ok(r) => r,
        Err(e) => {
          error!("[{}] error parsing float {}: {}", tid, min, e);
          -1.0
        },
      };
    debug!("[{}] uptime {}", tid, minute_load_avg);

    Ok(StatusResponse {
      success: true,
      queue_len: self.invoker_service.queue_len() as i64,
      used_mem: free_mem,
      total_mem: free_mem,
      cpu_us: us,
      cpu_sy: sy,
      cpu_id: id,
      cpu_wa: wa,
      load_avg_1minute: minute_load_avg
    })
  }
}

