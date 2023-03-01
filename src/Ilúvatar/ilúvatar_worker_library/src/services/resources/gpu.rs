use std::sync::Arc;
use iluvatar_library::{utils::execute_cmd, transaction::TransactionId, types::ComputeEnum};
use parking_lot::RwLock;
use anyhow::Result;
use tracing::warn;
use crate::worker_api::worker_config::ContainerResourceConfig;

#[allow(unused)]
#[derive(Debug)]
pub struct GPU {
  pub name: String,
}

pub struct GpuResourceTracker {
  gpus: RwLock<Vec<Arc<GPU>>>,
}
impl GpuResourceTracker {
  pub fn boxed(resources: Arc<ContainerResourceConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    Ok(Arc::new(GpuResourceTracker {
      gpus: RwLock::new(GpuResourceTracker::prepare_structs(resources, tid)?)
    }))
  }

  fn prepare_structs(resources: Arc<ContainerResourceConfig>, tid: &TransactionId) -> Result<Vec<Arc<GPU>>> {
    let mut ret = vec![];
    let gpu_config = match resources.resource_map.get(&ComputeEnum::gpu) {
      Some(c) => c.clone(),
      None => {
        warn!(tid=%tid, "resource_map did not have a GPU entry, skipping GPU resource setup");
        return Ok(ret)},
    };
    if iluvatar_library::utils::is_simulation() {
      for i in 0..gpu_config.count {
        ret.push(Arc::new(GPU { name: format!("GPU-{}", i) }))
      }
    } else {
      let output = execute_cmd("/usr/bin/nvidia-smi", &vec!["-L"], None, tid)?;
      let stdout = String::from_utf8_lossy(&output.stdout);
      let data = stdout.split("\n").collect::<Vec<&str>>();
      for row in data {
        let pos = row.find("UUID: ");
        if let Some(pos) = pos {
          let slice = &row[(pos+"UUID: ".len())..row.len()-1];
          ret.push(Arc::new(GPU { name: slice.to_string() }));
        }
      }
    }
    if ret.len() != gpu_config.count as usize {
      anyhow::bail!("Was able to prepare {} GPUs, but configuration expected {}", ret.len(), gpu_config.count);
    }
    Ok(ret)
  }

  pub fn acquire_gpu(self: &Arc<Self>) -> Option<Arc<GPU>> {
    match self.gpus.write().pop() {
      Some(gpu) =>  Some(gpu),
      None => None
    }
  }

  pub fn return_gpu(&self, gpu: Arc<GPU>) {
    self.gpus.write().push(gpu);
  }
}
