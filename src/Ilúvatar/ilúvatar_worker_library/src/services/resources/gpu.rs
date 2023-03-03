use std::sync::Arc;
use iluvatar_library::{utils::execute_cmd, transaction::TransactionId, types::ComputeEnum};
use parking_lot::RwLock;
use anyhow::Result;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use tracing::warn;
use crate::worker_api::worker_config::ContainerResourceConfig;

#[allow(unused)]
#[derive(Debug)]
pub struct GPU {
  pub name: String,
}

pub struct GpuResourceTracker {
  gpus: RwLock<Vec<Arc<GPU>>>,
  concurrency_semaphore: Arc<Semaphore>,
}
impl GpuResourceTracker {
  pub fn boxed(resources: Arc<ContainerResourceConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let gpus = GpuResourceTracker::prepare_structs(resources, tid)?;
    let sem =Arc::new(Semaphore::new(gpus.len()));
    Ok(Arc::new(GpuResourceTracker {
      gpus: RwLock::new(gpus),
      concurrency_semaphore: sem,
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

  /// Return a permit access to a single GPU
  /// Returns an error if none are available
  pub fn try_acquire_resource(&self) -> Result<OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
    match self.concurrency_semaphore.clone().try_acquire_many_owned(1) {
      Ok(p) => Ok(p),
      Err(e) => Err(e),
    }
  }

  /// Acquire a GPU so it can be attached to a container
  pub fn acquire_gpu(self: &Arc<Self>) -> Option<Arc<GPU>> {
    match self.gpus.write().pop() {
      Some(gpu) =>  Some(gpu),
      None => None
    }
  }

  /// Return a GPU that has been removed from a container
  pub fn return_gpu(&self, gpu: Arc<GPU>) {
    self.gpus.write().push(gpu);
  }
}
