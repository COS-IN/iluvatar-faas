use crate::worker_api::worker_config::ContainerResourceConfig;
use anyhow::Result;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::ComputeEnum,
    utils::{execute_cmd, execute_cmd_checked},
};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{info, trace, warn};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct GpuStatus {
    pub gpu_uuid: GpuUuid,
    /// The current performance state for the GPU. States range from P0 (maximum performance) to P12 (minimum performance).
    pub pstate: String,
    /// Total installed GPU memory.
    pub memory_total: u32,
    /// Total memory allocated by active contexts.
    pub memory_used: u32,
    /// Percent of time over the past sample period during which one or more kernels was executing on the GPU.
    /// The sample period may be between 1 second and 1/6 second depending on the product.
    pub utilization_gpu: f64,
    /// Percent of time over the past sample period during which global (device) memory was being read or written.
    /// The sample period may be between 1 second and 1/6 second depending on the product.
    pub utilization_memory: f64,
    /// The last measured power draw for the entire board, in watts. Only available if power management is supported. This reading is accurate to within +/- 5 watts.
    pub power_draw: f64,
    /// The software power limit in watts. Set by software like nvidia-smi.
    pub power_limit: f64,
}

pub type GpuUuid = String;

#[derive(Debug)]
pub struct GPU {
    pub gpu_uuid: GpuUuid,
}

/// Struct that manages GPU control between containers
/// A GPU can only be assigned to one container at a time, and must be reutrned via [GpuResourceTracker::return_gpu] after container deletion
/// For an invocation to use the GPU, it must have isolation over that resource by acquiring it via [GpuResourceTracker::try_acquire_resource]
pub struct GpuResourceTracker {
    gpus: RwLock<Vec<Arc<GPU>>>,
    concurrency_semaphore: Arc<Semaphore>,
}
impl GpuResourceTracker {
    pub fn boxed(resources: Arc<ContainerResourceConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
        let gpus = GpuResourceTracker::prepare_structs(resources, tid)?;
        let sem = Arc::new(Semaphore::new(gpus.len()));
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
                return Ok(ret);
            }
        };
        if gpu_config.count == 0 {
            info!(tid=%tid, "resource_map had 0 GPUs, skipping GPU resource setup");
            return Ok(ret);
        }
        if iluvatar_library::utils::is_simulation() {
            for i in 0..gpu_config.count {
                ret.push(Arc::new(GPU {
                    gpu_uuid: format!("GPU-{}", i),
                }))
            }
        } else {
            let output = execute_cmd("/usr/bin/nvidia-smi", &vec!["-L"], None, tid)?;
            if !output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!(
                    "nvidia-smi failed with a status of {}; stdout: '{}', stderr '{}' ",
                    output.status,
                    stdout,
                    stderr
                );
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            let data = stdout.split("\n").collect::<Vec<&str>>();
            for row in data {
                let pos = row.find("UUID: ");
                if let Some(pos) = pos {
                    let gpu_uuid = &row[(pos + "UUID: ".len())..row.len() - 1];
                    ret.push(Arc::new(GPU {
                        gpu_uuid: gpu_uuid.to_string(),
                    }));
                }
            }
        }
        if ret.len() != gpu_config.count as usize {
            anyhow::bail!(
                "Was able to prepare {} GPUs, but configuration expected {}",
                ret.len(),
                gpu_config.count
            );
        }
        info!(tid=%tid, gpus=?ret, "GPUs prepared");
        Ok(ret)
    }

    /// Return a permit access to a single GPU
    /// Returns an error if none are available
    pub fn try_acquire_resource(&self) -> Result<OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        self.concurrency_semaphore.clone().try_acquire_many_owned(1)
    }

    /// Acquire a GPU so it can be attached to a container
    /// [None] means no GPU is available
    pub fn acquire_gpu(self: &Arc<Self>) -> Option<Arc<GPU>> {
        self.gpus.write().pop()
    }

    /// Return a GPU that has been removed from a container
    pub fn return_gpu(&self, gpu: Arc<GPU>) {
        self.gpus.write().push(gpu);
    }

    /// get the utilization of GPUs on the system
    pub fn gpu_utilization(&self, tid: &TransactionId) -> Result<Vec<GpuStatus>> {
        let mut ret: Vec<GpuStatus> = vec![];
        if !std::path::Path::new("/usr/bin/nvidia-smi").exists() {
            trace!(tid=%tid, "nvidia-smi not found, not checking GPU utilization");
            return Ok(ret);
        }
        let args = vec![
            "--query-gpu=gpu_uuid,pstate,memory.total,memory.used,utilization.gpu,utilization.memory,power.draw,power.limit",
            "--format=csv,noheader,nounits",
        ];
        let nvidia = execute_cmd_checked("/usr/bin/nvidia-smi", &args, None, tid)?;
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .trim(csv::Trim::All)
            .from_reader(nvidia.stdout.as_slice());
        for record in rdr.deserialize() {
            match record {
                Ok(rec) => ret.push(rec),
                Err(e) => {
                    bail_error!(tid=%tid, error=%e, "Failed to deserialized GPU record from nvidia-smi")
                }
            }
        }
        Ok(ret)
    }
}
