use crate::{
    services::containers::{docker::DockerIsolation, ContainerIsolationService},
    worker_api::worker_config::GPUResourceConfig,
};
use anyhow::Result;
use iluvatar_library::{
    bail_error, threading::tokio_thread, transaction::TransactionId, types::MemSizeMb, utils::execute_cmd_checked,
};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, trace};

pub type GpuUuid = String;

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub enum Pstate {
    P0,
    P1,
    P2,
    P3,
    P4,
    P5,
    P6,
    P7,
    P8,
    P9,
    P10,
    P11,
    P12,
}
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct GpuStatus {
    pub gpu_uuid: GpuUuid,
    /// The current performance state for the GPU. States range from P0 (maximum performance) to P12 (minimum performance).
    pub pstate: Pstate,
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
impl GpuStatus {
    pub fn update(&mut self, new_status: GpuStatus) {
        let alpha = 0.6;
        self.pstate = new_status.pstate;
        self.memory_used = Self::moving_avg_u(alpha, self.memory_used, new_status.memory_used);
        self.utilization_gpu = Self::moving_avg_f(alpha, self.utilization_gpu, new_status.utilization_gpu);
        self.utilization_memory = Self::moving_avg_f(alpha, self.utilization_memory, new_status.utilization_memory);
        self.power_draw = Self::moving_avg_f(alpha, self.power_draw, new_status.power_draw);
    }
    fn moving_avg_f(alpha: f64, old: f64, new: f64) -> f64 {
        (new * alpha) + (old * (1.0 - alpha))
    }
    fn moving_avg_u(alpha: f64, old: u32, new: u32) -> u32 {
        ((new as f64 * alpha) + (old as f64 * (1.0 - alpha))) as u32
    }
}

#[derive(Debug)]
pub struct GPU {
    pub gpu_uuid: GpuUuid,
    pub gpu_private_id: u32,
    pub hardware_memory_mb: MemSizeMb,
    pub allocated_mb: MemSizeMb,
    /// From 1-100
    pub thread_pct: u32,
}
impl GPU {
    pub fn split_resources(
        gpu_uuid: GpuUuid,
        hardware_memory_mb: MemSizeMb,
        config: &Arc<GPUResourceConfig>,
        tid: &TransactionId,
    ) -> Result<Vec<Arc<Self>>> {
        let (spots, mem_size) = Self::compute_spots(hardware_memory_mb, &gpu_uuid, config, tid)?;
        let thread_pct = match config.mps_limit_active_threads {
            Some(true) => (100.0 / spots as f64) as u32,
            _ => 100,
        };
        let mut ret = vec![];
        for i in 0..spots {
            ret.push(Arc::new(Self {
                gpu_uuid: gpu_uuid.clone(),
                gpu_private_id: i,
                hardware_memory_mb,
                allocated_mb: mem_size,
                thread_pct,
            }))
        }
        Ok(ret)
    }

    /// Return the number of spots on the GPU and the amount of memory in each spot
    fn compute_spots(
        hardware_memory_mb: MemSizeMb,
        gpu_uuid: &GpuUuid,
        config: &Arc<GPUResourceConfig>,
        tid: &TransactionId,
    ) -> Result<(u32, MemSizeMb)> {
        if config.driver_hook_enabled() {
            return match config.funcs_per_device {
                Some(fs) => Ok((fs, hardware_memory_mb)),
                None => Ok((16, hardware_memory_mb)),
            };
        }

        let per_func_memory_mb = if config.use_standalone_mps.unwrap_or(false) {
            config.per_func_memory_mb.unwrap_or(hardware_memory_mb)
        } else if config.per_func_memory_mb.is_some() {
            config.per_func_memory_mb.unwrap()
        } else {
            hardware_memory_mb
        };
        if per_func_memory_mb == 0 {
            anyhow::bail!(
                "GPU function assigned memory was set to zero, either hardware {} or per_func {:?}",
                hardware_memory_mb,
                config.per_func_memory_mb
            );
        }
        let spots = hardware_memory_mb / per_func_memory_mb;
        info!(tid=%tid, gpu_uuid=%gpu_uuid, spots=spots, hardware_memory_mb=hardware_memory_mb, per_func_memory_mb=per_func_memory_mb, "GPU available spots");
        Ok((spots as u32, hardware_memory_mb))
    }
}

const MPS_CONTAINER_NAME: &str = "iluvatar-mps-daemon";
lazy_static::lazy_static! {
  static ref GPU_RESC_TID: TransactionId = "GPU_RESC_TRACK".to_string();
}
/// Struct that manages GPU control between containers
/// A GPU can only be assigned to one container at a time, and must be reutrned via [GpuResourceTracker::return_gpu] after container deletion
/// For an invocation to use the GPU, it must have isolation over that resource by acquiring it via [GpuResourceTracker::try_acquire_resource]
pub struct GpuResourceTracker {
    gpus: RwLock<Vec<Arc<GPU>>>,
    concurrency_semaphore: Arc<Semaphore>,
    docker: Arc<dyn ContainerIsolationService>,
    _handle: tokio::task::JoinHandle<()>,
    status_info: RwLock<Vec<GpuStatus>>,
}
impl GpuResourceTracker {
    pub fn boxed(
        resources: &Option<Arc<GPUResourceConfig>>,
        tid: &TransactionId,
        docker: &Arc<dyn ContainerIsolationService>,
        status_config: &Arc<crate::worker_api::worker_config::StatusConfig>,
    ) -> Result<Option<Arc<Self>>> {
        if let Some(config) = resources.clone() {
            let gpus = GpuResourceTracker::prepare_structs(&config, tid)?;
            if config.mps_enabled() {
                if let Some(docker) = docker.as_any().downcast_ref::<DockerIsolation>() {
                    Self::start_mps(docker, tid)?;
                } else {
                    bail_error!("MPS is enabled, but isolation service passed was not `docker`");
                }
            } else {
                Self::set_shared_exclusive(tid)?;
            }
            let (handle, tx) = tokio_thread(
                status_config.report_freq_ms,
                GPU_RESC_TID.to_owned(),
                Self::gpu_utilization,
            );

            let svc = Arc::new(GpuResourceTracker {
                concurrency_semaphore: Self::create_concurrency_semaphore(&config, &gpus, tid),
                gpus: RwLock::new(gpus),
                docker: docker.clone(),
                _handle: handle,
                status_info: RwLock::new(vec![]),
            });
            tx.send(svc.clone())?;
            return Ok(Some(svc));
        }
        Ok(None)
    }

    fn create_concurrency_semaphore(
        _config: &Arc<GPUResourceConfig>,
        gpus: &Vec<Arc<GPU>>,
        _tid: &TransactionId,
    ) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(gpus.len()))
    }

    fn start_mps(docker: &DockerIsolation, tid: &TransactionId) -> Result<()> {
        debug!(tid=%tid, "Setting MPS exclusive");
        Self::set_gpu_exclusive(tid)?;
        debug!(tid=%tid, "Launching MPS container");
        let args = vec![
            "--name",
            MPS_CONTAINER_NAME,
            "--gpus",
            "all",
            "--ipc=host",
            "--entrypoint",
            "/usr/bin/nvidia-cuda-mps-control",
            "-v",
            "/tmp/nvidia-mps:/tmp/nvidia-mps",
        ];
        let img_name = "docker.io/nvidia/cuda:11.8.0-base-ubuntu20.04";
        docker.docker_run(args, img_name, "iluvatar_mps_control", Some("-f"), tid, None)
    }

    fn set_shared_exclusive(tid: &TransactionId) -> Result<()> {
        let output = execute_cmd_checked("/usr/bin/nvidia-smi", vec!["-L"], None, tid)?;
        let cow: std::borrow::Cow<'_, str> = String::from_utf8_lossy(&output.stdout);
        let gpu_cnt = cow
            .split('\n')
            .filter(|str| !str.is_empty())
            .collect::<Vec<&str>>()
            .len();
        for i in 0..gpu_cnt {
            execute_cmd_checked(
                "/usr/bin/nvidia-smi",
                vec!["-i", i.to_string().as_str(), "-c", "DEFAULT"],
                None,
                tid,
            )?;
        }
        Ok(())
    }

    fn set_gpu_exclusive(tid: &TransactionId) -> Result<()> {
        let output = execute_cmd_checked("/usr/bin/nvidia-smi", vec!["-L"], None, tid)?;
        let cow: std::borrow::Cow<'_, str> = String::from_utf8_lossy(&output.stdout);
        let gpu_cnt = cow
            .split('\n')
            .filter(|str| !str.is_empty())
            .collect::<Vec<&str>>()
            .len();
        for i in 0..gpu_cnt {
            execute_cmd_checked(
                "/usr/bin/nvidia-smi",
                vec!["-i", i.to_string().as_str(), "-c", "EXCLUSIVE_PROCESS"],
                None,
                tid,
            )?;
        }
        Ok(())
    }

    /// Return the count of 'physical' GPUs, and list of GPU spot structs
    fn make_simulated_gpus(gpu_config: &Arc<GPUResourceConfig>, tid: &TransactionId) -> Result<(u32, Vec<Arc<GPU>>)> {
        let mut ret = vec![];
        let mut found = 0;
        let memory_mb = gpu_config
            .memory_mb
            .ok_or_else(|| anyhow::format_err!("`memory_mb` config must be provided"))?;
        for i in 0..gpu_config.count {
            let gpu_uuid = format!("GPU-{}", i);
            ret.extend(GPU::split_resources(gpu_uuid, memory_mb, gpu_config, tid)?);
            found += 1;
        }
        Ok((found, ret))
    }

    /// Return the count of 'physical' GPUs, and list of GPU spot structs
    fn make_real_gpus(gpu_config: &Arc<GPUResourceConfig>, tid: &TransactionId) -> Result<(u32, Vec<Arc<GPU>>)> {
        let mut ret = vec![];
        let mut found = 0;
        let output = execute_cmd_checked(
            "/usr/bin/nvidia-smi",
            vec!["--query-gpu=gpu_uuid,memory.total", "--format=csv,noheader,nounits"],
            None,
            tid,
        )?;
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .trim(csv::Trim::All)
            .from_reader(output.stdout.as_slice());
        for result in reader.records() {
            match result {
                Ok(r) => {
                    let memory_mb = r
                        .get(1)
                        .ok_or_else(|| anyhow::format_err!("Reading GPU memory failed"))?
                        .parse()
                        .or_else(|e| anyhow::bail!("Failed to parse GPU memory.total {:?} from '{:?}'", e, r))?;
                    let gpu_uuid = r
                        .get(0)
                        .ok_or_else(|| anyhow::format_err!("Reading GPU uuid failed from '{:?}'", r))?
                        .to_string();
                    ret.extend(GPU::split_resources(gpu_uuid, memory_mb, gpu_config, tid)?);
                }
                Err(e) => bail_error!(tid=%tid, error=%e, "Failed to read record from nvidia-smi"),
            }
            found += 1;
        }
        Ok((found, ret))
    }

    fn prepare_structs(gpu_config: &Arc<GPUResourceConfig>, tid: &TransactionId) -> Result<Vec<Arc<GPU>>> {
        if gpu_config.count == 0 {
            info!(tid=%tid, "GPU config had 0 GPUs, skipping GPU resource setup");
            return Ok(vec![]);
        }
        let ret;
        let count;
        if iluvatar_library::utils::is_simulation() {
            (count, ret) = Self::make_simulated_gpus(gpu_config, tid)?;
        } else {
            (count, ret) = Self::make_real_gpus(gpu_config, tid)?;
        }
        if count != gpu_config.count {
            anyhow::bail!(
                "Was able to prepare {} GPUs, but configuration expected {}",
                count,
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
    async fn gpu_utilization(svc: Arc<Self>, tid: TransactionId) {
        if !std::path::Path::new("/usr/bin/nvidia-smi").exists() {
            trace!(tid=%tid, "nvidia-smi not found, not checking GPU utilization");
            return;
        }
        let args = vec![
          "--query-gpu=gpu_uuid,pstate,memory.total,memory.used,utilization.gpu,utilization.memory,power.draw,power.limit",
          "--format=csv,noheader,nounits",
        ];
        let nvidia = match execute_cmd_checked("/usr/bin/nvidia-smi", args, None, &tid) {
            Ok(r) => r,
            Err(e) => {
                error!(tid=%tid, error=%e, "Failed to call nvidia-smi");
                return;
            }
        };
        let is_empty = (*svc.status_info.read()).is_empty();
        let mut ret: Vec<GpuStatus> = vec![];
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .trim(csv::Trim::All)
            .from_reader(nvidia.stdout.as_slice());
        for record in rdr.deserialize::<GpuStatus>() {
            match record {
                Ok(rec) => {
                    if is_empty {
                        ret.push(rec);
                    } else {
                        let mut lck = svc.status_info.write();
                        for stat in &mut *lck {
                            if stat.gpu_uuid == rec.gpu_uuid {
                                stat.update(rec);
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!(tid=%tid, error=%e, "Failed to deserialized GPU record from nvidia-smi"),
            }
        }
        if is_empty {
            debug!(tid=%tid, "Setting GPU status info for first time");
            *svc.status_info.write() = ret;
        }
    }

    /// get the utilization of GPUs on the system
    pub fn gpu_status(&self, _tid: &TransactionId) -> Vec<GpuStatus> {
        (*self.status_info.read()).clone()
    }
}
impl Drop for GpuResourceTracker {
    fn drop(&mut self) {
        if let Some(docker) = self.docker.as_any().downcast_ref::<DockerIsolation>() {
            let tid: &TransactionId = &GPU_RESC_TID;
            match docker.get_logs(MPS_CONTAINER_NAME, tid) {
                Ok((stdout, stderr)) => info!(stdout=%stdout, stderr=%stderr, tid=%tid, "MPS daemon exit logs"),
                Err(e) => error!(error=%e, tid=%tid, "Failed to get MPS daemon logs"),
            }
        }
    }
}
