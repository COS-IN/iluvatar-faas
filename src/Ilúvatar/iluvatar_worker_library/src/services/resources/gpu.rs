use crate::{
    services::containers::{docker::DockerIsolation, ContainerIsolationService},
    worker_api::worker_config::{ContainerResourceConfig, GPUResourceConfig},
};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::ring_buff::{RingBuffer, Wireable};
use iluvatar_library::threading::tokio_logging_thread;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{DroppableToken, MemSizeMb},
    utils::{execute_cmd_checked, execute_cmd_checked_async, missing_or_zero_default},
    ToAny,
};
use nvml_wrapper::{error::NvmlError, Nvml};
use parking_lot::{RwLock, RwLockReadGuard};
use std::fmt::Display;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, trace, warn};

pub type GpuUuid = String;
pub type InternalGpuId = u32;

/// //////////////////////////////////////////////////////////////
/// HIGH LEVEL GPU DOCS
///
/// Hardware enforcement of resource restrictions must be enabled & managed by the user.
/// I.E. enabling MPS/MIG and verifying the correct distribution.
///
/// Structs:
/// There are two types to control generic access to devices.
/// [GPU]: control the # of container attached to a GPU. Used to create containers
/// [GpuToken]: indicate that the holder can _execute_ on the specified GPU.
///     They must hold/acquire a [GPU] as well to actualize this.
///     To invoke on the wrong GPU is a logic ERROR (bug)
/// Memory:
/// [GpuResourceTracker] helps track memory usage per-container, but does not enforce any limits/controls.
/// Calling [GpuResourceTracker::update_mem_usage] to update when a container's memory usage has notable changed.
/// I.e. container removal, post-invocation, swapping onto/off device
/// [GpuResourceTracker::get_free_mem] returns the available space in MB on the GPU, to allower caller to make decision on allocation.
///
/// Management of memory is left to high-up systems.
/// [crate::services::containers::containermanager::ContainerManager] exposes two functions to help ease this
///     [crate::services::containers::containermanager::ContainerManager::make_room_on_gpu]
///     [crate::services::containers::containermanager::ContainerManager::move_off_device]
///
/// Each container (i.e. [crate::services::containers::structs::ContainerT]/[crate::services::containers::structs::Container] has an internal memory (exposed via functions)
///   to track how much memory is last used on GPU and whether it was moved onto/off of device
///
/// The system that directs containers to move memory on/off device is responsible for calling [GpuResourceTracker] to update with relevant changes.
/// //////////////////////////////////////////////////////////////

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
impl From<nvml_wrapper::enum_wrappers::device::PerformanceState> for Pstate {
    fn from(value: nvml_wrapper::enum_wrappers::device::PerformanceState) -> Self {
        match value {
            nvml_wrapper::enum_wrappers::device::PerformanceState::Zero => Self::P0,
            nvml_wrapper::enum_wrappers::device::PerformanceState::One => Self::P1,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Two => Self::P2,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Three => Self::P3,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Four => Self::P4,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Five => Self::P5,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Six => Self::P6,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Seven => Self::P7,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Eight => Self::P8,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Nine => Self::P9,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Ten => Self::P10,
            nvml_wrapper::enum_wrappers::device::PerformanceState::Eleven => Self::P11,
            _ => Self::P12,
        }
    }
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
    /// Instant GPU compute utilization, this field is not in the CSV, so reader must be flexible to it being missing.
    /// Percent of time over the past sample period during which one or more kernels was executing on the GPU.
    /// The sample period may be between 1 second and 1/6 second depending on the product.
    pub instant_utilization_gpu: f64,
    /// A moving average of GPU compute utilization
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
    /// Number of functions running at the time
    pub num_running: u32,
    /// Tracked on-device memory allocation, if being monitored.
    pub tracked_mem: MemSizeMb,
    /// Estimated utilization manually tracked by service to account for newly launched functions
    pub est_utilization_gpu: f64,
}
#[derive(Debug, serde::Deserialize, serde::Serialize, Clone, ToAny)]
pub struct GpuStatVec(Vec<GpuStatus>);
impl Wireable for GpuStatVec {}
impl Display for GpuStatVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match serde_json::to_string::<Vec<GpuStatus>>(&self.0) {
            Ok(s) => s,
            Err(_e) => return Err(std::fmt::Error {}),
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct GpuParseStatus {
    pub gpu_uuid: GpuUuid,
    /// The current performance state for the GPU. States range from P0 (maximum performance) to P12 (minimum performance).
    pub pstate: Pstate,
    /// Total installed GPU memory.
    pub memory_total: u32,
    /// Total memory allocated by active contexts.
    pub memory_used: u32,
    /// A moving average of GPU compute utilization
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
    pub fn new(gpu_uuid: GpuUuid, memory_mb: MemSizeMb, num_running: u32) -> Self {
        GpuStatus {
            gpu_uuid,
            pstate: Pstate::P0,
            memory_total: memory_mb as u32,
            memory_used: 0,
            instant_utilization_gpu: 0.0,
            utilization_gpu: 0.0,
            utilization_memory: 0.0,
            power_draw: 0.0,
            power_limit: 0.0,
            num_running,
            est_utilization_gpu: 0.0,
            tracked_mem: 0,
        }
    }
    pub fn update(&mut self, new_status: GpuParseStatus, num_running: u32, tracked_mem: MemSizeMb) {
        let alpha = 0.6;
        self.pstate = new_status.pstate;
        self.instant_utilization_gpu = new_status.utilization_gpu;
        self.memory_used = Self::moving_avg_u(alpha, self.memory_used, new_status.memory_used);
        self.utilization_gpu = Self::moving_avg_f(alpha, self.utilization_gpu, new_status.utilization_gpu);
        self.utilization_memory = Self::moving_avg_f(alpha, self.utilization_memory, new_status.utilization_memory);
        self.power_draw = Self::moving_avg_f(alpha, self.power_draw, new_status.power_draw);
        self.num_running = num_running;
        self.tracked_mem = tracked_mem;
        self.est_utilization_gpu = self.utilization_gpu;
    }
    fn moving_avg_f(alpha: f64, old: f64, new: f64) -> f64 {
        (new * alpha) + (old * (1.0 - alpha))
    }
    fn moving_avg_u(alpha: f64, old: u32, new: u32) -> u32 {
        ((new as f64 * alpha) + (old as f64 * (1.0 - alpha))) as u32
    }
}
impl From<GpuParseStatus> for GpuStatus {
    fn from(val: GpuParseStatus) -> Self {
        GpuStatus {
            gpu_uuid: val.gpu_uuid,
            pstate: val.pstate,
            memory_total: val.memory_total,
            memory_used: val.memory_used,
            instant_utilization_gpu: val.utilization_gpu,
            utilization_gpu: val.utilization_gpu,
            utilization_memory: val.utilization_memory,
            power_draw: val.power_draw,
            power_limit: val.power_limit,
            est_utilization_gpu: val.utilization_gpu,
            num_running: 0,
            tracked_mem: 0,
        }
    }
}

type MetadataCollection = HashMap<InternalGpuId, GpuMetadata>;
#[allow(unused)]
struct GpuMetadata {
    pub gpu_uuid: GpuUuid,
    pub hardware_id: InternalGpuId,
    pub hardware_memory_mb: MemSizeMb,
    pub device_allocated_memory: RwLock<MemSizeMb>,
    pub num_structs: u32,
    pub max_running: u32,
    pub sem: Arc<Semaphore>,
}
impl GpuMetadata {
    fn new(
        gpu_uuid: GpuUuid,
        hardware_id: InternalGpuId,
        memory_mb: MemSizeMb,
        structs: &[GPU],
        sem: Arc<Semaphore>,
    ) -> Self {
        GpuMetadata {
            gpu_uuid: gpu_uuid.clone(),
            hardware_memory_mb: memory_mb,
            num_structs: structs.len() as u32,
            max_running: sem.available_permits() as u32,
            sem,
            hardware_id,
            device_allocated_memory: RwLock::new(0),
        }
    }
}
type GpuCollection = DashMap<InternalGpuId, Vec<GPU>>;
pub type ProtectedGpuRef<'a> = RwLockReadGuard<'a, Option<GPU>>;
#[derive(Debug)]
#[allow(unused)]
pub struct GPU {
    pub gpu_uuid: GpuUuid,
    pub gpu_hardware_id: InternalGpuId,
    struct_id: InternalGpuId,
    /// Total memory size of the device
    hardware_memory_mb: MemSizeMb,
    /// Size in MB the owner is allotted on device
    pub allotted_mb: MemSizeMb,
    /// From 1-100, percentage of compute on device allotted
    pub thread_pct: u32,
}
impl GPU {
    pub fn split_resources(
        gpu_uuid: &GpuUuid,
        gpu_hardware_id: InternalGpuId,
        hardware_memory_mb: MemSizeMb,
        config: &Arc<GPUResourceConfig>,
        tid: &TransactionId,
    ) -> Result<Vec<Self>> {
        let (spots, mem_size) = Self::compute_spots(hardware_memory_mb, gpu_uuid, config, tid)?;
        let thread_pct = match config.concurrent_running_funcs {
            Some(0) => (100.0 / spots as f64) as u32,
            Some(pct) => (100.0 / pct as f64) as u32,
            _ => 100,
        };
        let mut ret = vec![];
        for i in 0..spots {
            ret.push(Self {
                gpu_uuid: gpu_uuid.clone(),
                struct_id: i,
                hardware_memory_mb,
                allotted_mb: mem_size,
                thread_pct,
                gpu_hardware_id,
            })
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
            return Ok((
                missing_or_zero_default(&config.funcs_per_device, 16),
                hardware_memory_mb,
            ));
        }

        let per_func_memory_mb = if config.use_standalone_mps.unwrap_or(false) {
            missing_or_zero_default(&config.per_func_memory_mb, hardware_memory_mb)
        } else if let Some(per_func_memory_mb) = config.per_func_memory_mb {
            per_func_memory_mb
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
        info!(tid=tid, gpu_uuid=%gpu_uuid, spots=spots, hardware_memory_mb=hardware_memory_mb, per_func_memory_mb=per_func_memory_mb, "GPU available spots");
        Ok((spots as u32, per_func_memory_mb))
    }
}

const MPS_CONTAINER_NAME: &str = "iluvatar-mps-daemon";
lazy_static::lazy_static! {
  static ref GPU_RESC_TID: TransactionId = "GPU_RESC_TRACK".to_string();
}
/// Struct that manages GPU control between containers
/// A GPU can only be assigned to one container at a time, and must be returned via [GpuResourceTracker::return_gpu] after container deletion
/// For an invocation to use the GPU, it must have isolation over that resource by acquiring it via [GpuResourceTracker::try_acquire_resource]
pub struct GpuResourceTracker {
    gpus: GpuCollection,
    total_gpu_structs: u32,
    gpu_metadata: MetadataCollection,
    docker: Option<Arc<dyn ContainerIsolationService>>,
    _handle: tokio::task::JoinHandle<()>,
    status_info: RwLock<Vec<GpuStatus>>,
    _container_config: Arc<ContainerResourceConfig>,
    config: Arc<GPUResourceConfig>,
    nvml: Option<Nvml>,
}
impl GpuResourceTracker {
    pub async fn boxed(
        resources: &Option<Arc<GPUResourceConfig>>,
        container_config: &Arc<ContainerResourceConfig>,
        tid: &TransactionId,
        docker: &Option<&Arc<dyn ContainerIsolationService>>,
        status_config: &Arc<crate::worker_api::worker_config::StatusConfig>,
        ring_buff: &Arc<RingBuffer>,
    ) -> Result<Option<Arc<Self>>> {
        if let Some(config) = resources.clone() {
            if config.count == 0 {
                return Ok(None);
            }
            let (gpu_structs, metadata) = Self::prepare_structs(&config, tid)?;
            let mut nvml = None;
            if !iluvatar_library::utils::is_simulation() {
                match config.mig_enabled() {
                    true => Self::enable_mig(tid),
                    false => Self::disable_mig(tid),
                }?;

                if config.mps_enabled() {
                    if docker.is_some() {
                        if let Some(docker) = docker.as_ref().unwrap().as_any().downcast_ref::<DockerIsolation>() {
                            Self::start_mps(&config, docker, tid).await?;
                        } else {
                            bail_error!("MPS is enabled, but isolation service passed was not `docker`");
                        }
                    } else {
                        bail_error!("MPS is enabled, but docker service not present");
                    }
                } else if !config.is_tegra.unwrap_or(false) {
                    Self::set_gpus_shared(tid)?;
                }

                nvml = match Nvml::init() {
                    Ok(n) => Some(n),
                    Err(e) => {
                        if !config.is_tegra.unwrap_or(false) {
                            error!(tid=tid, error=%e, "Error loading NVML");
                        }
                        None
                    },
                };
            }
            let (handle, tx) = tokio_logging_thread(
                missing_or_zero_default(&config.status_update_freq_ms, status_config.report_freq_ms),
                GPU_RESC_TID.to_owned(),
                ring_buff.clone(),
                Self::gpu_utilization,
            )?;
            let mut stat_vec = vec![];
            for struc in gpu_structs.iter() {
                let first = struc
                    .value()
                    .first()
                    .ok_or_else(|| anyhow::format_err!("No GPU structs exist to make beginning status vector"))?;
                stat_vec.push(GpuStatus::new(first.gpu_uuid.clone(), first.hardware_memory_mb, 0));
            }

            let svc = Arc::new(GpuResourceTracker {
                total_gpu_structs: gpu_structs.iter().map(|v| v.len()).sum::<usize>() as u32,
                docker: docker.cloned(),
                _handle: handle,
                status_info: RwLock::new(stat_vec),
                _container_config: container_config.clone(),
                gpus: gpu_structs,
                gpu_metadata: metadata,
                nvml,
                config,
            });
            tx.send(svc.clone())?;
            return Ok(Some(svc));
        }
        Ok(None)
    }

    fn create_concurrency_semaphore(
        config: &Arc<GPUResourceConfig>,
        gpu_hardware_id: InternalGpuId,
        gpus: &[GPU],
        _tid: &TransactionId,
    ) -> Result<Arc<Semaphore>> {
        let gpu_cnt = gpus.len();
        let cnt = missing_or_zero_default(&config.concurrent_running_funcs, gpu_cnt as u32);
        if cnt > gpu_cnt as u32 {
            anyhow::bail!("Value set for the number of concurrently running functions on GPU {} is larger than the number of available GPUs structs", gpu_hardware_id);
        }
        Ok(Arc::new(Semaphore::new(cnt as usize)))
    }

    async fn start_mps(
        gpu_config: &Arc<GPUResourceConfig>,
        docker: &DockerIsolation,
        tid: &TransactionId,
    ) -> Result<()> {
        debug!(tid = tid, "Setting MPS exclusive");
        if !gpu_config.is_tegra.unwrap_or(false) {
            Self::set_gpu_exclusive(tid)?;
        }
        debug!(tid = tid, "Launching MPS container");
        let devices = vec![bollard::models::DeviceRequest {
            driver: Some("".into()),
            count: Some(-1),
            device_ids: None,
            capabilities: Some(vec![vec!["gpu".into()]]),
            options: Some(HashMap::new()),
        }];
        let cfg = bollard::models::HostConfig {
            ipc_mode: Some("host".to_owned()),
            binds: Some(vec!["/tmp/nvidia-mps:/tmp/nvidia-mps".to_owned()]),
            runtime: Some("nvidia".to_owned()),
            device_requests: Some(devices),
            ..Default::default()
        };
        let img_name = "docker.io/nvidia/cuda:11.8.0-base-ubuntu20.04";
        let entrypoint = vec!["/usr/bin/nvidia-cuda-mps-control".to_owned(), "-f".to_owned()];
        docker
            .docker_run(
                tid,
                img_name,
                MPS_CONTAINER_NAME,
                vec![],
                1024,
                1,
                &None,
                None,
                Some(cfg),
                Some(entrypoint),
            )
            .await
    }

    fn list_gpus(tid: &TransactionId) -> Result<Vec<String>> {
        let output = execute_cmd_checked("/usr/bin/nvidia-smi", vec!["-L"], None, tid)?;
        let cow: std::borrow::Cow<'_, str> = String::from_utf8_lossy(&output.stdout);
        Ok(cow
            .split('\n')
            .filter(|str| !str.is_empty() && str.starts_with("GPU "))
            .map(|s| s.to_string())
            .collect::<Vec<String>>())
    }

    /// Applies nvidia-smi args across each GPU ID, appending '"-i", "<ID>"' to the list of args
    fn apply_smi_across_gpus(tid: &TransactionId, args: Vec<&str>) -> Result<()> {
        let mut args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let gpu_cnt = Self::list_gpus(tid)?.len();
        args.push("-i".to_string());
        args.push("0".to_string());
        let len = args.len();
        for i in 0..gpu_cnt {
            args[len - 1] = i.to_string();
            execute_cmd_checked("/usr/bin/nvidia-smi", &args, None, tid)?;
        }
        Ok(())
    }

    /// Enable MIG on all GPUs
    fn enable_mig(tid: &TransactionId) -> Result<()> {
        info!(tid = tid, "Enabling MIG");
        // TODO: a better way to ensure MIG is safely enabled?
        Self::disable_mig(tid)?;
        Self::apply_smi_across_gpus(tid, vec!["-mig", "1"])
    }
    /// Disable MIG on all GPUs
    /// This also deletes all created MIG profiles
    fn disable_mig(tid: &TransactionId) -> Result<()> {
        info!(tid = tid, "Disabling MIG");
        Self::apply_smi_across_gpus(tid, vec!["-mig", "0"])
    }

    /// Set all GPUs to be shared, useful for MPS
    fn set_gpus_shared(tid: &TransactionId) -> Result<()> {
        Self::apply_smi_across_gpus(tid, vec!["-c", "DEFAULT"])
    }
    /// Set all GPUs to EXCLUSIVE_PROCESS, useful for MPS
    fn set_gpu_exclusive(tid: &TransactionId) -> Result<()> {
        Self::apply_smi_across_gpus(tid, vec!["-c", "EXCLUSIVE_PROCESS"])
    }

    /// Return the count of 'physical' GPUs, and list of GPU spot structs
    fn make_simulated_gpus(
        gpu_config: &Arc<GPUResourceConfig>,
        tid: &TransactionId,
    ) -> Result<(GpuCollection, MetadataCollection)> {
        let ret = GpuCollection::new();
        let mut meta = MetadataCollection::new();
        let memory_mb = gpu_config
            .memory_mb
            .ok_or_else(|| anyhow::format_err!("`memory_mb` config must be provided during simulation"))?;
        for gpu_hardware_id in 0..gpu_config.count {
            let gpu_uuid = format!("GPU-{}", gpu_hardware_id);
            let gpu_structs = GPU::split_resources(&gpu_uuid, gpu_hardware_id, memory_mb, gpu_config, tid)?;
            let sem = Self::create_concurrency_semaphore(gpu_config, gpu_hardware_id, &gpu_structs, tid)?;
            let metadata = GpuMetadata {
                gpu_uuid: gpu_uuid.clone(),
                hardware_id: gpu_hardware_id,
                hardware_memory_mb: memory_mb,
                num_structs: gpu_structs.len() as u32,
                max_running: sem.available_permits() as u32,
                sem,
                device_allocated_memory: RwLock::new(0),
            };
            meta.insert(gpu_hardware_id, metadata);
            ret.insert(gpu_hardware_id, gpu_structs);
        }
        Ok((ret, meta))
    }

    /// Return the count of 'physical' GPUs, and list of GPU spot structs
    fn make_real_gpus(
        gpu_config: &Arc<GPUResourceConfig>,
        tid: &TransactionId,
    ) -> Result<(GpuCollection, MetadataCollection)> {
        if gpu_config.mig_enabled() {
            return Self::make_mig_gpus(gpu_config, tid);
        }

        let ret = GpuCollection::new();
        let mut meta = MetadataCollection::new();
        if gpu_config.is_tegra.unwrap_or(false) {
            let gpu_uuid = "tegra_00-0000-0000-0000-dummy_uuid00".to_string();
            let memory_mb: MemSizeMb = 30623;
            let gpu_hardware_id: InternalGpuId = 0;
            let gpu_structs = GPU::split_resources(&gpu_uuid, gpu_hardware_id, memory_mb, gpu_config, tid)?;
            let sem = Self::create_concurrency_semaphore(gpu_config, gpu_hardware_id, &gpu_structs, tid)?;
            let metadata = GpuMetadata::new(gpu_uuid.clone(), gpu_hardware_id, memory_mb, &gpu_structs, sem);
            meta.insert(gpu_hardware_id, metadata);
            ret.insert(gpu_hardware_id, gpu_structs);
            return Ok((ret, meta));
        }

        let output = execute_cmd_checked(
            "/usr/bin/nvidia-smi",
            vec![
                "--query-gpu=gpu_uuid,index,memory.total",
                "--format=csv,noheader,nounits",
            ],
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
                    let gpu_uuid = r
                        .get(0)
                        .ok_or_else(|| anyhow::format_err!("Reading GPU uuid failed from '{:?}'", r))?
                        .to_string();
                    let gpu_hardware_id = r
                        .get(1)
                        .ok_or_else(|| anyhow::format_err!("Reading GPU index failed"))?
                        .parse()
                        .or_else(|e| anyhow::bail!("Failed to parse GPU index {:?} from '{:?}'", e, r))?;
                    let memory_mb = r
                        .get(2)
                        .ok_or_else(|| anyhow::format_err!("Reading GPU memory failed"))?
                        .parse()
                        .or_else(|e| anyhow::bail!("Failed to parse GPU memory.total {:?} from '{:?}'", e, r))?;
                    let structs = GPU::split_resources(&gpu_uuid, gpu_hardware_id, memory_mb, gpu_config, tid)?;
                    let sem = Self::create_concurrency_semaphore(gpu_config, gpu_hardware_id, &structs, tid)?;
                    let metadata = GpuMetadata::new(gpu_uuid.clone(), gpu_hardware_id, memory_mb, &structs, sem);
                    meta.insert(gpu_hardware_id, metadata);
                    ret.insert(gpu_hardware_id, structs);
                },
                Err(e) => bail_error!(tid=tid, error=%e, "Failed to read record from nvidia-smi"),
            }
        }
        Ok((ret, meta))
    }

    fn mig_line_split(line: &str) -> Vec<String> {
        line.split(" ")
            .filter(|str| str != &"MIG" && !str.is_empty())
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
    }
    fn choose_mig_profile(mig_shares: u32, tid: &TransactionId) -> Result<String> {
        let output = execute_cmd_checked(
            "/usr/bin/nvidia-smi",
            vec!["mig", "--list-gpu-instance-profiles"],
            None,
            tid,
        )?;
        let cow: std::borrow::Cow<'_, str> = String::from_utf8_lossy(&output.stdout);
        let mut lines = cow
            .split('\n')
            .filter(|str| !str.is_empty())
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let headers = lines.remove(2).replace(['|'], "");
        let headers = headers
            .split(" ")
            .filter(|str| !str.is_empty())
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let mut chosen_profile = None;
        for line in lines {
            let line = line.replace(['|'], "");
            let mut parts = Self::mig_line_split(&line);
            if parts.len() == headers.len() {
                let instances = parts.remove(3);
                let instances = instances.split('/').next().unwrap();
                let int = instances.parse::<u32>().unwrap();
                if int == mig_shares {
                    let profile_id = parts.remove(2);
                    chosen_profile = Some(profile_id);
                    break;
                }
            }
        }
        match chosen_profile {
            None => bail_error!("No matching MIG profiles fit the desired number of shares, check the documentation."),
            Some(p) => Ok(p),
        }
    }
    fn make_mig_gpus(
        gpu_config: &Arc<GPUResourceConfig>,
        tid: &TransactionId,
    ) -> Result<(GpuCollection, MetadataCollection)> {
        let ret = GpuCollection::new();
        let mut meta = MetadataCollection::new();
        let mig_shares = match gpu_config.mig_shares {
            None => bail_error!("Trying to make MIG shares when value wasn't set"),
            Some(0) => bail_error!("Trying to make MIG shares when value was 0"),
            Some(m) => m,
        };
        let profile = Self::choose_mig_profile(mig_shares, tid)?;
        for _ in 0..mig_shares {
            let _output = execute_cmd_checked(
                "/usr/bin/nvidia-smi",
                vec!["mig", "--create-gpu-instance", profile.as_str(), "-C"],
                None,
                tid,
            )?;
        }
        let output = execute_cmd_checked("/usr/bin/nvidia-smi", vec!["-L"], None, tid)?;
        let cow: std::borrow::Cow<'_, str> = String::from_utf8_lossy(&output.stdout);
        let migs = cow
            .split('\n')
            .filter(|str| str.starts_with("  MIG "))
            .collect::<Vec<&str>>();
        for (gpu_hardware_id, line) in migs.iter().enumerate() {
            let gpu_hardware_id = gpu_hardware_id as InternalGpuId;
            let parts = line.split(' ').filter(|str| !str.is_empty()).collect::<Vec<&str>>();
            let gb = parts[1].split('.').collect::<Vec<&str>>()[1];
            let memory_mb = &gb[0..gb.len() - 2].parse::<MemSizeMb>().unwrap() * 1024;
            let uuid = parts[parts.len() - 1];
            let uuid = uuid[0..uuid.len() - 1].to_string();
            let structs = GPU::split_resources(&uuid, gpu_hardware_id as InternalGpuId, memory_mb, gpu_config, tid)?;
            let sem = Self::create_concurrency_semaphore(gpu_config, gpu_hardware_id as InternalGpuId, &structs, tid)?;
            let metadata = GpuMetadata::new(uuid, gpu_hardware_id, memory_mb, &structs, sem);
            meta.insert(gpu_hardware_id, metadata);
            ret.insert(gpu_hardware_id, structs);
        }
        Ok((ret, meta))
    }

    fn prepare_structs(
        gpu_config: &Arc<GPUResourceConfig>,
        tid: &TransactionId,
    ) -> Result<(GpuCollection, MetadataCollection)> {
        if gpu_config.count == 0 {
            info!(tid = tid, "GPU config had 0 GPUs, skipping GPU resource setup");
            return Ok((GpuCollection::new(), MetadataCollection::new()));
        }
        let (structs, metadata) = if iluvatar_library::utils::is_simulation() {
            Self::make_simulated_gpus(gpu_config, tid)?
        } else {
            Self::make_real_gpus(gpu_config, tid)?
        };
        let found_physical = structs.len();
        let expected = missing_or_zero_default(&gpu_config.mig_shares, 1) * gpu_config.count;
        if found_physical as u32 != expected {
            anyhow::bail!(
                "Was able to prepare {} GPUs, but configuration expected {}",
                found_physical,
                expected
            );
        }
        info!(tid=tid, gpus=?structs, "GPUs prepared");
        Ok((structs, metadata))
    }

    /// Return a permit access to run on the given GPU
    /// If [gpu] is [None], then this will return a token for the least loaded GPU
    /// Returns an error if none are available for execution
    pub fn try_acquire_resource(
        self: &Arc<Self>,
        gpu: Option<&GPU>,
        tid: &TransactionId,
    ) -> Result<GpuToken, tokio::sync::TryAcquireError> {
        let limit = missing_or_zero_default(&self.config.limit_on_utilization, 0);
        match gpu {
            Some(gpu) => {
                let gpu_hardware_id = gpu.gpu_hardware_id;
                if limit == 0 {
                    return match self.gpu_metadata.get(&gpu_hardware_id) {
                        Some(val) => match val.sem.clone().try_acquire_many_owned(1) {
                            Ok(p) => {
                                self.status_info.write()[gpu_hardware_id as usize].num_running += 1;
                                Ok(GpuToken::new(p, gpu_hardware_id, tid.clone(), self))
                            },
                            Err(e) => Err(e),
                        },
                        None => {
                            error!(tid=tid, uuid=%gpu.gpu_uuid, private_id=gpu_hardware_id, "Tried to acquire permit for unknown GPU");
                            Err(tokio::sync::TryAcquireError::NoPermits)
                        },
                    };
                }
                let gpu_stat = self.status_info.read();
                if gpu_stat.len() > gpu_hardware_id as usize {
                    if gpu_stat[gpu_hardware_id as usize].est_utilization_gpu <= limit as f64 {
                        drop(gpu_stat);
                        return match self.gpu_metadata.get(&gpu_hardware_id) {
                            Some(val) => match val.sem.clone().try_acquire_many_owned(1) {
                                Ok(p) => {
                                    let mut gpu_stat = self.status_info.write();
                                    let stat = &mut gpu_stat[gpu_hardware_id as usize];
                                    stat.est_utilization_gpu += if stat.num_running > 0 {
                                        stat.est_utilization_gpu / stat.num_running as f64
                                    } else {
                                        50.0
                                    };
                                    stat.num_running += 1;
                                    Ok(GpuToken::new(p, gpu_hardware_id, tid.clone(), self))
                                },
                                Err(e) => Err(e),
                            },
                            None => {
                                error!(tid=tid, uuid=%gpu.gpu_uuid, private_id=gpu_hardware_id, "Tried to acquire permit for unknown GPU");
                                Err(tokio::sync::TryAcquireError::NoPermits)
                            },
                        };
                    }
                } else {
                    warn!(private_id=gpu_hardware_id, stat=?gpu_stat, "GPU id not found");
                }
                Err(tokio::sync::TryAcquireError::NoPermits)
            },
            None => self.try_acquire_least_loaded_resource(tid, limit),
        }
    }

    fn try_acquire_least_loaded_resource(
        self: &Arc<Self>,
        tid: &TransactionId,
        limit: u32,
    ) -> Result<GpuToken, tokio::sync::TryAcquireError> {
        if limit == 0 {
            let mut gpu_hardware_id = 0;
            let mut available = 0;
            for (_, meta) in self.gpu_metadata.iter() {
                let avil = meta.sem.available_permits();
                if avil >= available {
                    available = avil;
                    gpu_hardware_id = meta.hardware_id;
                }
            }
            match self.gpu_metadata.get(&gpu_hardware_id) {
                Some(val) => match val.sem.clone().try_acquire_many_owned(1) {
                    Ok(p) => {
                        self.status_info.write()[gpu_hardware_id as usize].num_running += 1;
                        Ok(GpuToken::new(p, gpu_hardware_id, tid.clone(), self))
                    },
                    Err(e) => Err(e),
                },
                None => Err(tokio::sync::TryAcquireError::NoPermits),
            }
        } else {
            let limit = limit as f64;
            let gpu_stat = self.status_info.read();
            let mut min_util = 101.0;
            let mut gpu_hardware_id = None;

            for (i, stat) in gpu_stat.iter().enumerate() {
                if stat.est_utilization_gpu <= limit && stat.est_utilization_gpu <= min_util {
                    gpu_hardware_id = Some(i);
                    min_util = stat.est_utilization_gpu;
                }
            }
            drop(gpu_stat);
            if let Some(gpu_hardware_id) = gpu_hardware_id {
                match self.gpu_metadata.get(&(gpu_hardware_id as u32)) {
                    Some(val) => match val.sem.clone().try_acquire_many_owned(1) {
                        Ok(p) => {
                            let mut gpu_stat = self.status_info.write();
                            let stat: &mut GpuStatus = &mut gpu_stat[gpu_hardware_id];
                            stat.est_utilization_gpu += if stat.num_running > 0 {
                                stat.est_utilization_gpu / stat.num_running as f64
                            } else {
                                50.0
                            };
                            stat.num_running += 1;
                            Ok(GpuToken::new(p, gpu_hardware_id as u32, tid.clone(), self))
                        },
                        Err(e) => Err(e),
                    },
                    None => {
                        error!(
                            tid = tid,
                            private_id = gpu_hardware_id,
                            "Tried to acquire permit for unknown GPU"
                        );
                        Err(tokio::sync::TryAcquireError::NoPermits)
                    },
                }
            } else {
                Err(tokio::sync::TryAcquireError::NoPermits)
            }
        }
    }

    /// Call on dropping a GPU execute token
    /// reduces the est_utilization_gpu for that GPU immediately (w/o querying which is slow) to make room for another function
    fn drop_gpu_resource(&self, gpu_id: InternalGpuId) {
        let mut gpu_stat = self.status_info.write();
        let stat: &mut GpuStatus = &mut gpu_stat[gpu_id as usize];
        stat.est_utilization_gpu = if stat.num_running > 0 {
            stat.num_running -= 1;
            (stat.est_utilization_gpu / (stat.num_running + 1) as f64) * stat.num_running as f64
        } else {
            0.0
        };
    }

    pub fn outstanding(&self) -> u32 {
        self.total_gpu_structs
            - self
                .gpu_metadata
                .iter()
                .map(|item| item.1.sem.available_permits() as u32)
                .sum::<u32>()
    }

    pub fn total_gpus(&self) -> u32 {
        self.total_gpu_structs
    }

    pub fn physical_gpus(&self) -> u32 {
        self.gpus.len() as u32
    }
    pub fn max_concurrency(&self) -> u32 {
        self.gpu_metadata.iter().map(|g| g.1.max_running).sum::<u32>()
    }

    /// Acquire a GPU so it can be attached to a container.
    /// Returns a pointer to the least-loaded GPU
    /// [None] means no GPU is available.
    pub fn acquire_gpu(self: &Arc<Self>, tid: &TransactionId) -> Option<GPU> {
        let mut best_idx = 0;
        let mut available = 0;
        for item in self.gpus.iter() {
            let avil = item.value().len();
            if avil > available {
                available = avil;
                best_idx = *item.key();
            }
        }

        let gpu = self.gpus.get_mut(&best_idx)?.pop();
        if let Some(g) = &gpu {
            debug!(
                tid = tid,
                gpu_uuid = g.gpu_uuid,
                struct_id = g.struct_id,
                "GPU allocating"
            );
        }
        gpu
    }

    /// Return a GPU that has been removed from a container
    pub fn return_gpu(&self, gpu: GPU, tid: &TransactionId) {
        debug!(
            tid = tid,
            gpu_uuid = gpu.gpu_uuid,
            struct_id = gpu.struct_id,
            hardware_id = gpu.gpu_hardware_id,
            "GPU returned"
        );
        match self.gpus.get_mut(&gpu.gpu_hardware_id) {
            Some(mut v) => {
                #[cfg(debug_assertions)]
                for cont in v.iter() {
                    if cont.gpu_uuid == gpu.gpu_uuid
                        && cont.struct_id == gpu.struct_id
                        && cont.gpu_hardware_id == gpu.gpu_hardware_id
                    {
                        error!(
                            tid = tid,
                            gpu_uuid = gpu.gpu_uuid,
                            struct_id = gpu.struct_id,
                            hardware_id = gpu.gpu_hardware_id,
                            "Tried to return GPU twice"
                        );
                        break;
                    }
                }
                v.push(gpu)
            },
            None => {
                let keys = self
                    .gpus
                    .iter()
                    .map(|i| *i.key())
                    .map(|i| i.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");
                error!(
                    tid = tid,
                    keys = keys,
                    gpu_uuid = gpu.gpu_uuid,
                    struct_id = gpu.struct_id,
                    hardware_id = gpu.gpu_hardware_id,
                    "Tried to return illegal GPU"
                )
            },
        }
    }

    /// get the utilization of GPUs on the system
    #[tracing::instrument(level = "debug", skip_all)]
    async fn smi_gpu_utilization(&self, tid: &TransactionId) -> Result<Vec<GpuStatus>> {
        let mut ret: Vec<GpuStatus> = vec![];
        if !std::path::Path::new("/usr/bin/nvidia-smi").exists() {
            trace!(tid = tid, "nvidia-smi not found, not checking GPU utilization");
            return Ok(ret);
        }
        let args = vec![
            "--query-gpu=gpu_uuid,pstate,memory.total,memory.used,utilization.gpu,utilization.memory,power.draw,power.limit",
            "--format=csv,noheader,nounits",
        ];
        let nvidia = match execute_cmd_checked_async("/usr/bin/nvidia-smi", args, None, tid).await {
            Ok(r) => r,
            Err(e) => {
                error!(tid=tid, error=%e, "Failed to call nvidia-smi");
                return Err(e);
            },
        };
        let is_empty = (*self.status_info.read()).is_empty();
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .trim(csv::Trim::All)
            .from_reader(nvidia.stdout.as_slice());
        for record in rdr.deserialize::<GpuParseStatus>() {
            match record {
                Ok(rec) => {
                    if is_empty {
                        ret.push(rec.into());
                    } else {
                        let mut lck = self.status_info.write();
                        for (i, stat) in lck.iter_mut().enumerate() {
                            if stat.gpu_uuid == rec.gpu_uuid {
                                let (running, tracked_mem) = if let Some(meta) = self.gpu_metadata.get(&(i as u32)) {
                                    (
                                        meta.max_running - meta.sem.available_permits() as u32,
                                        *meta.device_allocated_memory.read(),
                                    )
                                } else {
                                    (0, 0)
                                };
                                stat.update(rec, running, tracked_mem);
                                ret.push(stat.clone());
                                break;
                            }
                        }
                    }
                },
                Err(e) => {
                    let stdout = String::from_utf8_lossy(&nvidia.stdout);
                    error!(tid=tid, error=%e, stdout=%stdout, "Failed to deserialized GPU record from nvidia-smi")
                },
            }
        }
        Ok(ret)
    }

    #[cfg(target_os = "linux")]
    #[tracing::instrument(level = "debug", skip_all)]
    async fn nvml_gpu_utilization(&self, nvml: &Nvml, _tid: &TransactionId) -> Result<Vec<GpuStatus>, NvmlError> {
        let is_empty = (*self.status_info.read()).is_empty();
        let mut ret: Vec<GpuStatus> = vec![];
        let dev_count = nvml.device_count()?;

        for i in 0..dev_count {
            let device = nvml.device_by_index(i)?;

            let utilization = device.utilization_rates()?;
            let memory = device.memory_info()?;

            let stat = GpuParseStatus {
                gpu_uuid: device.uuid()?,
                memory_total: (memory.total / (1024 * 1024)) as u32,
                memory_used: (memory.used / (1024 * 1024)) as u32,
                utilization_gpu: utilization.gpu as f64,
                utilization_memory: utilization.memory as f64,
                power_draw: device.power_usage()? as f64 / 1000.0,
                power_limit: device.enforced_power_limit()? as f64 / 1000.0,
                pstate: device.performance_state()?.into(),
            };
            if is_empty {
                ret.push(stat.into());
            } else {
                let mut lck = self.status_info.write();
                if lck.len() > i as usize {
                    let (running, tracked_mem) = if let Some(meta) = self.gpu_metadata.get(&i) {
                        (
                            meta.max_running - meta.sem.available_permits() as u32,
                            *meta.device_allocated_memory.read(),
                        )
                    } else {
                        (0, 0)
                    };
                    lck[i as usize].update(stat, running, tracked_mem);
                    ret.push(lck[i as usize].clone());
                }
            }
        }
        Ok(ret)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn simulation_gpu_util(&self, _tid: &TransactionId) -> Result<Vec<GpuStatus>> {
        let mut status: Vec<GpuStatus> = vec![];
        // TODO: proper GPU utilization
        for (_gpu_id, metadata) in self.gpu_metadata.iter() {
            status.push(GpuStatus::new(
                metadata.gpu_uuid.clone(),
                metadata.hardware_memory_mb,
                metadata.max_running - metadata.sem.available_permits() as u32,
            ));
        }
        Ok(status)
    }

    /// get the utilization of GPUs on the system
    #[tracing::instrument(level = "debug", skip_all)]
    async fn gpu_utilization(self: &Arc<Self>, tid: &TransactionId) -> Result<GpuStatVec> {
        let status = if iluvatar_library::utils::is_simulation() {
            self.simulation_gpu_util(tid).await
        } else if let Some(nvml) = &self.nvml {
            match self.nvml_gpu_utilization(nvml, tid).await {
                Ok(s) => Ok(s),
                Err(e) => {
                    error!(tid=tid, error=%e, "Error using NVML to query device utilization");
                    self.smi_gpu_utilization(tid).await
                },
            }
        } else {
            self.smi_gpu_utilization(tid).await
        }?;
        *self.status_info.write() = status.clone();
        let r = GpuStatVec(status);
        info!(tid=tid, gpu_util=%r, "GPU status");
        Ok(r)
    }

    /// get the utilization of GPUs on the system
    pub fn gpu_status(&self, _tid: &TransactionId) -> Vec<GpuStatus> {
        (*self.status_info.read()).clone()
    }

    pub fn update_mem_usage(&self, gpu: &GPU, amt: MemSizeMb) {
        if let Some(meta) = self.gpu_metadata.get(&gpu.gpu_hardware_id) {
            debug!(
                gpu_id = gpu.struct_id,
                mem_diff = amt,
                curr_used = *meta.device_allocated_memory.write(),
                "updating device memory usage"
            );
            *meta.device_allocated_memory.write() += amt;
        }
    }
    pub fn get_free_mem_by_id(&self, gpu_hardware_id: InternalGpuId) -> MemSizeMb {
        match self.gpu_metadata.get(&gpu_hardware_id) {
            Some(meta) => {
                let free = meta.hardware_memory_mb - *meta.device_allocated_memory.read();
                debug!(
                    hardware = meta.hardware_memory_mb,
                    tracked = *meta.device_allocated_memory.read(),
                    free = free,
                    "Tracked GPU mem alloc get_free_mem"
                );
                meta.hardware_memory_mb - *meta.device_allocated_memory.read()
            },
            None => 0,
        }
    }

    pub fn get_free_mem(&self, gpu: &GPU) -> MemSizeMb {
        self.get_free_mem_by_id(gpu.gpu_hardware_id)
    }
}
impl Drop for GpuResourceTracker {
    fn drop(&mut self) {
        let tid: TransactionId = GPU_RESC_TID.clone();
        if let Some(d) = &self.docker {
            if self.config.mps_enabled() {
                if let Some(docker) = d.as_any().downcast_ref::<DockerIsolation>() {
                    let (h, _rt) = match tokio::runtime::Handle::try_current() {
                        Ok(h) => (h, None),
                        Err(_) => match iluvatar_library::tokio_utils::build_tokio_runtime(&None, &None, &None, &tid) {
                            Ok(rt) => (rt.handle().clone(), Some(rt)),
                            Err(e) => {
                                error!(error=%e, tid=tid, "Failed to create runtime");
                                return;
                            },
                        },
                    };
                    match h.block_on(docker.get_logs(MPS_CONTAINER_NAME, &tid)) {
                        Ok((stdout, stderr)) => info!(stdout=%stdout, stderr=%stderr, tid=tid, "MPS daemon exit logs"),
                        Err(e) => error!(error=%e, tid=tid, "Failed to get MPS daemon logs"),
                    }
                }
            }
        }
        let _ = Self::disable_mig(&tid).map_err(|e| error!(error=%e, tid=tid, "Failed to disable MIG on GPUs"));
        let _ = Self::set_gpus_shared(&tid).map_err(|e| error!(error=%e, tid=tid, "Failed to set shared on GPUs"));
    }
}

pub struct GpuToken {
    _token: OwnedSemaphorePermit,
    pub gpu_id: InternalGpuId,
    tid: TransactionId,
    svc: Arc<GpuResourceTracker>,
}
impl GpuToken {
    pub fn new(
        token: OwnedSemaphorePermit,
        gpu_id: InternalGpuId,
        tid: TransactionId,
        svc: &Arc<GpuResourceTracker>,
    ) -> Self {
        Self {
            _token: token,
            gpu_id,
            tid,
            svc: svc.clone(),
        }
    }
}
impl Drop for GpuToken {
    fn drop(&mut self) {
        self.svc.drop_gpu_resource(self.gpu_id);
        debug!(tid = self.tid, gpu = self.gpu_id, "Dropping GPU token");
    }
}
impl iluvatar_library::types::DroppableMovableTrait for GpuToken {}
impl From<GpuToken> for DroppableToken {
    fn from(val: GpuToken) -> Self {
        Box::new(val)
    }
}
