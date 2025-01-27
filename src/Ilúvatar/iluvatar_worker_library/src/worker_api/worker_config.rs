use crate::services::invocation::dispatching::greedy_weight::GreedyWeightConfig;
use crate::services::invocation::dispatching::{landlord::LandlordConfig, EnqueueingPolicy};
use crate::services::{containers::docker::DockerConfig, invocation::queueing::gpu_mqfq::MqfqConfig};
use config::{Config, File};
use iluvatar_library::{
    energy::EnergyConfig,
    influx::InfluxConfig,
    logging::LoggingConfig,
    types::{ComputeEnum, MemSizeMb},
    utils::port_utils::Port,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct Configuration {
    /// name for the server
    pub name: String,
    /// address to listen on
    pub address: String,
    /// port to listen on
    pub port: Port,
    /// request timeout length in seconds
    pub timeout_sec: u64,
    /// See documentation [here](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.event_interval) for details
    pub tokio_event_interval: u32,
    /// See documentation [here](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.global_queue_interval) for details
    pub tokio_queue_interval: u32,
    /// Restrictions on functions on registration
    pub limits: Arc<FunctionLimits>,
    pub logging: Arc<LoggingConfig>,
    /// Settings for handling container networking connections
    /// Optional because not all isolation backends require it
    pub networking: Option<Arc<NetworkingConfig>>,
    pub container_resources: Arc<ContainerResourceConfig>,
    /// full URL to access the controller/load balancer, required for worker registration.
    /// If missing or empty, the worker will not try and register with the controller.
    /// If registration fails, the worker will exit.
    pub load_balancer_host: Option<String>,
    pub load_balancer_port: Option<Port>,
    /// Optional because energy monitoring is not required
    pub energy: Option<Arc<EnergyConfig>>,
    pub invocation: Arc<InvocationConfig>,
    /// Energy cap disabled if not present
    #[cfg(feature = "power_cap")]
    pub energy_cap: Option<Arc<crate::services::invocation::energy_limiter::EnergyCapConfig>>,
    pub status: Arc<StatusConfig>,
    pub influx: Option<Arc<InfluxConfig>>,
}

#[derive(Debug, Deserialize, Default)]
/// total resources the worker is allowed to allocate to containers
pub struct ContainerResourceConfig {
    /// total memory pool in MB
    pub memory_mb: MemSizeMb,
    /// eviction algorithm to use
    pub eviction: String,
    /// timeout on container startup before error
    pub startup_timeout_ms: u64,
    /// amount of memory the container pool monitor will try and maintain as a buffer (eager eviction)
    pub memory_buffer_mb: MemSizeMb,
    /// how often the container pool monitor will run, in milliseconds
    pub pool_freq_ms: u64,
    /// the snapshotter to use with containerd (if relevant)
    /// Supported ones are [here](https://github.com/containerd/containerd/tree/main/docs/snapshotters)
    ///   WARNING: using 'overlayfs' can cause race conditions on process startup inside a container before all files are available
    pub snapshotter: String,
    /// The max number of containers allowed to be created concurrently
    ///   Calls to containerd can become extremely delayed if too many happen at once, ~10
    ///   If 0 then the concurrency is unlimited
    pub concurrent_creation: u32,

    /// Configuration to be passed to Docker
    /// Currently this is also passed to Containerd for repository authentication
    pub docker_config: Option<DockerConfig>,
    /// Settings for the CPU compute resources the worker can use
    pub cpu_resource: Arc<CPUResourceConfig>,
    /// Settings for the CPU compute resources the worker can use
    pub gpu_resource: Option<Arc<GPUResourceConfig>>,
}
#[derive(Debug, Deserialize, Default)]
/// Configuration detailing a single type of compute.
pub struct CPUResourceConfig {
    /// number of cores it can use, i.e. number of concurrent functions allowed at once
    /// If this is set to 0, then allocations of CPUs will not be limited
    pub count: u32,
    /// If provided and greater than [Self::count], the resource will be over-subscribed to that limit
    pub max_oversubscribe: Option<u32>,
    /// Frequency at which to check the system load and optionally increase the allowed invocation concurrency.
    /// Used with [Self::max_oversubscribe], disabled if 0
    pub concurrency_update_check_ms: Option<u64>,
    /// The maximum normalized load average before reducing concurrency.
    /// Used with [Self::max_oversubscribe] and cannot be 0.
    /// Ilúvatar assumes that it will be the only program running on the system with this enabled, and has access to all CPUs.
    pub max_load: Option<f64>,
}
#[derive(Debug, Deserialize, Default)]
/// Configuration detailing a single type of compute
pub struct GPUResourceConfig {
    /// Number of GPU devices it can use, i.e. number of concurrent functions allowed at once.
    /// If this is set to 0, then GPUs will not be used by the worker
    pub count: u32,
    /// The amount of physical memory each GPU has.
    /// Used for simulations
    pub memory_mb: Option<MemSizeMb>,
    /// How often to update GPU resource usage status
    /// Maybe be delayed if update takes longer than freq
    pub status_update_freq_ms: Option<u64>,
    /// Set up a standalone MPS daemon to control GPU access.
    pub use_standalone_mps: Option<bool>,
    /// How much physical memory each function is 'allocated' on the GPU.
    /// Allows (hardware / this) number of items on GPU to have access to the GPU at once.
    /// If missing, entire GPU is allocated to function.
    pub per_func_memory_mb: Option<MemSizeMb>,
    /// Use [CUDA_MPS_ACTIVE_THREAD_PERCENTAGE](https://docs.nvidia.com/deploy/mps/index.html#topic_5_2_5) in proportion to GPU memory allocation.
    pub mps_limit_active_threads: Option<bool>,
    /// Enable driver hook library to force unified memory in function.
    /// Must also pass [Self::funcs_per_device] as greater than 0
    pub use_driver_hook: Option<bool>,
    /// Maximum number of function containers to allow on device when using driver hook.
    /// Must also pass [Self::use_driver_hook] as true, defaults to 16 (maximum pre-volta supported MPS clients)
    pub funcs_per_device: Option<u32>,
    /// Maximum number of functions to run concurrently on GPU
    /// Number is per-GPU
    /// If empty, defaults to [Self::funcs_per_device]
    pub concurrent_running_funcs: Option<u32>,
    pub prefetch_memory: Option<bool>,
    /// Monitor GPU utilization as a means of limiting when additional items can be run on a device
    /// If present and > 0, this is additional to limiting the number of running functions per device
    /// Otherwise, does nothing
    pub limit_on_utilization: Option<u32>,
    /// Tegra Platform - avoids use to nvidia-smi.
    pub is_tegra: Option<bool>,
}

impl GPUResourceConfig {
    /// Returns true if MPS (of any sort) is enabled
    pub fn mps_enabled(&self) -> bool {
        self.use_standalone_mps.unwrap_or(false)
    }
    /// Returns true if using driver hook is enabled
    pub fn driver_hook_enabled(&self) -> bool {
        self.use_driver_hook.unwrap_or(false)
    }
    /// Returns true if sending memory hints is enabled
    pub fn send_driver_memory_hints(&self) -> bool {
        self.driver_hook_enabled() && self.prefetch_memory.unwrap_or(false)
    }
}

#[derive(Debug, Deserialize)]
/// limits to place on an individual invocation
pub struct FunctionLimits {
    /// minimum memory allocation allowed
    pub mem_min_mb: MemSizeMb,
    /// maximum memory allocation allowed
    pub mem_max_mb: MemSizeMb,
    /// maximum cpu allocation allowed
    pub cpu_max: u32,
    /// invocation length timeout
    pub timeout_sec: u64,
}

#[derive(Debug, Deserialize)]
/// Internal knobs for how the [crate::services::invocation::InvokerFactory], and types it creates, work
pub struct InvocationConfig {
    /// number of retries before giving up on an invocation
    /// Setting to 0 means no retries
    pub retries: u32,
    /// Duration in milliseconds the worker queue will sleep between checking for new invocations
    pub queue_sleep_ms: u64,
    /// Queue to use for different compute resources
    pub queues: HashMap<ComputeEnum, String>,
    /// Queueing policy to use for different compute resources
    pub queue_policies: HashMap<ComputeEnum, String>,
    /// The policy by which the worker decides how to enqueue polymorphic functions
    /// By default it uses [EnqueueingPolicy::All]
    pub enqueueing_policy: Option<EnqueueingPolicy>,
    pub enqueuing_log_details: Option<bool>,
    pub speedup_ratio: Option<f64>,
    /// If present and not zero, invocations with an execution duration less than this
    ///   will bypass concurrency restrictions and be run immediately
    pub bypass_duration_ms: Option<u64>,
    pub mqfq_config: Option<Arc<MqfqConfig>>,
    pub landlord_config: Option<Arc<LandlordConfig>>,
    pub greedy_weight_config: Option<Arc<GreedyWeightConfig>>,
}
impl InvocationConfig {
    pub fn log_details(&self) -> bool {
        self.enqueuing_log_details.unwrap_or(false)
    }
}

#[derive(Debug, Deserialize)]
/// Networking details to connect containers to the network
pub struct NetworkingConfig {
    /// bridge name to create
    pub bridge: String,
    /// path to cnitool executable
    pub cnitool: String,
    /// directory with cnitool plugins
    pub cni_plugin_bin: String,
    /// name of cni json file for bridge setup
    pub cni_name: String,
    /// use a pool of network namespaces
    pub use_pool: bool,
    /// number of free namespaces to keep in the pool
    pub pool_size: usize,
    /// frequency of namespace pool monitor runs, in milliseconds
    pub pool_freq_ms: u64,
    /// network interface to attach bridge to
    pub hardware_interface: String,
}

#[derive(Debug, Deserialize)]
/// Config related to status monitoring of the worker system & host
pub struct StatusConfig {
    pub report_freq_ms: u64,
}

/// A wrapper type for the loaded global worker configuration
pub type WorkerConfig = Arc<Configuration>;

impl Configuration {
    pub fn new(config_fpath: &Option<&str>, overrides: Option<Vec<(String, String)>>) -> anyhow::Result<Self> {
        let mut sources = vec!["worker/src/worker.json", "worker/src/worker.dev.json"];
        if let Some(config_fpath) = config_fpath {
            sources.push(config_fpath);
        }
        let mut s = Config::builder()
            .add_source(
                sources
                    .iter()
                    .filter(|path| std::path::Path::new(&path).exists())
                    .map(|path| File::with_name(path))
                    .collect::<Vec<_>>(),
            )
            .add_source(
                config::Environment::with_prefix("ILUVATAR_WORKER")
                    .try_parsing(true)
                    .separator("__"),
            );
        if let Some(overrides) = overrides {
            for (k, v) in overrides {
                s = match s.set_override(&k, v.clone()) {
                    Ok(s) => s,
                    Err(e) => {
                        anyhow::bail!("Failed to set override '{}' to '{}' because {}", k, v, e)
                    },
                };
            }
        }
        match s.build() {
            Ok(s) => match s.try_deserialize() {
                Ok(cfg) => Ok(cfg),
                Err(e) => anyhow::bail!("Failed to deserialize configuration because '{}'", e),
            },
            Err(e) => anyhow::bail!("Failed to build configuration because '{}'", e),
        }
    }

    pub fn boxed(
        config_fpath: &Option<&str>,
        overrides: Option<Vec<(String, String)>>,
    ) -> anyhow::Result<WorkerConfig> {
        Ok(Arc::new(Configuration::new(config_fpath, overrides)?))
    }
}
