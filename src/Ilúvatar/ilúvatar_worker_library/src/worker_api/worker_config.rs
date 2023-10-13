use crate::services::invocation::queueing::EnqueueingPolicy;
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
    /// full URL to access the controller/load balancer, required for worker registration
    pub load_balancer_url: String,
    /// Optional because energy monitoring is not required
    pub energy: Option<Arc<EnergyConfig>>,
    pub invocation: Arc<InvocationConfig>,
    /// Energy cap disabled if not present
    #[cfg(feature = "power_cap")]
    pub energy_cap: Option<Arc<crate::services::invocation::energy_limiter::EnergyCapConfig>>,
    pub status: Arc<StatusConfig>,
    pub influx: Option<Arc<InfluxConfig>>,
}

#[derive(Debug, Deserialize)]
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

    /// Settings for the different compute resources the worker can use
    pub resource_map: HashMap<ComputeEnum, Arc<ComputeResourceConfig>>,
}
#[derive(Debug, Deserialize)]
/// Configuration detailing a single type of compute
pub struct ComputeResourceConfig {
    /// number of cores it can use, i.e. number of concurrent functions allowed at once
    /// If this is set to 0, then allocations of the resource will not be managed.
    /// Depending on resource type, will not be allowed (i.e. GPU must have exact number)
    pub count: u32,
    /// If provided and greated than [Self::count], the resource will be over-subscribed
    /// Not all resources support oversubscription
    pub max_oversubscribe: Option<u32>,
    /// Frequency at which to check the system load and optionally increase the allowed invocation concurrency.
    /// Used with [Self::max_oversubscribe] and cannot be 0
    pub concurrency_update_check_ms: Option<u64>,
    /// The maximum normalized load average before reducing concurrency.
    /// Used with [Self::max_oversubscribe] and cannot be 0
    pub max_load: Option<f64>,
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
    /// Queueing policy to use for different compute resources
    pub queue_policies: HashMap<ComputeEnum, String>,
    /// The policy by which the worker decides how to enqueue polymorphic functions
    /// By default it uses [EnqueueingPolicy::All]
    pub enqueueing_policy: Option<EnqueueingPolicy>,
    /// If present and not zero, invocations with an execution duration less than this
    ///   will bypass concurrency restrictions and be run immediately
    pub bypass_duration_ms: Option<u64>,
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
    /// number of free namspaces to keep in the pool
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
    pub fn new(config_fpath: &Option<&String>, overrides: Option<Vec<(String, String)>>) -> anyhow::Result<Self> {
        let mut sources = vec!["worker/src/worker.json", "worker/src/worker.dev.json"];
        if let Some(config_fpath) = config_fpath {
            sources.push(config_fpath.as_str());
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
                    }
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
        config_fpath: &Option<&String>,
        overrides: Option<Vec<(String, String)>>,
    ) -> anyhow::Result<WorkerConfig> {
        Ok(Arc::new(Configuration::new(config_fpath, overrides)?))
    }
}
