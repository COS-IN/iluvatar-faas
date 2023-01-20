use std::sync::Arc;
use iluvatar_library::energy::EnergyConfig;
use iluvatar_library::{types::MemSizeMb, utils::port_utils::Port, logging::LoggingConfig};
use iluvatar_library::graphite::GraphiteConfig;
use serde::Deserialize;
use config::{Config, ConfigError, File};

#[derive(Debug, Deserialize)]
#[allow(unused)]
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
  pub limits: Arc<FunctionLimits>,
  pub logging: Arc<LoggingConfig>,
  pub networking: Arc<NetworkingConfig>,
  pub container_resources: Arc<ContainerResources>,
  /// full URL to access the controller/load balancer, required for worker registration
  pub load_balancer_url: String,
  pub graphite: Arc<GraphiteConfig>,
  pub energy: Arc<EnergyConfig>,
  pub invocation: Arc<InvocationConfig>,
  pub status: Arc<StatusConfig>,
}

#[derive(Debug, Deserialize)]
/// total resources the worker is allowed to allocate to conainers
pub struct ContainerResources {
  /// total memory pool in MB
  pub memory_mb: MemSizeMb,
  /// number of cores it can use, i.e. number of concurrent functions allowed at once
  /// If this is set to 0, then core allocations will not be managed
  pub cores: u32,
  /// eviciton algorithm to use
  pub eviction: String,
  /// timeout on container startup before error
  pub startup_timeout_ms: u64,
  /// amount of memory the container pool monitor will try and maintain as a buffer (eager eviction)
  pub memory_buffer_mb: MemSizeMb,
  /// how often the container pool monitor will run, in milliseconds
  pub pool_freq_ms: u64,
  /// container backend to use: 
  /// containerd, docker (not implemented yet)
  pub backend: String,
  /// the snapshotter to use with containerd (if relevant)
  /// Supported ones are [here](https://github.com/containerd/containerd/tree/main/docs/snapshotters)
  ///   WARNING: using 'overlayfs' can cause race conditions on process startup inside a container before all files are available
  pub snapshotter: String,
  /// The max number of containers allowed to be created concurrently
  ///   Calls to containerd can become extremely delayed if too many happen at once, ~10 
  ///   If 0 then the concurreny is unlimited
  pub concurrent_creation: u32,
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
  /// Queueing policy to use.
  /// Cerrently implemented are [none (no queue), fcfs (first come first serve), ]
  pub queue_policy: String,
  /// The number of concurrent invocations allowed on the worker
  /// If this number is hit, items will be left in the queue until work completes
  /// Not affected by other resource limitations
  pub concurrent_invokes: u32,
  /// If not zero, invocations with an execution duration less than this
  ///   will bypass concurrency restrictions and be run immediately
  pub bypass_duration_ms: u64,
  /// Frequency at which to check the system load and optionally increase the allowed invocation concurreny.
  /// Used with [AvailableScalingInvoker] and cannot be 0
  pub concurrency_udpate_check_ms: Option<u64>,
  /// The maximum allowable concurrency. 
  /// Used with [AvailableScalingInvoker] and cannot be 0
  pub max_concurrency: Option<u64>,
  /// The maximum normalized load average before reducing concurrency. 
  /// Used with [AvailableScalingInvoker] and cannot be 0
  pub max_load: Option<f64>,
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
  pub report_freq_ms: u64
}

pub type WorkerConfig = Arc<Configuration>;

impl Configuration {
  pub fn new(cleaning: bool, config_fpath: &String) -> Result<Self, ConfigError> {
    let sources = vec!["worker/src/worker.json", "worker/src/worker.dev.json", config_fpath.as_str()];
    let s = Config::builder()
      .add_source(
        sources.iter().filter(|path| {
          std::path::Path::new(&path).exists()
        })
        .map(|path| File::with_name(path))
        .collect::<Vec<_>>())
      .add_source(config::Environment::with_prefix("ILUVATAR_WORKER")
          .try_parsing(true)
          .separator("__"));
    let s = match cleaning {
      false => s.build()?,
      // disable network pool during cleaning
      true => s.set_override("networking.use_pool", false)?.build()?,
    };
    s.try_deserialize()
  }

  pub fn boxed(cleaning: bool, config_fpath: &String) -> Result<WorkerConfig, ConfigError> {
    Ok(Arc::new(Configuration::new(cleaning, config_fpath)?))
  }
}
