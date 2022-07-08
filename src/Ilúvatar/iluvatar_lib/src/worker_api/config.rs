use std::sync::Arc;
use crate::types::MemSizeMb;
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
  pub port: i32,
  /// request timeout length in seconds
  pub timeout_sec: u64,
  pub limits: FunctionLimits,
  pub logging: Logging,
  pub networking: Networking,
  pub container_resources: ContainerResources,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
/// total resources the worker is allowed to allocate to conainers
pub struct ContainerResources {
  /// total memory pool in MB
  pub memory_mb: MemSizeMb,
  /// number of cores it can use, i.e. number of concurrent functions allowed at once
  pub cores: u32,
  /// eviciton algorithm to use
  pub eviction: String,
  /// timeout on container startup before error
  pub startup_timeout_ms: u64,
  /// amount of memory the container pool monitor will try and maintain as a buffer (eager eviction)
  pub memory_buffer_mb: MemSizeMb,
  /// how often the container pool monitor will run, in seconds
  pub pool_freq_sec: u64,
  /// container backend to use: 
  /// containerd, docker (not implemented yet)
  pub backend: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
/// limits to place on an individual invocaiton
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
#[allow(unused)]
/// details about how/where to log to
/// a symlink to the current log file will be at "./iluvitar_worker.log"
pub struct Logging {
  /// the min log level
  pub level: String,
  /// directory to store logs in 
  pub directory: String,
  /// log filename start string
  pub basename: String
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Networking {
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
  /// frequency of namespace pool monitor runs
  pub pool_freq_sec: u64,
  /// network interface to attach bridge to
  pub hardware_interface: String,
}


pub type WorkerConfig = Arc<Configuration>;

impl Configuration {
  pub fn new(fpath: std::option::Option<&str>, cleaning: bool) -> Result<Self, ConfigError> {
    let fpath = match fpath {
        Some(f) => f,
        None => "worker/src/worker.json",
    };
    let s = Config::builder()
      .add_source(File::with_name(fpath))
      .build()?;
    let mut cfg: Configuration = s.try_deserialize()?;
    if cleaning {
      // disable network pool during cleaning
      cfg.networking.use_pool = false;
    }
    Ok(cfg)
  }

  pub fn boxed(fpath: std::option::Option<&str>, cleaning: bool) -> Result<WorkerConfig, ConfigError> {
    Ok(Arc::new(Configuration::new(fpath, cleaning)?))
  }
}
