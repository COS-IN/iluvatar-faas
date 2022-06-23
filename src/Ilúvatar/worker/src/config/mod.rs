use std::sync::Arc;
use serde::Deserialize;
use config::{Config, ConfigError, File};

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Configuration {
  pub name: String,
  pub address: String,
  pub port: i32,
  pub timeout_sec: u64,
  pub limits: FunctionLimits,
  pub logging: Logging,
  pub networking: Networking,
  pub container_resources: ContainerResources,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct ContainerResources {
  pub memory_mb: u32,
  pub cores: u32,
  pub eviction: String,
  pub startup_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct FunctionLimits {
  pub mem_min_mb: u32,
  pub mem_max_mb: u32,
  pub cpu_max: u32,
  pub timeout_sec: u64,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Logging {
  pub level: String,
  pub directory: String,
  pub basename: String
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Networking {
  pub bridge: String,
  pub cnitool: String,
  pub cni_plugin_bin: String,
  pub cni_name: String,
  pub use_pool: bool,
  pub pool_size: usize,
  pub pool_freq_sec: u64,
}


pub type WorkerConfig = Arc<Configuration>;

impl Configuration {
  pub fn new(fpath: std::option::Option<&str>) -> Result<Self, ConfigError> {
    let fpath = match fpath {
        Some(f) => f,
        None => "worker/src/worker.json",
    };
    let s = Config::builder()
    .add_source(File::with_name(fpath))
    .build()?;
    s.try_deserialize()
  }

  pub fn boxed(fpath: std::option::Option<&str>) -> Result<WorkerConfig, ConfigError> {
    Ok(Arc::new(Configuration::new(fpath)?))
  }
}
