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

pub type WorkerConfig = Arc<Configuration>;

impl Configuration {
  pub fn new() -> Result<Self, ConfigError> {
    let s = Config::builder()
    .add_source(File::with_name("worker/src/worker.json"))
    .build()?;
    s.try_deserialize()
  }

  pub fn boxed() -> Result<WorkerConfig, ConfigError> {
    Ok(Arc::new(Configuration::new()?))
  }
}
