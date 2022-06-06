use serde::Deserialize;
use config::{Config, ConfigError, File};

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Configuration {
  pub name: String,
  pub address: String,
  pub port: i32,
}

impl Configuration {
  pub fn new() -> Result<Self, ConfigError> {
    let s = Config::builder()
    .add_source(File::with_name("worker/src/worker.json"))
    .build()?;
    s.try_deserialize()
  }
}
