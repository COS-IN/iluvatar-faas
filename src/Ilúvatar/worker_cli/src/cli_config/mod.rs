use serde::Deserialize;
use config::{Config, ConfigError, File};

pub mod args;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Worker {
  pub name: String,
  pub address: String,
  pub port: i32,

}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct CliSettings {
  workers: Vec<Box<Worker>>,
}

impl CliSettings {
  pub fn new() -> Result<Self, ConfigError> {
    let s = Config::builder()
    .add_source(File::with_name("worker_cli/src/worker_cli.json"))
    .build()?;

    // You can deserialize (and thus freeze) the entire configuration as
    s.try_deserialize()
  }

  pub fn get_worker(self, name: &str) -> Result<Box<Worker>, &str> {
    for item in self.workers {
      if item.name == name {
        return Ok(item);
      }
    }
    Err("Could not find worker")
  }
}
