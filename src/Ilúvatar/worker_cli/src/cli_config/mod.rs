use clap::ArgMatches;
use serde::Deserialize;
use config::{Config, ConfigError, File};

use self::args::get_val;

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
  pub fn new(args: &ArgMatches) -> Result<Self, ConfigError> {
    let mut sources = vec!["worker_cli/src/worker_cli.json".to_string(), "~/.config/Ilúvatar/worker_cli.json".to_string()];
    let _ = get_val("config", args).and_then(|path: String| { sources.push(path); Ok(()) });
    let s = Config::builder()
    .add_source(
      sources.iter()
      .filter(|path| {
        std::path::Path::new(&path).exists()
      })
              .map(|path| File::with_name(path))
              .collect::<Vec<_>>()
    )
    .build()?;
    s.try_deserialize()
  }

  pub fn get_worker(self, name: String) -> Result<Box<Worker>, &'static str> {
    for item in self.workers {
      if item.name == name {
        return Ok(item);
      }
    }
    Err("Could not find worker")
  }
}
