extern crate clap;
// use clap::{ArgMatches, App, SubCommand, Arg};
use serde::Deserialize;
use config::{Config, ConfigError, File};
use iluvatar_library::{utils::port_utils::Port, types::{MemSizeMb, IsolationEnum}};
use clap::{command, Parser, Subcommand};

#[derive(Parser, Debug)]
pub struct InvokeArgs {
  #[arg(short, long)]
  /// Function arguments
  pub arguments: Option<Vec<String>>,
  #[arg(short, long)]
  /// Name of function to invoke
  pub name: String,
  #[arg(short, long)]
  /// Version of function to invoke
  pub version: String
}
#[derive(Parser, Debug)]
pub struct AsyncCheck {
  #[arg(long)]
  /// Cookie for async invoke to check
  pub cookie: String,
}
#[derive(Parser, Debug)]
pub struct PrewarmArgs {
  #[arg(short, long)]
  /// Name of function to prewarm
  pub name: String,
  #[arg(short, long)]
  /// Version of function to prewarm
  pub version: String,
  #[arg(short, long)]
  /// Memory in mb to allocate
  pub memory: Option<MemSizeMb>,
  #[arg(short, long)]
  /// Number of CPUs to allocate 
  pub cpu: Option<u32>,
  #[arg(short, long)]
  /// Image of function to register
  pub image: Option<String>,
}
#[derive(Parser, Debug)]
pub struct RegisterArgs {
  #[arg(short, long)]
  /// Name of function to register
  pub name: String,
  #[arg(short, long)]
  /// Version of function to register
  pub version: String,
  #[arg(short, long)]
  /// Image of function to register
  pub image: String,
  #[arg(short, long)]
  /// Memory in mb to allocate
  pub memory: MemSizeMb,
  #[arg(short, long)]
  /// Number of CPUs to allocate 
  pub cpu: u32,
  #[arg(long, required=true, num_args = 1..)]
  /// Isolation mechanisms supported by the function 
  pub isolation: Vec<IsolationEnum>,
}
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
  #[arg(short, long)]
  /// Sets a custom config file
  pub config: Option<String>,
  #[arg(short, long)]
  /// Name of worker to send request to
  pub worker: String,

  #[command(subcommand)]
  pub command: Commands,
}
#[derive(Subcommand, Debug)]
pub enum Commands {
  /// Invoke a function
  Invoke (InvokeArgs),
  /// Invoke a function asynchronously
  InvokeAsync (InvokeArgs),
  /// Check on the status of an asynchronously invoked function
  InvokeAsyncCheck (AsyncCheck),
  Prewarm (PrewarmArgs),
  Register (RegisterArgs),
  Status,
  Health,
  Ping
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Worker {
  pub name: String,
  pub address: String,
  pub port: Port,
}

#[derive(Debug, Deserialize)]
pub struct CliSettings {
  workers: Vec<Box<Worker>>,
}

impl CliSettings {
  pub fn new(args: &Args) -> Result<Self, ConfigError> {
    let mut sources = vec!["worker_cli/src/worker_cli.json".to_string(), "~/.config/Ilúvatar/worker_cli.json".to_string()];
    if let Some(c) = args.config.as_ref() {
      sources.push(c.clone());
    }
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

  pub fn get_worker(self, name: &String) -> Result<Box<Worker>, &'static str> {
    for item in self.workers {
      if &item.name == name {
        return Ok(item);
      }
    }
    Err("Could not find worker")
  }
}
