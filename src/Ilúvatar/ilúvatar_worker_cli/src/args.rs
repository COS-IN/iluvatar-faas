extern crate clap;
// use clap::{ArgMatches, App, SubCommand, Arg};
use serde::Deserialize;
use config::{Config, ConfigError, File};
use iluvatar_library::{utils::port_utils::Port, types::MemSizeMb};
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
#[allow(unused)]
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

// pub fn parse() -> ArgMatches {
//   App::new("ilúvatar_worker_cli")
//     .version("0.1.0")
//     .about("Interacts with Ilúvatar workers")
//     .args_from_usage(
//         "-c, --config=[FILE] 'Sets a custom config file'
//         -w, --worker=[NAME]           'Name of worker to send request to'")
//     .subcommand(SubCommand::with_name("ping")
//                 .about("Pings a worker to check if it is up"))
                
//     .subcommand(SubCommand::with_name("invoke")
//                 .about("Invoke a function")
//                 .arg(Arg::with_name("memory")
//                   .short('m')
//                   .long("memory")
//                   .help("Memory limit of the function")
//                   .required(false)
//                   .default_value("128")
//                   .takes_value(true))
//                 .arg(Arg::with_name("arguments")
//                   .short('a')
//                   .long("arguments")
//                   .help("Function arguments")
//                   .required(false)
//                   .multiple(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("name")
//                   .short('n')
//                   .long("name")
//                   .help("Name of function to invoke")
//                   .required(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("version")
//                   .long("version")
//                   .default_value("0.1.0")
//                   .help("Version of function to invoke")
//                   .required(false)
//                   .takes_value(true)))

//     .subcommand(SubCommand::with_name("invoke-async")
//                 .about("Invoke a function asynchronously")
//                 .arg(Arg::with_name("name")
//                   .short('n')
//                   .long("name")
//                   .help("Name of function to invoke")
//                   .required(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("arguments")
//                   .short('a')
//                   .long("arguments")
//                   .help("Function arguments")
//                   .required(false)
//                   .multiple(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("memory")
//                   .short('m')
//                   .long("memory")
//                   .help("Memory limit of the function")
//                   .required(false)
//                   .default_value("0")
//                   .takes_value(true))
//                 .arg(Arg::with_name("version")
//                   .long("version")
//                   .default_value("0.1.0")
//                   .help("Version of function to invoke")
//                   .required(false)
//                   .takes_value(true)))

//     .subcommand(SubCommand::with_name("invoke-async-check")
//                 .about("Check on the status of an asynchronously invoked function")
//                 .arg(Arg::with_name("cookie")
//                   .short('c')
//                   .long("cookie")
//                   .help("Cookie for async invoke to check")
//                   .required(true)
//                   .takes_value(true)))
                    
//     .subcommand(SubCommand::with_name("prewarm")
//                 .about("Prewarm a function")
//                 .arg(Arg::with_name("name")
//                   .short('n')
//                   .long("name")
//                   .help("Name of function to invoke")
//                   .required(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("image")
//                   .short('i')
//                   .long("image")
//                   .help("Fully qualified image name for function")
//                   .required(false)
//                   .default_value("")
//                   .takes_value(true))
//                 .arg(Arg::with_name("memory")
//                   .short('m')
//                   .long("memory")
//                   .help("Memory limit of the function")
//                   .required(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("cpu")
//                   .short('c')
//                   .long("cpu")
//                   .help("Number of CPUs to allocate to function")
//                   .required(false)
//                   .default_value("0")
//                   .takes_value(true))
//                 .arg(Arg::with_name("version")
//                   .long("version")
//                   .default_value("0.1.0")
//                   .help("Version of function to invoke")
//                   .required(false)
//                   .takes_value(true)))
      
//     .subcommand(SubCommand::with_name("register")
//                 .about("Register a new function")
//                 .arg(Arg::with_name("name")
//                   .short('n')
//                   .long("name")
//                   .help("Name of function to register")
//                   .required(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("image")
//                   .short('i')
//                   .long("image")
//                   .help("Fully qualified image name for function")
//                   .required(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("memory")
//                   .short('m')
//                   .long("memory")
//                   .help("Memory limit of the function")
//                   .required(true)
//                   .takes_value(true))
//                 .arg(Arg::with_name("cpu")
//                   .short('c')
//                   .long("cpu")
//                   .help("Number of CPUs to allocate to function")
//                   .required(false)
//                   .default_value("1")
//                   .takes_value(true))
//                 .arg(Arg::with_name("parallel-invokes")
//                   .short('p')
//                   .long("parallel-invokes")
//                   .help("Number of parallel invocations allowed inside one sandbox")
//                   .required(false)
//                   .default_value("1")
//                   .takes_value(true))
//                 .arg(Arg::with_name("version")
//                   .long("version")
//                   .default_value("0.1.0")
//                   .help("Version of function to register")
//                   .required(false)
//                   .takes_value(true)))

//     .subcommand(SubCommand::with_name("status")
//                 .about("Get the current status"))
//     .subcommand(SubCommand::with_name("health")
//                 .about("Get the current health status"))
//     .get_matches()
// }
