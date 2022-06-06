extern crate iluvatar_worker_cli;
use iluvatar_worker_cli::cli_config::CliSettings;
use iluvatar_worker_cli::cli_config::args::parse;
use iluvatar_worker_cli::commands;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

  let args = parse();

  let worker_name = args.value_of("name").unwrap();
  let settings = CliSettings::new().unwrap();
  println!("Config = {:?}", settings);
  let worker = settings.get_worker(worker_name).unwrap();

  println!("name = {:?}", worker_name);
  println!("worker = {:?}", worker);

  match args.subcommand() {
    ("ping", Some(_)) => { commands::ping(worker).await },
    ("invoke", Some(_sub_m)) => { commands::invoke(worker).await },
    ("register", Some(_sub_m)) => { commands::register(worker).await },
    ("status", Some(_sub_m)) => { commands::status(worker).await },
    (text,_) => { panic!("Unsupported command {}", text) },
  };
  Ok(())
}
