pub mod commands;
pub mod args;
use args::parse;
use iluvatar_library::utils::config::get_val;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

  let args = parse();

  let worker_name = get_val("worker", &args)?;
  let settings = args::CliSettings::new(&args).unwrap();
  let worker = settings.get_worker(worker_name).unwrap();

  match args.subcommand() {
    Some( ("ping", _)) => { commands::ping(worker).await },
    Some(("invoke", _sub_m)) => { commands::invoke(worker, _sub_m).await },
    Some(("invoke-async", _sub_m)) => { commands::invoke_async(worker, _sub_m).await },
    Some(("invoke-async-check", _sub_m)) => { commands::invoke_async_check(worker, _sub_m).await },
    Some(("prewarm", _sub_m)) => { commands::prewarm(worker, _sub_m).await },
    Some(("register", _sub_m)) => { commands::register(worker, _sub_m).await },
    Some(("status", _sub_m)) => { commands::status(worker).await },
    Some(("health", _sub_m)) => { commands::health(worker).await },
    Some((text,_)) => { panic!("Unsupported command {}", text) },
    None => { panic!("Unsupported command") },
  }?;
  Ok(())
}
