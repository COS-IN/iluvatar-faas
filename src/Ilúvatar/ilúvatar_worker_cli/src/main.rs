pub mod commands;
pub mod args;
use args::Args;
use clap::Parser;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
  let cli = Args::parse();
  let settings = args::CliSettings::new(&cli).unwrap();
  let worker = settings.get_worker(&cli.worker).unwrap();

  match cli.command {
    args::Commands::Invoke(args) => commands::invoke(worker, args).await,
    args::Commands::InvokeAsync(args) => commands::invoke_async(worker, args).await,
    args::Commands::InvokeAsyncCheck(args) => commands::invoke_async_check(worker, args).await,
    args::Commands::Prewarm(args) => commands::prewarm(worker, args).await,
    args::Commands::Register(args) => commands::register(worker, args).await,
    args::Commands::Status => commands::health(worker).await,
    args::Commands::Health => commands::status(worker).await,
    args::Commands::Ping => commands::ping(worker).await,
  }

  // match args.subcommand() {
  //   Some( ("ping", _)) => { commands::ping(worker).await },
  //   Some(("invoke", _sub_m)) => { commands::invoke(worker, _sub_m).await },
  //   Some(("invoke-async", _sub_m)) => { commands::invoke_async(worker, _sub_m).await },
  //   Some(("invoke-async-check", _sub_m)) => { commands::invoke_async_check(worker, _sub_m).await },
  //   Some(("prewarm", _sub_m)) => { commands::prewarm(worker, _sub_m).await },
  //   Some(("register", _sub_m)) => { commands::register(worker, _sub_m).await },
  //   Some(("status", _sub_m)) => { commands::status(worker).await },
  //   Some(("health", _sub_m)) => { commands::health(worker).await },
  //   Some((text,_)) => { panic!("Unsupported command {}", text) },
  //   None => { panic!("Unsupported command") },
  // }?;
  // Ok(())
}
