pub mod args;
pub mod commands;

use anyhow::Result;
use args::Args;
use clap::Parser;
use iluvatar_library::bail_error;
use iluvatar_library::logging::{start_tracing, LoggingConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let _drops = start_tracing(
        &Arc::new(LoggingConfig {
            level: "INFO".to_string(),
            stdout: Some(true),
            spanning: "NONE".to_string(),
            ..Default::default()
        }),
        &"LOAD_GEN_MAIN".to_string(),
    )?;
    let cli = match Args::try_parse() {
        Ok(arg) => arg,
        Err(e) => bail_error!("Failed to parse args with error '{}'", e),
    };
    if let Err(e) = match cli.command {
        args::Commands::Invoke(args) => commands::invoke(cli.host, cli.port, args).await,
        args::Commands::InvokeAsync(args) => commands::invoke_async(cli.host, cli.port, args).await,
        args::Commands::InvokeAsyncCheck(args) => commands::invoke_async_check(cli.host, cli.port, args).await,
        args::Commands::Prewarm(args) => commands::prewarm(cli.host, cli.port, args).await,
        args::Commands::Register(args) => commands::register(cli.host, cli.port, args).await,
        args::Commands::Status => commands::health(cli.host, cli.port).await,
        args::Commands::Health => commands::status(cli.host, cli.port).await,
        args::Commands::Ping => commands::ping(cli.host, cli.port).await,
        args::Commands::List => commands::list_registered_funcs(cli.host, cli.port).await,
    } {
        bail_error!("Command failed because of error {}", e);
    };
    Ok(())
}
