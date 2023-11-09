pub mod args;
pub mod commands;
use anyhow::Result;
use args::Args;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Args::parse();
    match cli.command {
        args::Commands::Invoke(args) => commands::invoke(cli.host, cli.port, args).await,
        args::Commands::InvokeAsync(args) => commands::invoke_async(cli.host, cli.port, args).await,
        args::Commands::InvokeAsyncCheck(args) => commands::invoke_async_check(cli.host, cli.port, args).await,
        args::Commands::Prewarm(args) => commands::prewarm(cli.host, cli.port, args).await,
        args::Commands::Register(args) => commands::register(cli.host, cli.port, args).await,
        args::Commands::Status => commands::health(cli.host, cli.port).await,
        args::Commands::Health => commands::status(cli.host, cli.port).await,
        args::Commands::Ping => commands::ping(cli.host, cli.port).await,
    }
}
