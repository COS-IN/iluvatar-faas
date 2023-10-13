pub mod args;
pub mod commands;
use anyhow::Result;
use args::Args;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Args::parse();
    match cli.command {
        args::Commands::Invoke(args) => commands::invoke(cli.address, cli.port, args).await,
        args::Commands::InvokeAsync(args) => commands::invoke_async(cli.address, cli.port, args).await,
        args::Commands::InvokeAsyncCheck(args) => commands::invoke_async_check(cli.address, cli.port, args).await,
        args::Commands::Prewarm(args) => commands::prewarm(cli.address, cli.port, args).await,
        args::Commands::Register(args) => commands::register(cli.address, cli.port, args).await,
        args::Commands::Status => commands::health(cli.address, cli.port).await,
        args::Commands::Health => commands::status(cli.address, cli.port).await,
        args::Commands::Ping => commands::ping(cli.address, cli.port).await,
    }
}
