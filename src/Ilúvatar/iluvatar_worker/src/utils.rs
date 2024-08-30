use clap::{command, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    /// Sets a custom config file
    pub config: Option<String>,
    /// Use direct mode for writing logs, rather than async version. Helpful for debugging
    #[arg(short, long)]
    pub direct_logs: Option<bool>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the worker
    Clean,
}
