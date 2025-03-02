// extern crate clap;
use clap::{command, Parser, Subcommand};
use iluvatar_library::types::{Compute, ContainerServer, Isolation};
use iluvatar_library::{types::MemSizeMb, utils::port_utils::Port};

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
    pub version: String,
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
    #[arg(long)]
    /// Supported compute by the function
    pub compute: Compute,
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
    #[arg(long, value_enum, num_args = 1.., required=false, default_values_t = [Isolation::CONTAINERD])]
    /// Isolation mechanisms supported by the function
    pub isolation: Vec<Isolation>,
    #[arg(long, value_enum, num_args = 1.., required=false, default_values_t = [Compute::CPU])]
    /// Supported compute by the function
    pub compute: Vec<Compute>,
    #[arg(long, required=false, default_value_t = ContainerServer::HTTP)]
    pub server: ContainerServer,
}
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    /// Hostname to target
    pub host: String,
    #[arg(short, long)]
    pub port: Port,
    #[command(subcommand)]
    pub command: Commands,
}
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Invoke a function
    Invoke(InvokeArgs),
    /// Invoke a function asynchronously
    InvokeAsync(InvokeArgs),
    /// Check on the status of an asynchronously invoked function
    InvokeAsyncCheck(AsyncCheck),
    /// Prewarm an instance of a function on the worker
    Prewarm(PrewarmArgs),
    /// Register a function with a worker, this must be done before prewarming or invoking
    Register(RegisterArgs),
    /// Print the resource status of the worker
    Status,
    /// Query worker health
    Health,
    /// Play table tennis
    Ping,
    /// List all registered functions
    List,
}
