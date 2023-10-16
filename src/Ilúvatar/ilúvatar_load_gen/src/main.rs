pub mod benchmark;
pub mod scaling;
#[path = "./trace/trace.rs"]
pub mod trace;
pub mod utils;
use benchmark::BenchmarkArgs;
use clap::{command, Parser, Subcommand};
use scaling::ScalingArgs;
use trace::TraceArgs;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Scaling(ScalingArgs),
    Trace(TraceArgs),
    Benchmark(BenchmarkArgs),
}

fn main() -> anyhow::Result<()> {
    match Args::parse().command {
        Commands::Scaling(args) => scaling::scaling(args),
        Commands::Trace(args) => trace::run_trace(args),
        Commands::Benchmark(args) => benchmark::benchmark_functions(args),
    }?;
    Ok(())
}
