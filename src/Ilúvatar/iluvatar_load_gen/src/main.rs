pub mod benchmark;
pub mod scaling;
#[path = "./trace/trace.rs"]
pub mod trace;
pub mod utils;

use crate::utils::wrap_logging;
use benchmark::BenchmarkArgs;
use clap::{command, Parser, Subcommand};
use scaling::ScalingArgs;
use trace::TraceArgs;

const LOAD_GEN_PREFIX: &str = "LOAD_GEN";

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
        Commands::Scaling(args) => wrap_logging(args.out_folder.clone(), args.log_stdout, args, scaling::scaling),
        Commands::Trace(args) => trace::run_trace(args),
        Commands::Benchmark(args) => wrap_logging(
            args.out_folder.clone(),
            args.log_stdout,
            args,
            benchmark::benchmark_functions,
        ),
    }?;
    Ok(())
}
