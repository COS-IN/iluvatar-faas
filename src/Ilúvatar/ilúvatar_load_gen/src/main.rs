pub mod scaling;
pub mod utils;
#[path ="./trace/trace.rs"]
pub mod trace;
pub mod benchmark;
use benchmark::BenchmarkArgs;
use clap::{Parser, Subcommand, command};
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
  Scaling (ScalingArgs),
  Trace (TraceArgs),
  Benchmark (BenchmarkArgs),
}

fn main() -> anyhow::Result<()> {
  let cli = Args::parse();
  // println!("verbose: {:?}", cli);
  
  match cli.command {
    Commands::Scaling(args) => scaling::scaling(args),
    Commands::Trace(args) => trace::run_trace(args),
    Commands::Benchmark(args) => benchmark::benchmark_functions(args),
  }?;
  Ok(())
}
