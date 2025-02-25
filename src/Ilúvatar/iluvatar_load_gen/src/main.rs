pub mod benchmark;
pub mod scaling;
#[path = "./trace/trace.rs"]
pub mod trace;
pub mod utils;

use benchmark::BenchmarkArgs;
use clap::{command, Parser, Subcommand};
use iluvatar_library::bail_error;
use iluvatar_library::logging::LoggingConfig;
use scaling::ScalingArgs;
use std::sync::Arc;
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

fn start_logging(path: &str, _stdout: bool) -> anyhow::Result<impl Drop> {
    let overrides = vec![
        ("directory".to_string(), path.to_string()),
        ("basename".to_string(), "load_gen".to_string()),
    ];
    let log_cfg = iluvatar_library::config::load_config::<LoggingConfig>(None, None, Some(overrides), LOAD_GEN_PREFIX)?;
    iluvatar_library::logging::start_tracing(Arc::new(log_cfg), "load_gen", &"LOAD_GEN_MAIN".to_string())
}

fn wrap_logging<T>(path: String, stdout: bool, args: T, run: fn(args: T) -> anyhow::Result<()>) -> anyhow::Result<()> {
    let _drop = start_logging(&path, stdout)?;
    match run(args) {
        Err(e) => bail_error!(error=%e, "Load failed, check error log"),
        _ => Ok(()),
    }
}

fn main() -> anyhow::Result<()> {
    match Args::parse().command {
        Commands::Scaling(args) => wrap_logging(args.out_folder.clone(), args.log_stdout, args, scaling::scaling),
        Commands::Trace(args) => wrap_logging(args.out_folder.clone(), args.log_stdout, args, trace::run_trace),
        Commands::Benchmark(args) => wrap_logging(
            args.out_folder.clone(),
            args.log_stdout,
            args,
            benchmark::benchmark_functions,
        ),
    }?;
    Ok(())
}
