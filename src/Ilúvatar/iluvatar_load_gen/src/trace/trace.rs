use crate::utils::{wrap_logging, LoadType, RunType, Target};
use anyhow::{anyhow, Result};
use clap::Parser;
use controller_trace::{controller_trace_live, controller_trace_sim};
use iluvatar_library::tokio_utils::SimulationGranularity;
use iluvatar_library::types::{Compute, ContainerServer, Isolation};
use iluvatar_library::{types::MemSizeMb, utils::port::Port};
use iluvatar_worker_library::services::containers::simulator::simstructs::SimulationInvocation;
use std::collections::HashMap;

mod controller_trace;
mod trace_utils;
mod worker_trace;

#[derive(Parser, Debug)]
pub struct TraceArgs {
    #[arg(short, long, value_enum, default_value_t=RunType::Live)]
    /// System setup
    setup: RunType,
    #[arg(short, long, value_enum, default_value_t=Target::Worker)]
    /// Target for the load
    target: Target,
    #[arg(short, long, value_enum, default_value_t=LoadType::Functions)]
    /// Type of load to apply
    load_type: LoadType,
    #[arg(short, long)]
    /// The trace input csv file to use
    input_csv: String,
    #[arg(short, long)]
    /// The metadata associated with the trace input, also a csv file
    metadata_csv: String,
    #[arg(long)]
    /// The number of pre-warmed containers to create for each function.
    /// Computes how many containers to prewarm based on function characteristics.
    /// If this is 0, then there will be _no_ prewarms.
    /// There will be no prewarmed GPU containers.
    prewarms: Option<u32>,
    #[arg(long, default_value_t = 5)]
    /// The maximum number of prewarms any function is allowed to have
    max_prewarms: u32,
    #[arg(long)]
    /// Configuration file for the simuated worker(s)
    worker_config: Option<String>,
    #[arg(long)]
    /// Configuration file for the simulated controller
    controller_config: Option<String>,
    #[arg(long)]
    /// Number of workers to run with if performing controller simulation
    workers: Option<u32>,
    #[arg(short, long)]
    /// If using FunctionBench data, this file is the results of the `benchmark` run. Used to pick which function matches the trace function.
    function_data: Option<String>,
    #[arg(short, long)]
    /// Port controller/worker is listening on
    port: Port,
    #[arg(long)]
    /// Host controller/worker is on
    host: String,
    #[arg(short, long)]
    /// Folder to output results to
    pub out_folder: String,
    #[arg(long)]
    /// Output load generator logs to stdout
    pub log_stdout: bool,
    #[arg(long, value_enum, default_value_t=SimulationGranularity::MS)]
    /// Time granularity of system simulation
    sim_gran: SimulationGranularity,
    #[arg(long, default_value_t = 1)]
    /// Step size to increment simulation clock per tick
    tick_step: u64,
}

pub fn run_trace(args: TraceArgs) -> Result<()> {
    match args.target {
        Target::Worker => match args.setup {
            RunType::Simulation => worker_trace::simulated_worker(args),
            RunType::Live => wrap_logging(
                args.out_folder.clone(),
                args.log_stdout,
                args,
                worker_trace::live_worker,
            ),
        },
        Target::Controller => match args.setup {
            RunType::Simulation => controller_trace_sim(args),
            RunType::Live => wrap_logging(args.out_folder.clone(), args.log_stdout, args, controller_trace_live),
        },
    }
}

fn load_metadata(path: &str) -> Result<HashMap<String, Function>> {
    let mut rdr = match csv::Reader::from_path(path) {
        Ok(r) => r,
        Err(e) => anyhow::bail!("Unable to open metadata csv file '{}' because of error '{}'", path, e),
    };
    let mut ret = HashMap::new();
    for result in rdr.deserialize() {
        let mut func: Function = result.map_err(|e| anyhow!("Error deserializing metadata {}", e))?;
        if func.func_name.starts_with("lookbusy") {
            func.use_lookbusy = Some(true);
        }
        ret.insert(func.func_name.clone(), func);
    }
    Ok(ret)
}

#[derive(Debug, serde::Deserialize, Default)]
/// Struct holding the details about a function that will be run against the Ilúvatar system
/// If deserialized from JSON or via CSV, column names must match exactly
pub struct Function {
    pub func_name: String,
    pub cold_dur_ms: u64,
    pub warm_dur_ms: u64,
    pub mem_mb: MemSizeMb,
    pub use_lookbusy: Option<bool>,
    /// An optioanl value denoting the mean inter-arrival-time of the function
    /// Used for optimized prewarming, in milliseconds
    pub mean_iat: Option<f64>,
    /// An optioanl value denoting the image to use for the function
    /// One will be chosen if not provided
    pub image_name: Option<String>,
    /// An optioanl value denoting the number of prewarms for the function
    /// There will be no prewarmed GPU containers
    pub prewarms: Option<u32>,
    /// The compute(s) to test the function with, in the form CPU|GPU|etc.
    /// If empty, will default to CPU
    /// Functions that want GPU compute will be mapped to code that can use it from the benchmark, if found
    #[serde(default = "Compute::default")]
    pub compute: Compute,
    /// The isolations(s) to test the function with, in the form CONTAINERD|DOCKER|etc.
    /// If empty, will default to CONTAINERD
    #[serde(default = "Isolation::default")]
    pub isolation: Isolation,
    #[serde(default = "ContainerServer::default")]
    pub server: ContainerServer,
    /// Used internally, The code name the function was mapped to
    pub chosen_name: Option<String>,
    pub sim_invoke_data: Option<SimulationInvocation>,
    /// Arguments to pass to each invocation of the function
    pub args: Option<String>,
}
#[derive(Debug, serde::Deserialize)]
pub struct CsvInvocation {
    func_name: String,
    invoke_time_ms: u64,
}
pub fn safe_cmp(a: &f64, b: &f64) -> std::cmp::Ordering {
    if a.is_nan() && b.is_nan() {
        panic!("cannot compare two nan numbers!")
    } else if a.is_nan() {
        std::cmp::Ordering::Greater
    } else if b.is_nan() {
        std::cmp::Ordering::Less
    } else {
        a.partial_cmp(b).unwrap()
    }
}

pub fn prepare_function_args(func: &Function, load_type: LoadType) -> Vec<String> {
    if let Some(args) = &func.args {
        return args.split(';').map(|x| x.to_string()).collect::<Vec<String>>();
    }
    if let Some(b) = func.use_lookbusy {
        if b {
            return vec![
                format!("cold_run_ms={}", func.cold_dur_ms),
                format!("warm_run_ms={}", func.warm_dur_ms),
                format!("mem_mb={}", func.mem_mb),
            ];
        }
    }
    match load_type {
        LoadType::Lookbusy => vec![
            format!("cold_run_ms={}", func.cold_dur_ms),
            format!("warm_run_ms={}", func.warm_dur_ms),
            format!("mem_mb={}", func.mem_mb),
        ],
        LoadType::Functions => vec![],
    }
}
