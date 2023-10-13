use crate::utils::{LoadType, RunType, Target};
use anyhow::Result;
use clap::Parser;
use iluvatar_library::{types::MemSizeMb, utils::port::Port};
use std::collections::HashMap;

mod controller_live;
mod controller_sim;
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
    /// There will be no prewarmed GPU containers
    /// If this is 0, then there will be _no_ prewarms.
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
    out_folder: String,
}

pub fn run_trace(args: TraceArgs) -> Result<()> {
    match args.target {
        Target::Worker => worker_trace::trace_worker(args),
        Target::Controller => match args.setup {
            RunType::Live => controller_live::controller_trace_live(args),
            RunType::Simulation => controller_sim::controller_trace_sim(args),
        },
    }
}

fn load_metadata(path: &String) -> Result<HashMap<String, Function>> {
    let mut rdr = match csv::Reader::from_path(path) {
        Ok(r) => r,
        Err(e) => anyhow::bail!("Unable to open metadata csv file '{}' because of error '{}'", path, e),
    };
    let mut ret = HashMap::new();
    for result in rdr.deserialize() {
        let mut func: Function = result.expect("Error deserializing metadata");
        if func.func_name.starts_with("lookbusy") {
            func.use_lookbusy = Some(true);
        }
        ret.insert(func.func_name.clone(), func);
    }
    Ok(ret)
}

#[derive(Debug, serde::Deserialize)]
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
    pub compute: Option<String>,
    /// Used internally, the parsed value from [Function::compute]
    pub parsed_compute: Option<iluvatar_library::types::Compute>,
    /// The isolations(s) to test the function with, in the form CONTAINERD|DOCKER|etc.
    /// If empty, will default to CONTAINERD
    pub isolation: Option<String>,
    /// Used internally, the parsed value from [Function::isolation]
    pub parsed_isolation: Option<iluvatar_library::types::Isolation>,
    /// Used internally, The code name the function was mapped to
    pub chosen_name: Option<String>,
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
        a.partial_cmp(&b).unwrap()
    }
}

fn prepare_function_args(func: &Function, load_type: LoadType) -> Vec<String> {
    if let Some(b) = func.use_lookbusy {
        if b {
            return vec![
                format!("cold_run={}", func.cold_dur_ms),
                format!("warm_run={}", func.warm_dur_ms),
                format!("mem_mb={}", func.mem_mb),
            ];
        }
    }
    match load_type {
        LoadType::Lookbusy => vec![
            format!("cold_run={}", func.cold_dur_ms),
            format!("warm_run={}", func.warm_dur_ms),
            format!("mem_mb={}", func.mem_mb),
        ],
        LoadType::Functions => vec![],
    }
}
