use crate::trace::prepare_function_args;
use crate::utils::*;
use anyhow::Result;
use clap::Parser;
use iluvatar_controller_library::server::controller_comm::ControllerAPIFactory;
use iluvatar_library::clock::{get_global_clock, now};
use iluvatar_library::tokio_utils::{build_tokio_runtime, TokioRuntime};
use iluvatar_library::types::{Compute, ContainerServer, Isolation, MemSizeMb, ResourceTimings};
use iluvatar_library::utils::config::args_to_json;
use iluvatar_library::{transaction::gen_tid, utils::port_utils::Port};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{collections::HashMap, path::Path};
use tracing::{error, info};

#[derive(Debug, serde::Deserialize, Clone)]
pub struct ToBenchmarkFunction {
    pub name: String,
    pub image_name: String,
    /// The compute(s) to test the function with, in the form CPU|GPU|etc.
    #[serde(default = "Compute::default")]
    pub compute: Compute,
    /// The isolations(s) to test the function with, in the form CONTAINERD|DOCKER|etc.
    #[serde(default = "Isolation::default")]
    pub isolation: Isolation,
    /// The memory to give the func
    /// If empty, will default to 512
    pub memory: Option<MemSizeMb>,
    /// The type of server in the image
    #[serde(default = "ContainerServer::default")]
    pub server: ContainerServer,
    /// Arguments to pass to each invocation of the function
    pub args: Option<String>,
}

#[derive(Serialize, Deserialize)]
/// Stores the benchmark data from any number of functions
pub struct BenchmarkStore {
    /// map of function name to data
    pub data: HashMap<String, FunctionStore>,
}
impl Default for BenchmarkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkStore {
    pub fn new() -> Self {
        BenchmarkStore { data: HashMap::new() }
    }
}
#[derive(Serialize, Deserialize)]
/// A struct to hold the benchmark results of a single function
pub struct FunctionStore {
    pub function_name: String,
    pub image_name: String,
    pub resource_data: ResourceTimings,
}
impl FunctionStore {
    pub fn new(image_name: String, function_name: String) -> Self {
        FunctionStore {
            function_name,
            image_name,
            resource_data: HashMap::new(),
        }
    }
}

#[derive(Parser, Debug)]
/// Benchmark functions through the system.
/// Functions will be run by iteratively by themselves (or in parallel with themselves if using threads).
/// All invocations will complete before a new function is run
pub struct BenchmarkArgs {
    #[arg(short, long, value_enum)]
    /// Target for the load
    target: Target,
    #[arg(long)]
    /// The csv with all the functions to be benchmarked listed inside of it. In the form <f_name>,<f_image>
    function_file: String,
    #[arg(long, default_value = "10")]
    /// Number of times to run each function cold
    cold_iters: u32,
    #[arg(long, default_value = "10")]
    /// Number of times to run function _after_ each cold start, expecting them to be warm (could vary because of load balancer)
    warm_iters: u32,
    #[arg(long, default_value = "0")]
    /// Duration in minutes that each function will be run for, being invoked in a closed loop.
    /// An alternative to cold/warm-iters.
    /// Leaving as 0 will use iters
    runtime: u32,
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
}

pub fn load_functions(args: &BenchmarkArgs) -> Result<Vec<ToBenchmarkFunction>> {
    let mut functions = Vec::new();

    let mut rdr = match csv::Reader::from_path(&args.function_file) {
        Ok(r) => r,
        Err(e) => anyhow::bail!(
            "Unable to open metadata csv file '{}' because of error '{}'",
            &args.function_file,
            e
        ),
    };
    for result in rdr.deserialize() {
        let func: ToBenchmarkFunction = result.expect("Error deserializing ToBenchmarkFunction");
        functions.push(func);
    }
    Ok(functions)
}

pub fn benchmark_functions(args: BenchmarkArgs) -> Result<()> {
    let functions = load_functions(&args)?;
    let threaded_rt = build_tokio_runtime(&None, &None, &None, &gen_tid())?;

    match args.target {
        Target::Worker => benchmark_worker(&threaded_rt, functions, args),
        Target::Controller => threaded_rt.block_on(benchmark_controller(
            args.host.clone(),
            args.port,
            functions,
            args.out_folder.clone(),
            args.cold_iters,
            args.warm_iters,
        )),
    }
}

pub async fn benchmark_controller(
    host: String,
    port: Port,
    functions: Vec<ToBenchmarkFunction>,
    out_folder: String,
    cold_repeats: u32,
    warm_repeats: u32,
) -> Result<()> {
    let factory = ControllerAPIFactory::boxed();
    let mut full_data = BenchmarkStore::new();
    for function in &functions {
        let mut func_data = FunctionStore::new(function.image_name.clone(), function.name.clone());
        info!("{}", function.name);
        let clock = get_global_clock(&gen_tid())?;
        let reg_tid = gen_tid();
        let api = factory.get_controller_api(&host, port, &reg_tid).await?;
        for iter in 0..cold_repeats {
            let name = format!("{}-bench-{}", function.name, iter);
            let version = format!("0.0.{}", iter);
            let _reg_dur = match controller_register(
                &name,
                &version,
                &function.image_name,
                512,
                function.isolation,
                function.compute,
                function.server,
                None,
                api.clone(),
            )
            .await
            {
                Ok(d) => d,
                Err(e) => {
                    error!("{}", e);
                    continue;
                },
            };

            'inner: for _ in 0..warm_repeats {
                match controller_invoke(&name, &version, None, clock.clone(), api.clone()).await {
                    Ok(invoke_result) => {
                        if invoke_result.controller_response.success {
                            let func_exec_us = invoke_result.function_output.body.latency * 1000000.0;
                            let invoke_lat = invoke_result.client_latency_us as f64;
                            let compute = Compute::from_bits_truncate(invoke_result.controller_response.compute);
                            let resource_entry = match func_data.resource_data.get_mut(&compute) {
                                Some(r) => r,
                                None => func_data.resource_data.entry(compute).or_default(),
                            };
                            if invoke_result.function_output.body.cold {
                                resource_entry
                                    .cold_results_sec
                                    .push(invoke_result.function_output.body.latency);
                                resource_entry.cold_over_results_us.push(invoke_lat - func_exec_us);
                                resource_entry
                                    .cold_worker_duration_us
                                    .push(invoke_result.client_latency_us);
                                resource_entry
                                    .cold_invoke_duration_us
                                    .push(invoke_result.controller_response.duration_us as u128);
                            } else {
                                resource_entry
                                    .warm_results_sec
                                    .push(invoke_result.function_output.body.latency);
                                resource_entry.warm_over_results_us.push(invoke_lat - func_exec_us);
                                resource_entry
                                    .warm_worker_duration_us
                                    .push(invoke_result.client_latency_us);
                                resource_entry
                                    .warm_invoke_duration_us
                                    .push(invoke_result.controller_response.duration_us as u128);
                            }
                        }
                    },
                    Err(e) => {
                        error!("{}", e);
                        break 'inner;
                    },
                }
            }
        }
        full_data.data.insert(function.name.clone(), func_data);
    }

    let p = Path::new(&out_folder).join("controller_function_benchmarks.json");
    save_result_json(p, &full_data)?;
    Ok(())
}

pub fn benchmark_worker(
    threaded_rt: &TokioRuntime,
    functions: Vec<ToBenchmarkFunction>,
    args: BenchmarkArgs,
) -> Result<()> {
    let mut full_data = BenchmarkStore::new();
    for f in &functions {
        full_data
            .data
            .insert(f.name.clone(), FunctionStore::new(f.image_name.clone(), f.name.clone()));
    }
    let mut invokes = vec![];
    let factory = iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory::boxed();
    let clock = get_global_clock(&gen_tid())?;
    let mut cold_repeats = args.cold_iters;

    for function in &functions {
        match args.runtime {
            0 => (),
            _ => {
                cold_repeats = 1;
            },
        };
        let memory = match function.memory.as_ref() {
            Some(c) => *c,
            None => 512,
        };
        let mut dummy = crate::trace::Function::default();
        let func_args = match &function.args {
            Some(arg) => {
                dummy.args = Some(arg.clone());
                args_to_json(&prepare_function_args(&dummy, LoadType::Functions))?
            },
            None => "{\"name\":\"TESTING\"}".to_string(),
        };
        for supported_compute in function.compute {
            info!("Running {} {}", &function.name, supported_compute);

            for iter in 0..cold_repeats {
                let name = format!("{}.{}.{}", &function.name, supported_compute, iter);
                let version = iter.to_string();
                let (_s, _reg_dur, _tid) = match threaded_rt.block_on(worker_register(
                    name.clone(),
                    &version,
                    function.image_name.clone(),
                    memory,
                    args.host.clone(),
                    args.port,
                    &factory,
                    function.isolation,
                    supported_compute,
                    function.server,
                    None,
                )) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("{:?}", e);
                        continue;
                    },
                };

                match args.runtime {
                    0 => {
                        for _ in 0..args.warm_iters + 1 {
                            match threaded_rt.block_on(worker_invoke(
                                &name,
                                &version,
                                &args.host,
                                args.port,
                                &gen_tid(),
                                Some(func_args.clone()),
                                clock.clone(),
                                &factory,
                            )) {
                                Ok(r) => invokes.push(r),
                                Err(e) => {
                                    error!("Invocation error: {}", e);
                                    continue;
                                },
                            };
                        }
                    },
                    duration_sec => {
                        let timeout = Duration::from_secs(duration_sec as u64);
                        let start = now();
                        while start.elapsed() < timeout {
                            match threaded_rt.block_on(worker_invoke(
                                &name,
                                &version,
                                &args.host,
                                args.port,
                                &gen_tid(),
                                Some(func_args.clone()),
                                clock.clone(),
                                &factory,
                            )) {
                                Ok(r) => invokes.push(r),
                                Err(e) => {
                                    error!("Invocation error: {}", e);
                                    continue;
                                },
                            };
                        }
                    },
                };
                if supported_compute != Compute::CPU {
                    match threaded_rt.block_on(worker_clean(&args.host, args.port, &gen_tid(), &factory)) {
                        Ok(_) => (),
                        Err(e) => error!("{:?}", e),
                    }
                }
            }
        }
    }

    for invoke in invokes.iter() {
        let parts = invoke.function_name.split('.').collect::<Vec<&str>>();
        let d = full_data
            .data
            .get_mut(parts[0])
            .expect("Unable to find function in result hash, but it should have been there");
        let invok_lat_f = invoke.client_latency_us as f64;
        let func_exec_us = invoke.function_output.body.latency * 1000000.0;
        let compute = Compute::from_bits_truncate(invoke.worker_response.compute);
        if invoke.worker_response.success {
            let resource_entry = match d.resource_data.get_mut(&compute) {
                Some(r) => r,
                None => d.resource_data.entry(compute).or_default(),
            };
            if invoke.function_output.body.cold {
                resource_entry
                    .cold_results_sec
                    .push(invoke.function_output.body.latency);
                resource_entry.cold_over_results_us.push(invok_lat_f - func_exec_us);
                resource_entry
                    .cold_worker_duration_us
                    .push(invoke.worker_response.duration_us as u128);
                resource_entry.cold_invoke_duration_us.push(invoke.client_latency_us);
            } else {
                resource_entry
                    .warm_results_sec
                    .push(invoke.function_output.body.latency);
                resource_entry.warm_over_results_us.push(invok_lat_f - func_exec_us);
                resource_entry
                    .warm_worker_duration_us
                    .push(invoke.worker_response.duration_us as u128);
                resource_entry.warm_invoke_duration_us.push(invoke.client_latency_us);
            }
        } else {
            error!("invoke failure {:?}", invoke.worker_response.json_result);
        }
    }
    let p = Path::new(&args.out_folder).join("worker_function_benchmarks.json");
    save_result_json(p, &full_data)?;
    let p = Path::new(&args.out_folder).join("benchmark-full.json");
    save_result_json(p, &invokes)?;
    let p = Path::new(&args.out_folder).join("benchmark-output.csv");
    save_worker_result_csv(p, &invokes)
}
