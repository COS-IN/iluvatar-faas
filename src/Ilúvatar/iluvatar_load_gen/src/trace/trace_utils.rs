use super::{Function, TraceArgs};
use crate::{
    benchmark::BenchmarkStore,
    trace::safe_cmp,
    utils::{
        load_benchmark_data, save_result_json, worker_prewarm, worker_register, CompletedControllerInvocation,
        LoadType, RunType, VERSION,
    },
};
use anyhow::Result;
use iluvatar_library::tokio_utils::TokioRuntime;
use iluvatar_library::{bail_error, transaction::TransactionId, types::Compute, utils::port::Port};
use iluvatar_worker_library::services::containers::simulator::simstructs::SimInvokeData;
use iluvatar_worker_library::worker_api::config::{Configuration, WorkerConfig};
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::{
    cmp::{max, min},
    collections::HashMap,
    fs::File,
    io::Write,
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;
use tracing::info;

pub fn load_trace_csv<T: serde::de::DeserializeOwned, P: AsRef<Path> + tracing::Value>(
    csv: P,
    tid: &TransactionId,
) -> Result<Vec<T>> {
    let mut trace_rdr = match csv::Reader::from_path(&csv) {
        Ok(csv) => csv,
        Err(e) => bail_error!(error=%e, tid=tid, path=csv, "Failed to open CSV file"),
    };
    let mut ret = vec![];
    for (i, result) in trace_rdr.deserialize().enumerate() {
        match result {
            Ok(item) => ret.push(item),
            Err(e) => bail_error!(error=%e, tid=tid, line_num=i, path=csv, "Failed to deserialize item"),
        }
    }
    Ok(ret)
}

fn compute_prewarms(func: &Function, default_prewarms: Option<u32>, max_prewarms: u32) -> u32 {
    match default_prewarms {
        None => 0,
        Some(0) => 0,
        Some(p) => match func.mean_iat {
            Some(iat_ms) => {
                let mut prewarms = f64::ceil(func.warm_dur_ms as f64 * 1.0 / iat_ms) as u32;
                let cold_prewarms = f64::ceil(func.cold_dur_ms as f64 * 1.0 / iat_ms) as u32;
                prewarms = max(prewarms, cold_prewarms);
                min(min(prewarms, p), max_prewarms)
            },
            None => p,
        },
    }
}

type ComputeChoiceList = Vec<(String, f64, f64, String)>;
fn choose_bench_data_for_func<'a>(func: &'a Function, bench_data: &'a BenchmarkStore) -> Result<ComputeChoiceList> {
    let mut data = vec![];
    for (k, v) in bench_data.data.iter() {
        // does benchmark match all compute options for function?
        let mut prefered_compute = Compute::CPU;
        for func_compute in func.compute.into_iter() {
            if !v.resource_data.contains_key(&func_compute) {
                continue;
            }
            if func_compute == Compute::GPU {
                prefered_compute = Compute::GPU;
            }
            if func_compute == Compute::FPGA {
                prefered_compute = Compute::FPGA;
            }
        }

        if let Some(timings) = v.resource_data.get(&prefered_compute) {
            let tot: u128 = timings.warm_invoke_duration_us.iter().sum();
            let avg_cold_us = timings.cold_invoke_duration_us.iter().sum::<u128>() as f64
                / timings.cold_invoke_duration_us.len() as f64;
            let avg_warm_us = tot as f64 / timings.warm_invoke_duration_us.len() as f64;
            // Cold uses E2E duration because of the cold start time needed
            data.push((
                k.clone(),
                avg_warm_us / 1000.0,
                avg_cold_us / 1000.0,
                v.image_name.clone(),
            ));
        }
    }
    Ok(data)
}

fn map_from_benchmark(
    funcs: &mut HashMap<String, Function>,
    bench: &BenchmarkStore,
    default_prewarms: Option<u32>,
    _trace_pth: &str,
    max_prewarms: u32,
    tid: &TransactionId,
) -> Result<()> {
    let mut total_prewarms = 0;
    for (_fname, func) in funcs.iter_mut() {
        if let Some((_last, elements)) = func.func_name.split('-').collect::<Vec<&str>>().split_last() {
            let name = elements.join("-");
            if bench.data.contains_key(&name) && func.image_name.is_some() {
                info!(tid=tid, function=%func.func_name, chosen_code=%name, "Function mapped to self name in benchmark");
                func.chosen_name = Some(name);
            }
        }
        if bench.data.contains_key(&func.func_name) && func.image_name.is_some() && func.chosen_name.is_none() {
            info!(tid=tid, function=%func.func_name, "Function mapped to exact name in benchmark");
            func.chosen_name = Some(func.func_name.clone());
        }
        if func.chosen_name.is_none() {
            let device_data: ComputeChoiceList = choose_bench_data_for_func(func, bench)?;
            let chosen = match device_data.iter().min_by(|a, b| safe_cmp(&a.1, &b.1)) {
                Some(n) => n,
                None => anyhow::bail!("failed to get a minimum func from {:?}", device_data),
            };
            let mut chosen_name = chosen.0.clone();
            let mut chosen_image = chosen.3.clone();
            let mut chosen_warm_time_ms = chosen.1;
            let mut chosen_cold_time_ms = chosen.1;

            for (name, avg_warm, avg_cold, image) in device_data.iter() {
                if func.warm_dur_ms as f64 >= *avg_warm && chosen_warm_time_ms < *avg_warm {
                    chosen_name.clone_from(name);
                    chosen_image.clone_from(image);
                    chosen_warm_time_ms = *avg_warm;
                    chosen_cold_time_ms = *avg_cold;
                }
            }

            if func.image_name.is_none() {
                info!(tid=tid, function=%&func.func_name, chosen_code=%chosen_name, "Function mapped to benchmark code");
                func.cold_dur_ms = chosen_cold_time_ms as u64;
                func.warm_dur_ms = chosen_warm_time_ms as u64;
                func.chosen_name = Some(chosen_name);
                func.image_name = Some(chosen_image);
            }
        }
        if func.prewarms.is_none() {
            let prewarms = compute_prewarms(func, default_prewarms, max_prewarms);
            func.prewarms = Some(prewarms);
            total_prewarms += prewarms;
        }
        match &func.chosen_name {
            None => info!(tid = tid, "not filling out sim_invoke_data"),
            Some(name) => {
                let mut sim_data = HashMap::new();
                for compute in func.compute.into_iter() {
                    let bench_data = bench.data.get(name).ok_or_else(|| {
                        anyhow::format_err!(
                            "Failed to get benchmark data for function '{}' with chosen_name '{}'",
                            func.func_name,
                            name
                        )
                    })?;
                    let compute_data: &iluvatar_library::types::FunctionInvocationTimings = bench_data
                        .resource_data
                        .get(&compute)
                        .ok_or_else(|| {
                            anyhow::format_err!(
                            "failed to find data in bench_data.resource_data for function '{}' with chosen_name '{}' using compute '{}'",
                            func.func_name,
                            name,
                            compute
                        )
                        })?;
                    let warm_us = compute_data.warm_invoke_duration_us.iter().sum::<u128>()
                        / compute_data.warm_invoke_duration_us.len() as u128;
                    let cold_us = compute_data.cold_invoke_duration_us.iter().sum::<u128>()
                        / compute_data.cold_invoke_duration_us.len() as u128;
                    let compute_data = SimInvokeData {
                        warm_dur_ms: (warm_us / 1000) as u64,
                        cold_dur_ms: (cold_us / 1000) as u64,
                    };
                    sim_data.insert(compute, compute_data);
                }
                func.sim_invoke_data = Some(sim_data);
            },
        }
    }
    info!(tid = tid, "A total of {} prewarmed containers", total_prewarms);
    Ok(())
}

fn map_from_lookbusy(
    funcs: &mut HashMap<String, Function>,
    default_prewarms: Option<u32>,
    max_prewarms: u32,
) -> Result<()> {
    for (_fname, func) in funcs.iter_mut() {
        func.image_name = Some("docker.io/alfuerst/lookbusy-iluvatar-action:latest".to_string());
        func.prewarms = Some(compute_prewarms(func, default_prewarms, max_prewarms));
        func.mem_mb += 50;
    }
    Ok(())
}

fn map_from_args(
    funcs: &mut HashMap<String, Function>,
    default_prewarms: Option<u32>,
    max_prewarms: u32,
) -> Result<()> {
    for (_fname, func) in funcs.iter_mut() {
        func.prewarms = Some(compute_prewarms(func, default_prewarms, max_prewarms));
    }
    Ok(())
}

pub fn map_functions_to_prep(
    _runtype: RunType,
    load_type: LoadType,
    func_json_data_path: &Option<String>,
    funcs: &mut HashMap<String, Function>,
    default_prewarms: Option<u32>,
    trace_pth: &str,
    max_prewarms: u32,
    tid: &TransactionId,
) -> Result<()> {
    match load_type {
        LoadType::Lookbusy => map_from_lookbusy(funcs, default_prewarms, max_prewarms),
        LoadType::Functions => {
            if let Some(func_json_data) = load_benchmark_data(func_json_data_path)? {
                map_from_benchmark(funcs, &func_json_data, default_prewarms, trace_pth, max_prewarms, tid)
            } else {
                map_from_args(funcs, default_prewarms, max_prewarms)
            }
        },
    }
}

fn worker_prewarm_functions(
    prewarm_data: &HashMap<String, Function>,
    host: &str,
    port: Port,
    rt: &TokioRuntime,
    factory: &Arc<WorkerAPIFactory>,
) -> Result<()> {
    let mut prewarm_calls = vec![];
    for (func_name, func) in prewarm_data.iter() {
        info!("prewarming {:?} containers for function '{}'", func.prewarms, func_name);
        for i in 0..func.prewarms.ok_or_else(|| {
            anyhow::anyhow!(
                "Function '{}' did not have a prewarm value, supply one or pass a benchmark file",
                func_name
            )
        })? {
            let tid = format!("{}-{}-prewarm", i, &func_name);
            let h_c = host.to_owned();
            let f_c = func_name.clone();
            let fct_cln = factory.clone();
            let compute = func.compute;
            prewarm_calls.push(async move {
                let mut errors = "Prewarm errors:".to_string();
                let mut it = (1..4).peekable();
                while let Some(i) = it.next() {
                    match worker_prewarm(&f_c, &VERSION, &h_c, port, &tid, &fct_cln, compute).await {
                        Ok((_s, _prewarm_dur)) => break,
                        Err(e) => {
                            errors = format!("{} iteration {}: '{}';\n", errors, i, e);
                            if it.peek().is_none() {
                                anyhow::bail!("prewarm failed because {}", errors)
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        },
                    };
                }
                Ok(())
            });
        }
    }
    while !prewarm_calls.is_empty() {
        let mut handles = vec![];
        for _ in 0..4 {
            match prewarm_calls.pop() {
                Some(p) => handles.push(rt.spawn(p)),
                None => break,
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        for handle in handles {
            rt.block_on(handle)??;
        }
    }
    Ok(())
}

pub fn worker_prepare_functions(
    runtype: RunType,
    funcs: &mut HashMap<String, Function>,
    host: &str,
    port: Port,
    load_type: LoadType,
    func_data: Option<String>,
    rt: &TokioRuntime,
    prewarms: Option<u32>,
    trace_pth: &str,
    factory: &Arc<WorkerAPIFactory>,
    max_prewarms: u32,
    tid: &TransactionId,
) -> Result<()> {
    map_functions_to_prep(
        runtype,
        load_type,
        &func_data,
        funcs,
        prewarms,
        trace_pth,
        max_prewarms,
        tid,
    )?;
    prepare_worker(funcs, host, port, runtype, rt, factory, &func_data)
}

fn prepare_worker(
    funcs: &mut HashMap<String, Function>,
    host: &str,
    port: Port,
    runtype: RunType,
    rt: &TokioRuntime,
    factory: &Arc<WorkerAPIFactory>,
    func_data: &Option<String>,
) -> Result<()> {
    match runtype {
        RunType::Live => {
            worker_wait_reg(funcs, rt, port, host, factory, func_data)?;
            worker_prewarm_functions(funcs, host, port, rt, factory)
        },
        RunType::Simulation => {
            worker_wait_reg(funcs, rt, port, host, factory, func_data)?;
            worker_prewarm_functions(funcs, host, port, rt, factory)
        },
    }
}

fn worker_wait_reg(
    funcs: &HashMap<String, Function>,
    rt: &TokioRuntime,
    port: Port,
    host: &str,
    factory: &Arc<WorkerAPIFactory>,
    func_data: &Option<String>,
) -> Result<()> {
    let mut func_iter = funcs.iter();
    let mut cont = true;
    let bench_data = load_benchmark_data(func_data)?;
    loop {
        let mut handles: Vec<JoinHandle<Result<(String, Duration, TransactionId)>>> = Vec::new();
        for _ in 0..40 {
            let (id, func) = match func_iter.next() {
                Some(d) => d,
                None => {
                    cont = false;
                    break;
                },
            };
            let f_c = func.func_name.clone();
            let h_c = host.to_owned();
            let fct_cln = factory.clone();
            let image = match &func.image_name {
                Some(i) => i.clone(),
                None => anyhow::bail!("Unable to get prepared `image_name` for function '{}'", id),
            };
            let comp = func.compute;
            let isol = func.isolation;
            let server = func.server;
            let mem = func.mem_mb;
            let func_timings = match &func.chosen_name {
                Some(chosen_name) => match bench_data.as_ref() {
                    Some(t) => match t.data.get(chosen_name) {
                        Some(d) => Some(d.resource_data.clone()),
                        None => anyhow::bail!(format!(
                            "Benchmark was passed but function '{}' was not present",
                            chosen_name
                        )),
                    },
                    None => None,
                },
                None => None,
            };
            handles.push(rt.spawn(async move {
                worker_register(
                    f_c,
                    &VERSION,
                    image,
                    mem,
                    h_c,
                    port,
                    &fct_cln,
                    isol,
                    comp,
                    server,
                    func_timings.as_ref(),
                )
                .await
            }));
        }
        for h in handles {
            let (_s, _d, _s2) = rt.block_on(h)??;
        }
        if !cont {
            return Ok(());
        }
    }
}

pub fn save_controller_results(results: Vec<CompletedControllerInvocation>, args: &TraceArgs) -> Result<()> {
    let pth = Path::new(&args.input_csv);
    let p = Path::new(&args.out_folder).join(format!(
        "output-full-{}.json",
        pth.file_stem().unwrap().to_str().unwrap()
    ));
    save_result_json(p, &results)?;

    let pth = Path::new(&args.input_csv);
    let p = Path::new(&args.out_folder).join(format!("output-{}", pth.file_name().unwrap().to_str().unwrap()));
    let mut f = match File::create(p) {
        Ok(f) => f,
        Err(e) => {
            anyhow::bail!("Failed to create output file because {}", e);
        },
    };
    let to_write = "success,function_name,was_cold,worker_duration_us,code_duration_sec,e2e_duration_us,tid\n";
    match f.write_all(to_write.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
            anyhow::bail!("Failed to write json of result because {}", e);
        },
    };
    for r in results {
        let to_write = format!(
            "{},{},{},{},{},{},{}\n",
            r.controller_response.success,
            r.function_name,
            r.function_output.body.cold,
            r.controller_response.duration_us,
            r.function_output.body.latency,
            r.client_latency_us,
            r.tid,
        );
        match f.write_all(to_write.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                info!("Failed to write result because {}", e);
                continue;
            },
        };
    }
    Ok(())
}

/// Copies the worker config to a new file uniquely named file, and changes the "name" field to match.
pub fn make_simulation_worker_config(id: usize, orig_config_path: &str) -> Result<(WorkerConfig, String)> {
    let dummy_worker_config = match Configuration::boxed(Some(orig_config_path), None) {
        Ok(c) => c,
        Err(e) => bail_error!(error=%e, "Failed to load base configuration for worker"),
    };
    let worker_name = format!("{}_{}", dummy_worker_config.name, id);
    let overrides = vec![("name".to_owned(), worker_name.clone())];
    let worker_config = match Configuration::boxed(Some(orig_config_path), Some(overrides)) {
        Ok(c) => c,
        Err(e) => bail_error!(error=%e, worker=worker_name, "Failed to load configuration for worker"),
    };
    let p = Path::new(orig_config_path)
        .parent()
        .unwrap()
        .join(format!("{}.json", worker_name));
    match File::create(&p) {
        Ok(f) => match serde_json::to_writer_pretty(f, &worker_config) {
            Ok(_) => (),
            Err(e) => anyhow::bail!("Failed to serialize worker-specific config because '{:?}'", e),
        },
        Err(e) => anyhow::bail!("Failed to create worker-specific config file because '{:?}'", e),
    };
    Ok((worker_config, p.to_string_lossy().to_string()))
}
