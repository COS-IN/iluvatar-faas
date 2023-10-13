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
use iluvatar_library::{
    logging::LocalTime,
    transaction::TransactionId,
    types::{CommunicationMethod, Compute, Isolation},
    utils::port::Port,
};
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
use tokio::{runtime::Runtime, task::JoinHandle};

fn compute_prewarms(func: &Function, default_prewarms: Option<u32>, max_prewarms: u32) -> u32 {
    if func.parsed_compute.unwrap() == Compute::GPU {
        return 0;
    }
    match default_prewarms {
        None => 0,
        Some(0) => 0,
        Some(p) => match func.mean_iat {
            Some(iat_ms) => {
                let mut prewarms = f64::ceil(func.warm_dur_ms as f64 * 1.0 / iat_ms) as u32;
                let cold_prewarms = f64::ceil(func.cold_dur_ms as f64 * 1.0 / iat_ms) as u32;
                prewarms = max(prewarms, cold_prewarms);
                min(min(prewarms, p), max_prewarms)
            }
            None => p,
        },
    }
}

type ComputeChoiceList = Vec<(String, f64, f64, String)>;
fn choose_bench_data_for_func<'a>(
    func: &'a Function,
    cpu_data: &'a ComputeChoiceList,
    gpu_data: &'a ComputeChoiceList,
) -> Result<&'a ComputeChoiceList> {
    if let Some(func_compute) = func.parsed_compute {
        if func_compute.contains(Compute::GPU) {
            if gpu_data.len() == 0 {
                anyhow::bail!(
                    "Function '{}' wants GPU compute but there are no items in the benchmark that support it",
                    func.func_name
                );
            }
            return Ok(gpu_data);
        } else if func_compute.contains(Compute::CPU) {
            return Ok(cpu_data);
        } else {
            anyhow::bail!(
                "Unsupported compute {:?} in 'choose_bench_data_for_func' for function '{}'",
                func_compute,
                func.func_name
            );
        }
    }
    return Ok(cpu_data);
}

fn map_from_benchmark(
    funcs: &mut HashMap<String, Function>,
    bench: &BenchmarkStore,
    default_prewarms: Option<u32>,
    _trace_pth: &String,
    max_prewarms: u32,
) -> Result<()> {
    let mut cpu_data = Vec::new();
    for (k, v) in bench.data.iter() {
        if let Some(cpu_timings) = v.resource_data.get(&(&Compute::CPU).try_into()?) {
            let tot: u128 = cpu_timings.warm_invoke_duration_us.iter().sum();
            let avg_cold_us = cpu_timings.cold_invoke_duration_us.iter().sum::<u128>() as f64
                / cpu_timings.cold_invoke_duration_us.len() as f64;
            let avg_warm_us = tot as f64 / cpu_timings.warm_invoke_duration_us.len() as f64;
            // Cold uses E2E duration because of the cold start time needed
            cpu_data.push((
                k.clone(),
                avg_warm_us / 1000.0,
                avg_cold_us / 1000.0,
                v.image_name.clone(),
            ));
        }
    }
    let mut gpu_data = Vec::new();
    for (k, v) in bench.data.iter() {
        if let Some(gpu_timings) = v.resource_data.get(&(&Compute::GPU).try_into()?) {
            let tot: u128 = gpu_timings.warm_invoke_duration_us.iter().sum();
            let avg_cold_us = gpu_timings.cold_invoke_duration_us.iter().sum::<u128>() as f64
                / gpu_timings.cold_invoke_duration_us.len() as f64;
            let avg_warm_us = tot as f64 / gpu_timings.warm_invoke_duration_us.len() as f64;
            // Cold uses E2E duration because of the cold start time needed
            gpu_data.push((
                k.clone(),
                avg_warm_us / 1000.0,
                avg_cold_us / 1000.0,
                v.image_name.clone(),
            ));
        }
    }

    let mut total_prewarms = 0;
    for (_fname, func) in funcs.iter_mut() {
        let device_data = choose_bench_data_for_func(func, &cpu_data, &gpu_data)?;
        let chosen = match device_data.iter().min_by(|a, b| safe_cmp(&a.1, &b.1)) {
            Some(n) => n,
            None => panic!("failed to get a minimum func from {:?}", cpu_data),
        };
        let mut chosen_name = chosen.0.clone();
        let mut chosen_image = chosen.3.clone();
        let mut chosen_warm_time_ms = chosen.1;
        let mut chosen_cold_time_ms = chosen.1;

        for (name, avg_warm, avg_cold, image) in cpu_data.iter() {
            if func.warm_dur_ms as f64 >= *avg_warm && chosen_warm_time_ms < *avg_warm {
                chosen_name = name.clone();
                chosen_image = image.clone();
                chosen_warm_time_ms = *avg_warm;
                chosen_cold_time_ms = *avg_cold;
            }
        }

        if func.chosen_name.is_none() {
            println!("{} mapped to function '{}'", &func.func_name, chosen_name);
            func.cold_dur_ms = chosen_cold_time_ms as u64;
            func.warm_dur_ms = chosen_warm_time_ms as u64;
            func.chosen_name = Some(chosen_name);
            func.image_name = Some(chosen_image);
        }
        if func.prewarms.is_none() {
            let prewarms = compute_prewarms(func, default_prewarms, max_prewarms);
            func.prewarms = Some(prewarms);
            total_prewarms += prewarms;
        }
    }
    println!("A total of {} prewarmed containers", total_prewarms);
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
        func.mem_mb = func.mem_mb + 50;
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
    runtype: RunType,
    load_type: LoadType,
    func_json_data_path: &Option<String>,
    funcs: &mut HashMap<String, Function>,
    default_prewarms: Option<u32>,
    trace_pth: &String,
    max_prewarms: u32,
) -> Result<()> {
    for (_, v) in funcs.iter_mut() {
        v.parsed_compute = match v.compute.as_ref() {
            Some(c) => Some(Compute::try_from(c)?),
            None => Some(Compute::CPU),
        };
        v.parsed_isolation = match v.isolation.as_ref() {
            Some(c) => Some(Isolation::try_from(c)?),
            None => Some(Isolation::CONTAINERD),
        };
        if runtype == RunType::Simulation && v.image_name.is_none() {
            v.image_name = Some("SimImage".to_owned());
        }
    }
    match load_type {
        LoadType::Lookbusy => {
            return map_from_lookbusy(funcs, default_prewarms, max_prewarms);
        }
        LoadType::Functions => {
            if let Some(func_json_data) = load_benchmark_data(func_json_data_path)? {
                return map_from_benchmark(funcs, &func_json_data, default_prewarms, trace_pth, max_prewarms);
            } else {
                return map_from_args(funcs, default_prewarms, max_prewarms);
            }
        }
    }
}

fn worker_prewarm_functions(
    prewarm_data: &HashMap<String, Function>,
    host: &String,
    port: Port,
    rt: &Runtime,
    factory: &Arc<WorkerAPIFactory>,
    communication_method: CommunicationMethod,
) -> Result<()> {
    let mut prewarm_calls = vec![];
    for (func_name, func) in prewarm_data.iter() {
        println!(
            "{} prewarming {:?} containers for function '{}'",
            LocalTime::new(&"PREWARM_LOAD_GEN".to_string())?.now_str()?,
            func.prewarms,
            func_name
        );
        for i in 0..func.prewarms.ok_or_else(|| {
            anyhow::anyhow!(
                "Function '{}' did not have a prewarm value, supply one or pass a benchmark file",
                func_name
            )
        })? {
            let tid = format!("{}-{}-prewarm", i, &func_name);
            let h_c = host.clone();
            let f_c = func_name.clone();
            let fct_cln = factory.clone();
            prewarm_calls.push(async move {
                let mut errors = "Prewarm errors:".to_string();
                let mut it = (1..4).into_iter().peekable();
                while let Some(i) = it.next() {
                    match worker_prewarm(
                        &f_c,
                        &VERSION,
                        &h_c,
                        port,
                        &tid,
                        &fct_cln,
                        Some(communication_method),
                        Compute::CPU,
                    )
                    .await
                    {
                        Ok((_s, _prewarm_dur)) => break,
                        Err(e) => {
                            errors = format!("{} iteration {}: '{}';\n", errors, i, e);
                            if it.peek().is_none() {
                                anyhow::bail!("prewarm failed because {}", errors)
                            }
                        }
                    };
                }
                Ok(())
            });
        }
    }
    while prewarm_calls.len() > 0 {
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
    host: &String,
    port: Port,
    load_type: LoadType,
    func_data: Option<String>,
    rt: &Runtime,
    prewarms: Option<u32>,
    trace_pth: &String,
    factory: &Arc<WorkerAPIFactory>,
    max_prewarms: u32,
) -> Result<()> {
    map_functions_to_prep(runtype, load_type, &func_data, funcs, prewarms, trace_pth, max_prewarms)?;
    prepare_worker(funcs, host, port, runtype, rt, factory, &func_data)
}

fn prepare_worker(
    funcs: &mut HashMap<String, Function>,
    host: &String,
    port: Port,
    runtype: RunType,
    rt: &Runtime,
    factory: &Arc<WorkerAPIFactory>,
    func_data: &Option<String>,
) -> Result<()> {
    match runtype {
        RunType::Live => {
            worker_wait_reg(&funcs, rt, port, host, factory, CommunicationMethod::RPC, func_data)?;
            worker_prewarm_functions(&funcs, host, port, rt, factory, CommunicationMethod::RPC)
        }
        RunType::Simulation => {
            worker_wait_reg(
                &funcs,
                rt,
                port,
                host,
                factory,
                CommunicationMethod::SIMULATION,
                func_data,
            )?;
            worker_prewarm_functions(&funcs, host, port, rt, factory, CommunicationMethod::SIMULATION)
        }
    }
}

fn worker_wait_reg(
    funcs: &HashMap<String, Function>,
    rt: &Runtime,
    port: Port,
    host: &String,
    factory: &Arc<WorkerAPIFactory>,
    method: CommunicationMethod,
    func_data: &Option<String>,
) -> Result<()> {
    let mut func_iter = funcs.into_iter();
    let mut cont = true;
    let bench_data = load_benchmark_data(&func_data)?;
    loop {
        let mut handles: Vec<JoinHandle<Result<(String, Duration, TransactionId)>>> = Vec::new();
        for _ in 0..40 {
            let (id, func) = match func_iter.next() {
                Some(d) => d,
                None => {
                    cont = false;
                    break;
                }
            };
            let f_c = func.func_name.clone();
            let h_c = host.clone();
            let fct_cln = factory.clone();
            let image = match &func.image_name {
                Some(i) => i.clone(),
                None => anyhow::bail!("Unable to get prepared `image_name` for function '{}'", id),
            };
            let comp = func
                .parsed_compute
                .expect("Did not have a `parsed_compute` when going to register");
            let isol = func
                .parsed_isolation
                .expect("Did not have a `parsed_coparsed_isolationmpute` when going to register");
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
                    Some(method),
                    isol,
                    comp,
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
        "output-full{}.json",
        pth.file_stem().unwrap().to_str().unwrap()
    ));
    save_result_json(p, &results)?;

    let pth = Path::new(&args.input_csv);
    let p = Path::new(&args.out_folder).join(format!("output-{}", pth.file_name().unwrap().to_str().unwrap()));
    let mut f = match File::create(p) {
        Ok(f) => f,
        Err(e) => {
            anyhow::bail!("Failed to create output file because {}", e);
        }
    };
    let to_write = format!(
        "success,function_name,was_cold,worker_duration_us,invocation_duration_us,code_duration_asec,e2e_duration_us\n"
    );
    match f.write_all(to_write.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
            anyhow::bail!("Failed to write json of result because {}", e);
        }
    };

    for r in results {
        let to_write = format!(
            "{},{},{},{},{},{},{}\n",
            r.controller_response.success,
            r.function_name,
            r.function_output.body.cold,
            r.controller_response.worker_duration_us,
            r.controller_response.result.duration_us,
            r.function_output.body.latency,
            r.client_latency_us
        );
        match f.write_all(to_write.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                println!("Failed to write result because {}", e);
                continue;
            }
        };
    }
    Ok(())
}
