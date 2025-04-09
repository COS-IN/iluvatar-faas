use super::{Function, TraceArgs};
use crate::trace::trace_utils::make_simulation_worker_config;
use crate::trace::{prepare_function_args, CsvInvocation};
use crate::utils::{wait_elapsed_live, wait_elapsed_sim};
use crate::{
    benchmark::BenchmarkStore,
    trace::trace_utils::{map_functions_to_prep, save_controller_results},
    utils::{
        controller_invoke, controller_prewarm, controller_register, load_benchmark_data, resolve_handles,
        CompletedControllerInvocation, VERSION,
    },
};
use anyhow::Result;
use iluvatar_controller_library::server::controller_comm::ControllerAPIFactory;
use iluvatar_controller_library::services::ControllerAPI;
use iluvatar_library::clock::{get_global_clock, now};
use iluvatar_library::logging::start_simulation_tracing;
use iluvatar_library::tokio_utils::{build_tokio_runtime, TokioRuntime};
use iluvatar_library::transaction::{gen_tid, TransactionId, LIVE_WORKER_LOAD_TID, SIMULATION_START_TID};
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_library::utils::{config::args_to_json, is_simulation, port::Port};
use iluvatar_rpc::rpc::RegisterWorkerRequest;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;
use tracing::info;

async fn controller_register_functions(
    funcs: &HashMap<String, Function>,
    host: &str,
    port: Port,
    benchmark: Option<&BenchmarkStore>,
    factory: Arc<ControllerAPIFactory>,
) -> Result<()> {
    for (fid, func) in funcs.iter() {
        let image = func
            .image_name
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Unable to get image name for function '{}'", fid))?;
        info!("Registering {}, {}", func.func_name, image);
        let func_timings = match &func.chosen_name {
            Some(chosen_name) => match benchmark.as_ref() {
                Some(t) => match t.data.get(chosen_name) {
                    Some(d) => Some(&d.resource_data),
                    None => anyhow::bail!(format!(
                        "Benchmark was passed but function '{}' was not present",
                        chosen_name
                    )),
                },
                None => None,
            },
            None => None,
        };
        let api = factory.get_controller_api(host, port, &gen_tid()).await?;
        let _reg_dur = controller_register(
            &func.func_name,
            &VERSION,
            image,
            func.mem_mb,
            func.isolation,
            func.compute,
            func.server,
            func_timings,
            api,
        )
        .await?;
    }
    Ok(())
}

async fn controller_prewarm_funcs(
    funcs: &HashMap<String, Function>,
    host: &str,
    port: Port,
    factory: Arc<ControllerAPIFactory>,
) -> Result<()> {
    for (fid, func) in funcs.iter() {
        for i in 0..func.prewarms.ok_or_else(|| {
            anyhow::anyhow!(
                "Function '{}' did not have a prewarm value, supply one or pass a benchmark file",
                fid
            )
        })? {
            let tid = format!("{}-prewarm-{}", fid, i);
            let api = factory.get_controller_api(host, port, &tid).await?;
            let _reg_dur = controller_prewarm(&func.func_name, &VERSION, api, &tid).await?;
        }
    }
    Ok(())
}

pub fn controller_trace_live(args: TraceArgs) -> Result<()> {
    let tid: &TransactionId = &LIVE_WORKER_LOAD_TID;
    let threaded_rt = build_tokio_runtime(&None, &None, &None, tid)?;
    let factory = ControllerAPIFactory::boxed();
    let host = args.host.clone();
    info!(tid = tid, "starting simulated run");
    run_invokes(args, factory, threaded_rt, &host, tid)
}

async fn controller_sim_register_workers(
    num_workers: usize,
    server: &ControllerAPI,
    worker_config_pth: &str,
) -> Result<()> {
    for i in 0..num_workers {
        let (worker_config, spec_config) = make_simulation_worker_config(i, worker_config_pth)?;
        let gpus = worker_config
            .container_resources
            .gpu_resource
            .as_ref()
            .map_or(0, |c| c.count);
        let compute = match gpus {
            0 => Compute::CPU.bits(),
            _ => (Compute::CPU | Compute::GPU).bits(),
        };
        let r = RegisterWorkerRequest {
            name: worker_config.name.clone(),
            host: spec_config,
            port: 0,
            memory: worker_config.container_resources.memory_mb,
            cpus: worker_config.container_resources.cpu_resource.count,
            gpus,
            compute,
            isolation: (Isolation::CONTAINERD | Isolation::DOCKER).bits(),
        };
        let response = server.register_worker(r).await;
        match response {
            Ok(_) => (),
            Err(e) => anyhow::bail!("Registering simulated worker failed with '{:?}'", e),
        }
    }
    Ok(())
}

fn run_invokes(
    args: TraceArgs,
    api_factory: Arc<ControllerAPIFactory>,
    threaded_rt: TokioRuntime,
    host: &str,
    tid: &TransactionId,
) -> Result<()> {
    info!(tid = tid, "Starting invocations");
    let clock = get_global_clock(tid)?;
    let mut metadata = super::load_metadata(&args.metadata_csv)?;
    map_functions_to_prep(
        crate::utils::RunType::Simulation,
        args.load_type,
        &args.function_data,
        &mut metadata,
        args.prewarms,
        &args.input_csv,
        args.max_prewarms,
        tid,
    )?;
    let bench_data = load_benchmark_data(&args.function_data)?;
    threaded_rt.block_on(controller_register_functions(
        &metadata,
        host,
        args.port,
        bench_data.as_ref(),
        api_factory.clone(),
    ))?;
    threaded_rt.block_on(controller_prewarm_funcs(
        &metadata,
        host,
        args.port,
        api_factory.clone(),
    ))?;

    let mut handles: Vec<JoinHandle<Result<CompletedControllerInvocation>>> = Vec::new();
    let api = threaded_rt.block_on(api_factory.get_controller_api(host, args.port, &gen_tid()))?;
    let trace: Vec<CsvInvocation> = crate::trace::trace_utils::load_trace_csv(&args.input_csv, tid)?;

    let results = threaded_rt.block_on(async {
        let start = now();
        for invocation in trace {
            let func = metadata.get(&invocation.func_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "Invocation had function name '{}' that wasn't in metadata",
                    invocation.func_name
                )
            })?;
            let api_cln = api.clone();
            let func_args = match is_simulation() {
                false => args_to_json(&prepare_function_args(func, args.load_type))?,
                true => serde_json::to_string(func.sim_invoke_data.as_ref().unwrap())?,
            };
            let clk = clock.clone();
            let f_c = func.func_name.clone();
            match is_simulation() {
                true => wait_elapsed_sim(&start, invocation.invoke_time_ms, args.tick_step, args.sim_gran).await,
                false => wait_elapsed_live(&start, invocation.invoke_time_ms).await,
            }
            handles.push(tokio::task::spawn(async move {
                controller_invoke(&f_c, &VERSION, Some(func_args), clk, api_cln).await
            }));
        }
        info!(tid = tid, "Invocations sent, awaiting on thread handles");
        resolve_handles(handles, crate::utils::ErrorHandling::Print).await
    })?;

    info!(tid = tid, "Threads closed, writing results to file");
    save_controller_results(results, &args)
}

pub fn controller_trace_sim(args: TraceArgs) -> Result<()> {
    let tid: &TransactionId = &SIMULATION_START_TID;
    iluvatar_library::utils::set_simulation(tid)?;
    let api_factory = ControllerAPIFactory::boxed();

    let worker_config_pth = args
        .worker_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Must have 'worker_config' for sim"))?
        .clone();
    let controller_config_pth = args
        .controller_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Must have 'controller_config' for sim"))?
        .clone();
    let threaded_rt = build_tokio_runtime(&None, &None, &None, tid)?;

    let controller_config =
        iluvatar_controller_library::server::controller_config::Configuration::boxed(&controller_config_pth)?;
    let num_workers = args.workers.ok_or_else(|| anyhow::anyhow!("Must have workers > 0"))? as usize;

    let _guard = start_simulation_tracing(&controller_config.logging, true, num_workers, "worker", tid)?;
    let controller =
        threaded_rt.block_on(async { api_factory.get_controller_api(&controller_config_pth, 0, tid).await })?;

    info!(tid = tid, "starting live run");
    threaded_rt.block_on(controller_sim_register_workers(
        num_workers,
        &controller,
        &worker_config_pth,
    ))?;
    run_invokes(args, api_factory, threaded_rt, &controller_config_pth, tid)
}
