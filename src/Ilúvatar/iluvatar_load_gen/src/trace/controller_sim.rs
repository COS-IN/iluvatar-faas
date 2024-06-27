use super::{trace_utils::save_controller_results, Function, TraceArgs};
use crate::{
    benchmark::BenchmarkStore,
    trace::{trace_utils::map_functions_to_prep, CsvInvocation},
    utils::{load_benchmark_data, resolve_handles, CompletedControllerInvocation, FunctionExecOutput, VERSION},
};
use anyhow::Result;
use iluvatar_controller_library::server::controller::Controller;
use iluvatar_library::{
    logging::LocalTime,
    transaction::{gen_tid, TransactionId, SIMULATION_START_TID},
    types::{CommunicationMethod, Compute, Isolation},
    utils::config::args_to_json,
};
use iluvatar_rpc::rpc::{iluvatar_controller_server::IluvatarController, LanguageRuntime};
use iluvatar_rpc::rpc::{InvokeRequest, RegisterRequest, RegisterWorkerRequest};
use iluvatar_worker_library::worker_api::worker_config::Configuration as WorkerConfig;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::{runtime::Builder, task::JoinHandle};
use tonic::Request;

async fn controller_sim_register_workers(
    num_workers: usize,
    server: &Arc<dyn IluvatarController>,
    worker_config_pth: &str,
    worker_config: &Arc<WorkerConfig>,
) -> Result<()> {
    for i in 0..num_workers {
        let gpus = worker_config
            .container_resources
            .gpu_resource
            .as_ref()
            .map_or(0, |c| c.count);
        let compute = match gpus {
          0 => Compute::CPU.bits(),
          _ => (Compute::CPU|Compute::GPU).bits(),
        };
        let r = RegisterWorkerRequest {
            name: format!("worker_{}", i),
            communication_method: CommunicationMethod::SIMULATION as u32,
            host: worker_config_pth.to_owned(),
            port: 0,
            memory: worker_config.container_resources.memory_mb,
            cpus: worker_config.container_resources.cpu_resource.count,
            gpus: gpus,
            compute: compute,
            isolation: (Isolation::CONTAINERD | Isolation::DOCKER).bits(),
        };
        let response = server.register_worker(Request::new(r)).await;
        match response {
            Ok(_) => (),
            Err(e) => anyhow::bail!("Registering simulated worker failed with '{:?}'", e),
        }
    }
    Ok(())
}

async fn controller_sim_register_functions(
    metadata: &HashMap<String, Function>,
    server: &Arc<dyn IluvatarController>,
    benchmark: Option<&BenchmarkStore>,
) -> Result<()> {
    for (_, func) in metadata.iter() {
        let func_timings = match &func.chosen_name {
            Some(chosen_name) => match benchmark.as_ref() {
                Some(t) => match t.data.get(chosen_name) {
                    Some(d) => serde_json::to_string(&d.resource_data)?,
                    None => anyhow::bail!(format!(
                        "Benchmark was passed but function '{}' was not present",
                        chosen_name
                    )),
                },
                None => "".to_owned(),
            },
            None => "".to_owned(),
        };
        let r = RegisterRequest {
            function_name: func.func_name.to_string(),
            function_version: VERSION.clone(),
            image_name: "".to_string(),
            memory: func.mem_mb,
            cpus: 1,
            parallel_invokes: 1,
            resource_timings_json: func_timings,
            transaction_id: format!("reg-{}", func.func_name),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            language: LanguageRuntime::Python3.into(),
        };
        let response = server.register(Request::new(r)).await;
        match response {
            Ok(_) => (),
            Err(e) => anyhow::bail!("Registering function failed with '{:?}'", e),
        }
    }
    Ok(())
}

async fn controller_sim_invoke(
    func_name: String,
    server: Arc<dyn IluvatarController>,
    warm_dur_ms: u64,
    cold_dur_ms: u64,
    clock: Arc<LocalTime>,
) -> Result<CompletedControllerInvocation> {
    let tid = gen_tid();
    let i = InvokeRequest {
        function_name: func_name.clone(),
        function_version: VERSION.clone(),
        json_args: args_to_json(&vec![
            format!("warm_dur_ms={}", warm_dur_ms),
            format!("cold_dur_ms={}", cold_dur_ms),
        ])?,
        transaction_id: tid.clone(),
    };

    let start = Instant::now();
    let invoke_start = clock.now_str()?;
    let response = server.invoke(Request::new(i)).await;
    let invok_lat = start.elapsed();
    let response = match response {
        Ok(r) => r.into_inner(),
        Err(e) => {
            return Ok(CompletedControllerInvocation::error(
                format!("Invocation error: {:?}", e),
                &func_name,
                &VERSION,
                &tid,
                invoke_start,
            ))
        }
    };

    match serde_json::from_str::<FunctionExecOutput>(&response.json_result) {
        Ok(feo) => Ok(CompletedControllerInvocation {
            controller_response: response,
            function_output: feo,
            client_latency_us: invok_lat.as_micros(),
            function_name: func_name.clone(),
            function_version: VERSION.clone(),
            invoke_start,
            transaction_id: tid,
        }),
        Err(e) => Ok(CompletedControllerInvocation::error(
            format!(
                "FunctionExecOutput Deserialization error: {}; {}",
                e, &response.json_result
            ),
            &func_name,
            &VERSION,
            &tid,
            invoke_start,
        )),
    }
}

pub fn controller_trace_sim(args: TraceArgs) -> Result<()> {
    iluvatar_library::utils::set_simulation();
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
    let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
    let clock = Arc::new(LocalTime::new(&gen_tid())?);

    let tid: &TransactionId = &SIMULATION_START_TID;
    let worker_config: Arc<WorkerConfig> = WorkerConfig::boxed(&Some(&worker_config_pth), None).unwrap();
    let controller_config =
        iluvatar_controller_library::server::controller_config::Configuration::boxed(&controller_config_pth).unwrap();
    let _guard =
        iluvatar_library::logging::start_tracing(controller_config.logging.clone(), &controller_config.name, tid)?;

    let server = threaded_rt.block_on(async { Controller::new(controller_config.clone(), tid).await })?;
    let server_data: Arc<dyn IluvatarController> = Arc::new(server);

    threaded_rt.block_on(controller_sim_register_workers(
        args.workers.ok_or_else(|| anyhow::anyhow!("Must have workers > 0"))? as usize,
        &server_data,
        &worker_config_pth,
        &worker_config,
    ))?;
    let mut metadata = super::load_metadata(&args.metadata_csv)?;
    map_functions_to_prep(
        crate::utils::RunType::Simulation,
        args.load_type,
        &args.function_data,
        &mut metadata,
        args.prewarms,
        &args.input_csv,
        args.max_prewarms,
    )?;
    let bench_data = load_benchmark_data(&args.function_data)?;
    threaded_rt.block_on(controller_sim_register_functions(
        &metadata,
        &server_data,
        bench_data.as_ref(),
    ))?;

    let mut trace_rdr = csv::Reader::from_path(&args.input_csv)?;
    let mut handles: Vec<JoinHandle<Result<CompletedControllerInvocation>>> = Vec::new();

    let start = SystemTime::now();
    for result in trace_rdr.deserialize() {
        let invocation: CsvInvocation = result?;
        let func = metadata.get(&invocation.func_name).ok_or_else(|| {
            anyhow::anyhow!(
                "Invocation had function name '{}' that wasn't in metadata",
                invocation.func_name
            )
        })?;
        loop {
            match start.elapsed() {
                Ok(t) => {
                    let ms = t.as_millis() as u64;
                    if ms >= invocation.invoke_time_ms {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(ms / 2));
                }
                Err(_) => (),
            }
        }
        let warm_dur_ms = func.warm_dur_ms;
        let cold_dur_ms = func.cold_dur_ms;
        let server_data_cln = server_data.clone();
        let clk = clock.clone();
        handles.push(threaded_rt.spawn(async move {
            controller_sim_invoke(invocation.func_name, server_data_cln, warm_dur_ms, cold_dur_ms, clk).await
        }));
    }
    let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;
    save_controller_results(results, &args)
}
