use super::{trace_utils::save_controller_results, Function, TraceArgs};
use crate::{
    benchmark::BenchmarkStore,
    trace::{trace_utils::map_functions_to_prep, CsvInvocation},
    utils::{load_benchmark_data, resolve_handles, CompletedControllerInvocation, FunctionExecOutput, VERSION},
};
use actix_web::{
    body::MessageBody,
    web::{Data, Json},
};
use anyhow::Result;
use iluvatar_controller_library::controller::structs::json::Invoke;
use iluvatar_controller_library::controller::web_server::{invoke, register_worker};
use iluvatar_controller_library::controller::{
    controller::Controller,
    controller_structs::json::{ControllerInvokeResult, Prewarm, RegisterFunction},
    web_server::{prewarm, register_function},
};
use iluvatar_library::{
    api_register::RegisterWorker,
    logging::LocalTime,
    transaction::{gen_tid, TransactionId, SIMULATION_START_TID},
    types::{CommunicationMethod, Compute, ComputeEnum, Isolation},
    utils::timing::TimedExt,
};
use iluvatar_worker_library::worker_api::worker_config::Configuration as WorkerConfig;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{runtime::Builder, task::JoinHandle};

async fn controller_sim_register_workers(
    num_workers: usize,
    server_data: &Data<Controller>,
    worker_config_pth: &String,
    worker_config: &Arc<WorkerConfig>,
) -> Result<()> {
    for i in 0..num_workers {
        let r = RegisterWorker {
            name: format!("worker_{}", i),
            communication_method: CommunicationMethod::SIMULATION,
            host: worker_config_pth.clone(),
            port: 0,
            memory: worker_config.container_resources.memory_mb,
            cpus: worker_config
                .container_resources
                .resource_map
                .get(&ComputeEnum::cpu)
                .expect("`resource_map` did not have CPU entry")
                .count,
            gpus: 0,
            compute: Compute::CPU,
            isolation: Isolation::CONTAINERD,
        };
        let response = register_worker(server_data.clone(), Json { 0: r }).await;
        if !response.status().is_success() {
            let text = response.body();
            anyhow::bail!(
                "Registering simulated worker failed with '{:?}' '{:?}",
                response.headers(),
                text
            )
        }
    }
    Ok(())
}

async fn controller_sim_register_functions(
    metadata: &HashMap<String, Function>,
    server_data: &Data<Controller>,
    benchmark: Option<&BenchmarkStore>,
) -> Result<()> {
    for (_, func) in metadata.iter() {
        let func_timings = match &func.chosen_name {
            Some(chosen_name) => match benchmark.as_ref() {
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
        let r = RegisterFunction {
            function_name: func.func_name.to_string(),
            function_version: VERSION.clone(),
            image_name: "".to_string(),
            memory: func.mem_mb,
            cpus: 1,
            parallel_invokes: 1,
            timings: func_timings,
        };
        let response = register_function(server_data.clone(), Json { 0: r }).await;
        if !response.status().is_success() {
            let text = response.body();
            anyhow::bail!("Registration failed with '{:?}' '{:?}", response.headers(), text)
        }
    }
    Ok(())
}

async fn controller_sim_prewarm_functions(
    metadata: &HashMap<String, Function>,
    server_data: &Data<Controller>,
) -> Result<()> {
    for (_, func) in metadata.iter() {
        for _ in 0..func.prewarms.ok_or_else(|| {
            anyhow::anyhow!(
                "Function '{:?}' did not have a prewarm value, supply one or pass a benchmark file",
                func
            )
        })? {
            let r = Prewarm {
                function_name: func.func_name.to_string(),
                function_version: VERSION.clone(),
            };
            let response = prewarm(server_data.clone(), Json { 0: r }).await;
            if !response.status().is_success() {
                let text = response.body();
                anyhow::bail!("Prewarm failed with '{:?}' '{:?}", response.headers(), text)
            }
        }
    }
    Ok(())
}

async fn controller_sim_invoke(
    func_name: String,
    server_data: Data<Controller>,
    warm_dur_ms: u64,
    cold_dur_ms: u64,
    clock: Arc<LocalTime>,
) -> Result<CompletedControllerInvocation> {
    let i = Invoke {
        function_name: func_name.clone(),
        function_version: VERSION.clone(),
        args: Some(vec![
            format!("warm_dur_ms={}", warm_dur_ms),
            format!("cold_dur_ms={}", cold_dur_ms),
        ]),
    };

    let invoke_start = clock.now_str()?;
    let (response, invok_lat) = invoke(server_data, Json { 0: i }).timed().await;
    if !response.status().is_success() {
        let text = response.body();
        return Ok(CompletedControllerInvocation::error(
            format!("Invocation error: {:?}", text),
            &func_name,
            &VERSION,
            None,
            invoke_start,
        ));
    }
    let b = response.into_body();
    let bytes = match b.try_into_bytes() {
        Ok(b) => b,
        Err(e) => {
            return Ok(CompletedControllerInvocation::error(
                format!("Error converting body into bytes: {:?}", e),
                &func_name,
                &VERSION,
                None,
                invoke_start,
            ))
        }
    };

    let r = match serde_json::from_slice::<ControllerInvokeResult>(&bytes) {
        Ok(r) => match serde_json::from_str::<FunctionExecOutput>(&r.result.json_result) {
            Ok(feo) => CompletedControllerInvocation {
                controller_response: r,
                function_output: feo,
                client_latency_us: invok_lat.as_micros(),
                function_name: func_name.clone(),
                function_version: VERSION.clone(),
                invoke_start,
            },
            Err(e) => CompletedControllerInvocation::error(
                format!(
                    "FunctionExecOutput Deserialization error: {}; {}",
                    e, &r.result.json_result
                ),
                &func_name,
                &VERSION,
                Some(&r.tid),
                invoke_start,
            ),
        },
        Err(e) => CompletedControllerInvocation::error(
            format!("Invocation error: {}", e),
            &func_name,
            &VERSION,
            None,
            invoke_start,
        ),
    };
    Ok(r)
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
        iluvatar_controller_library::controller::controller_config::Configuration::boxed(&controller_config_pth)
            .unwrap();
    let _guard =
        iluvatar_library::logging::start_tracing(controller_config.logging.clone(), &controller_config.name, tid)?;

    let server = threaded_rt.block_on(async { Controller::new(controller_config.clone(), tid).await })?;
    let server_data = actix_web::web::Data::new(server);

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
    threaded_rt.block_on(controller_sim_prewarm_functions(&metadata, &server_data))?;

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
