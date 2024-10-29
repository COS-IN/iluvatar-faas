use super::{CsvInvocation, TraceArgs};
use crate::trace::prepare_function_args;
use crate::utils::wait_elapsed_live;
use crate::{
    trace::trace_utils::worker_prepare_functions,
    utils::{
        resolve_handles, save_result_json, save_worker_result_csv, worker_invoke, CompletedWorkerInvocation, RunType,
        VERSION,
    },
};
use anyhow::Result;
use iluvatar_library::clock::{get_global_clock, now};
use iluvatar_library::tokio_utils::{build_tokio_runtime, SimulationGranularity};
use iluvatar_library::{
    transaction::{gen_tid, TransactionId},
    types::CommunicationMethod,
    utils::config::args_to_json,
};
use iluvatar_worker_library::worker_api::{worker_comm::WorkerAPIFactory, worker_config::Configuration};
use std::path::Path;
use tokio::task::JoinHandle;
use tracing::{info, trace};

pub fn trace_worker(args: TraceArgs) -> Result<()> {
    match args.setup {
        RunType::Simulation => simulated_worker(args),
        RunType::Live => live_worker(args),
    }
}

fn simulated_worker(args: TraceArgs) -> Result<()> {
    let tid: &TransactionId = &iluvatar_library::transaction::SIMULATION_START_TID;
    iluvatar_library::utils::set_simulation(tid)?;
    let worker_config_pth = args
        .worker_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Must have 'worker_config' for sim"))?
        .clone();
    let server_config = Configuration::boxed(&Some(&worker_config_pth), None)?;
    let threaded_rt = build_tokio_runtime(&None, &None, &None, tid)?;
    let _rt_guard = threaded_rt.enter();
    let _guard = iluvatar_library::logging::start_tracing(server_config.logging.clone(), &server_config.name, tid)?;

    let mut metadata = super::load_metadata(&args.metadata_csv)?;
    let trace: Vec<CsvInvocation> = crate::trace::trace_utils::load_trace_csv(&args.input_csv, tid)?;
    let factory = WorkerAPIFactory::boxed();
    let clock = get_global_clock(tid)?;
    worker_prepare_functions(
        RunType::Simulation,
        &mut metadata,
        &worker_config_pth,
        args.port,
        args.load_type,
        args.function_data,
        &threaded_rt,
        args.prewarms,
        &args.input_csv,
        &factory,
        args.max_prewarms,
    )?;

    // TODO: simulated clock
    let results = threaded_rt.block_on(async {
        let mut handles = Vec::new();
        info!("starting simulation run");
        let start = now();
        for invoke in trace {
            let func = metadata.get(&invoke.func_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "Invocation had function name '{}' that wasn't in metadata",
                    invoke.func_name
                )
            })?;

            let func_args = serde_json::to_string(&func.sim_invoke_data.as_ref().unwrap())?;
            // wait_elapsed(&start, invoke.invoke_time_ms);
            loop {
                let elapsed = start.elapsed();
                trace!(tid=%tid, elapsed=?elapsed, elapsed=elapsed.as_millis(), waiting=invoke.invoke_time_ms, "Elapsed clock time vs wait time");
                if invoke.invoke_time_ms as u128 <= elapsed.as_millis() {
                    break;
                } else {
                    iluvatar_library::tokio_utils::sim_scheduler_tick(SimulationGranularity::MS).await;
                }
            }
            let f_c = func.func_name.clone();
            let clk_clone = clock.clone();
            let fct_cln = factory.clone();
            let h_c = worker_config_pth.clone();
            info!(tid=%tid, elapsed=?start.elapsed(), waiting=invoke.invoke_time_ms, "Launching invocation");
            handles.push(tokio::task::spawn(async move {
                worker_invoke(
                    &f_c,
                    &VERSION,
                    &h_c,
                    args.port,
                    &gen_tid(),
                    Some(func_args),
                    clk_clone,
                    &fct_cln,
                    Some(CommunicationMethod::SIMULATION),
                )
                .await
            }));
            info!(tid=%tid, elapsed=?start.elapsed(), waiting=invoke.invoke_time_ms, "Invocation sent");
        }
        iluvatar_library::tokio_utils::sim_scheduler_tick(SimulationGranularity::MS).await;
        info!(tid=%tid, "Invocations sent, awaiting on thread handles");
        // resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)
        let mut ret = vec![];
        for h in handles {
            match h.await {
                Ok(r) => match r {
                    Ok(ok) => ret.push(ok),
                    Err(e) => tracing::error!("Error from thread: {:?}", e),
                },
                Err(thread_e) => tracing::error!("Joining error: {}", thread_e),
            };
        }
        ret.sort();
        Ok::<Vec<CompletedWorkerInvocation>, anyhow::Error>(ret)
    })?;

    info!(tid=%tid, "Threads closed, writing results to file");
    let pth = Path::new(&args.input_csv);
    let p = Path::new(&args.out_folder).join(format!(
        "output-{}",
        pth.file_name().expect("Could not find a file name").to_str().unwrap()
    ));
    save_worker_result_csv(p, &results)?;

    let p = Path::new(&args.out_folder).join(format!(
        "output-full-{}.json",
        pth.file_stem().expect("Could not find a file name").to_str().unwrap()
    ));
    save_result_json(p, &results)
}

fn live_worker(args: TraceArgs) -> Result<()> {
    let tid: &TransactionId = &iluvatar_library::transaction::LIVE_WORKER_LOAD_TID;
    let factory = WorkerAPIFactory::boxed();

    let threaded_rt = build_tokio_runtime(&None, &None, &None, tid)?;

    let mut metadata = super::load_metadata(&args.metadata_csv)?;

    worker_prepare_functions(
        RunType::Live,
        &mut metadata,
        &args.host,
        args.port,
        args.load_type,
        args.function_data,
        &threaded_rt,
        args.prewarms,
        &args.input_csv,
        &factory,
        args.max_prewarms,
    )?;

    let mut trace_rdr = match csv::Reader::from_path(&args.input_csv) {
        Ok(r) => r,
        Err(e) => anyhow::bail!(
            "Unable to open trace csv file '{}' because of error '{}'",
            args.input_csv,
            e
        ),
    };
    let mut handles: Vec<JoinHandle<Result<CompletedWorkerInvocation>>> = Vec::new();
    let clock = get_global_clock(tid)?;

    info!("starting live trace run");
    let start = now();
    for result in trace_rdr.deserialize() {
        let invoke: CsvInvocation = match result {
            Ok(i) => i,
            Err(e) => anyhow::bail!("Error deserializing csv invocation: {}", e),
        };
        let func = metadata.get(&invoke.func_name).ok_or_else(|| {
            anyhow::anyhow!(
                "Invocation had function name '{}' that wasn't in metadata",
                invoke.func_name
            )
        })?;
        let h_c = args.host.clone();
        let f_c = func.func_name.clone();
        let func_args = args_to_json(&prepare_function_args(func, args.load_type))?;
        wait_elapsed_live(&start, invoke.invoke_time_ms);

        let clk_clone = clock.clone();
        let fct_cln = factory.clone();
        handles.push(threaded_rt.spawn(async move {
            worker_invoke(
                &f_c,
                &VERSION,
                &h_c,
                args.port,
                &gen_tid(),
                Some(func_args),
                clk_clone,
                &fct_cln,
                None,
            )
            .await
        }));
    }

    let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;

    let pth = Path::new(&args.input_csv);
    let p = Path::new(&args.out_folder).join(format!(
        "output-{}",
        pth.file_name().expect("Could not find a file name").to_str().unwrap()
    ));
    save_worker_result_csv(p, &results)?;

    let p = Path::new(&args.out_folder).join(format!(
        "output-full-{}.json",
        pth.file_stem().expect("Could not find a file name").to_str().unwrap()
    ));
    save_result_json(p, &results)
}
