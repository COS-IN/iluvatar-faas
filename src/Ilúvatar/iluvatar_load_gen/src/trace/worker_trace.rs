use super::{CsvInvocation, TraceArgs};
use crate::trace::prepare_function_args;
use crate::utils::{wait_elapsed_live, wait_elapsed_sim};
use crate::{
    trace::trace_utils::{make_simulation_worker_config, worker_prepare_functions},
    utils::{resolve_handles, save_result_json, save_worker_result_csv, worker_invoke, VERSION},
};
use anyhow::Result;
use iluvatar_library::clock::{get_global_clock, now};
use iluvatar_library::tokio_utils::{build_tokio_runtime, TokioRuntime};
use iluvatar_library::utils::is_simulation;
use iluvatar_library::{
    logging::start_simulation_tracing,
    transaction::{gen_tid, TransactionId, LIVE_WORKER_LOAD_TID, SIMULATION_START_TID},
    utils::config::args_to_json,
};
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

fn run_invokes(
    tid: &TransactionId,
    args: TraceArgs,
    factory: Arc<WorkerAPIFactory>,
    runtime: TokioRuntime,
    host: String,
) -> Result<()> {
    let mut metadata = super::load_metadata(&args.metadata_csv)?;
    let trace: Vec<CsvInvocation> = crate::trace::trace_utils::load_trace_csv(&args.input_csv, tid)?;
    let clock = get_global_clock(tid)?;
    worker_prepare_functions(
        args.setup,
        &mut metadata,
        &host,
        args.port,
        args.load_type,
        args.function_data,
        &runtime,
        args.prewarms,
        &args.input_csv,
        &factory,
        args.max_prewarms,
        tid,
    )?;

    info!(tid = tid, "Starting invocations");
    let results = runtime.block_on(async {
        let mut handles = Vec::new();
        let start = now();
        for invoke in trace {
            let func = metadata.get(&invoke.func_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "Invocation had function name '{}' that wasn't in metadata",
                    invoke.func_name
                )
            })?;

            let func_args = match is_simulation() {
                false => args_to_json(&prepare_function_args(func, args.load_type))?,
                true => serde_json::to_string(func.sim_invoke_data.as_ref().unwrap())?,
            };
            let f_c = func.func_name.clone();
            let clk_clone = clock.clone();
            let fct_cln = factory.clone();
            let h_c = host.clone();
            match is_simulation() {
                true => wait_elapsed_sim(&start, invoke.invoke_time_ms, args.tick_step, args.sim_gran).await,
                false => wait_elapsed_live(&start, invoke.invoke_time_ms).await,
            }
            debug!(tid=tid, elapsed=?start.elapsed(), waiting=invoke.invoke_time_ms, "Launching invocation");
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
                )
                .await
            }));
            debug!(tid=tid, elapsed=?start.elapsed(), waiting=invoke.invoke_time_ms, "Invocation sent");
        }
        info!(tid = tid, "Invocations sent, awaiting on thread handles");
        resolve_handles(handles, crate::utils::ErrorHandling::Print).await
    })?;

    info!(tid = tid, "Threads closed, writing results to file");
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

pub fn simulated_worker(args: TraceArgs) -> Result<()> {
    let tid: &TransactionId = &SIMULATION_START_TID;
    iluvatar_library::utils::set_simulation(tid)?;
    let worker_config_pth = args
        .worker_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Must have 'worker_config' for sim"))?
        .clone();
    let (worker_config, spec_config) = make_simulation_worker_config(0, &worker_config_pth)?;
    let threaded_rt = build_tokio_runtime(&None, &None, &None, tid)?;
    let _rt_guard = threaded_rt.enter();
    let _guard = start_simulation_tracing(&worker_config.logging, false, 1, "worker", tid)?;
    let factory = WorkerAPIFactory::boxed();
    info!(tid = tid, "starting simulation run");

    run_invokes(tid, args, factory, threaded_rt, spec_config)
}

pub fn live_worker(args: TraceArgs) -> Result<()> {
    let tid: &TransactionId = &LIVE_WORKER_LOAD_TID;
    let factory = WorkerAPIFactory::boxed();
    let threaded_rt = build_tokio_runtime(&None, &None, &None, tid)?;
    let host = args.host.clone();
    info!(tid = tid, "starting live run");

    run_invokes(tid, args, factory, threaded_rt, host)
}
