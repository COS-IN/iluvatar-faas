use super::{Function, TraceArgs};
use crate::trace::{prepare_function_args, trace_utils::map_functions_to_prep, CsvInvocation};
use crate::{
    benchmark::BenchmarkStore,
    trace::trace_utils::save_controller_results,
    utils::{
        controller_invoke, controller_prewarm, controller_register, load_benchmark_data, resolve_handles,
        CompletedControllerInvocation, VERSION,
    },
};
use anyhow::Result;
use iluvatar_library::{logging::LocalTime, transaction::gen_tid, utils::port::Port};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{runtime::Builder, task::JoinHandle};

async fn controller_live_register_functions(
    funcs: &HashMap<String, Function>,
    host: &String,
    port: Port,
    benchmark: Option<&BenchmarkStore>,
) -> Result<()> {
    for (fid, func) in funcs.into_iter() {
        let image = func
            .image_name
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Unable to get image name for function '{}'", fid))?;
        println!("{}, {}", func.func_name, image);
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
        let _reg_dur = controller_register(
            &func.func_name,
            &VERSION,
            &image,
            func.mem_mb + 50,
            host,
            port,
            func_timings,
        )
        .await?;
    }
    Ok(())
}

async fn prewarm_funcs(funcs: &HashMap<String, Function>, host: &String, port: Port) -> Result<()> {
    for (fid, func) in funcs.into_iter() {
        for _ in 0..func.prewarms.ok_or_else(|| {
            anyhow::anyhow!(
                "Function '{}' did not have a prewarm value, supply one or pass a benchmark file",
                fid
            )
        })? {
            let _reg_dur = controller_prewarm(&func.func_name, &VERSION, host, port).await?;
        }
    }
    Ok(())
}

pub fn controller_trace_live(args: TraceArgs) -> Result<()> {
    let mut metadata = super::load_metadata(&args.metadata_csv)?;
    let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
    let client = match reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .pool_idle_timeout(None)
        .connect_timeout(Duration::from_secs(60))
        .build()
    {
        Ok(c) => Arc::new(c),
        Err(e) => panic!("Unable to build reqwest HTTP client: {:?}", e),
    };
    map_functions_to_prep(
        crate::utils::RunType::Live,
        args.load_type,
        &args.function_data,
        &mut metadata,
        args.prewarms,
        &args.input_csv,
        args.max_prewarms,
    )?;
    let bench_data = load_benchmark_data(&args.function_data)?;
    threaded_rt.block_on(controller_live_register_functions(
        &metadata,
        &args.host,
        args.port,
        bench_data.as_ref(),
    ))?;
    threaded_rt.block_on(prewarm_funcs(&metadata, &args.host, args.port))?;

    let mut trace_rdr = csv::Reader::from_path(&args.input_csv)?;
    let mut handles: Vec<JoinHandle<Result<CompletedControllerInvocation>>> = Vec::new();
    let clock = Arc::new(LocalTime::new(&gen_tid())?);

    println!("starting live trace run");

    let start = SystemTime::now();
    for result in trace_rdr.deserialize() {
        let invocation: CsvInvocation = result?;
        let func = metadata.get(&invocation.func_name).ok_or_else(|| {
            anyhow::anyhow!(
                "Invocation had function name '{}' that wasn't in metadata",
                invocation.func_name
            )
        })?;
        let h_c = args.host.clone();
        let f_c = func.func_name.clone();
        let func_args = prepare_function_args(func, args.load_type);

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
        let clk_cln = clock.clone();
        let http_c = client.clone();
        handles.push(threaded_rt.spawn(async move {
            controller_invoke(&f_c, &VERSION, &h_c, args.port, Some(func_args), clk_cln, http_c).await
        }));
    }
    let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;
    save_controller_results(results, &args)
}
