use crate::{
    trace::prepare_function_args,
    utils::{
        resolve_handles, save_result_json, worker_invoke, worker_prewarm, worker_register, ErrorHandling,
        RegistrationResult, Target, ThreadResult,
    },
};
use anyhow::Result;
use clap::Parser;
use iluvatar_library::clock::{get_global_clock, now};
use iluvatar_library::tokio_utils::build_tokio_runtime;
use iluvatar_library::types::ContainerServer;
use iluvatar_library::{
    transaction::gen_tid,
    types::{Compute, Isolation, MemSizeMb},
    utils::{config::args_to_json, file_utils::ensure_dir, port_utils::Port},
};
use rand::prelude::*;
use std::path::{Path, PathBuf};
use std::{sync::Arc, time::Duration};
use tokio::sync::Barrier;
use tracing::{error, info};

#[derive(Parser, Debug)]
/// Test scaling of worker with increasing amount of requests
pub struct ScalingArgs {
    #[arg(short, long, value_enum)]
    /// Target for the load
    target: Target,
    #[arg(short, long)]
    /// Number of threads to start
    start: u32,
    #[arg(short, long)]
    /// Number of threads to reach
    end: u32,
    #[arg(short, long)]
    /// Duration in seconds before increasing load
    duration: u32,
    #[arg(short, long, default_value = "docker.io/alfuerst/hello-iluvatar-action:latest")]
    /// The image to use
    image: String,
    #[arg(short, long)]
    /// Port controller/worker is listening on
    port: Port,
    #[arg(long)]
    /// Host controller/worker is on
    host: String,
    #[arg(short, long)]
    /// Folder to output results and logs to
    pub out_folder: String,
    #[arg(long)]
    /// Output load generator logs to stdout
    pub log_stdout: bool,
    #[arg(long, default_value_t=Isolation::CONTAINERD)]
    /// Isolation the image will use
    pub isolation: Isolation,
    #[arg(long, default_value_t=Compute::CPU)]
    /// Compute the image will use.
    pub compute: Compute,
    #[arg(long, default_value_t=ContainerServer::HTTP)]
    pub server: ContainerServer,
    #[arg(long)]
    /// Memory need for the function
    memory_mb: MemSizeMb,
    #[arg(long)]
    /// Arguments to pass to function, in [x=y;...] format
    function_args: Option<String>,
}

pub fn scaling(args: ScalingArgs) -> Result<()> {
    ensure_dir(PathBuf::new().join(&args.out_folder))?;

    for threads in args.start..(args.end + 1) {
        let runtime = build_tokio_runtime(&None, &None, &Some(threads as usize), &"SCALING_TID".to_string())?;
        info!("Running with {} threads", threads);
        let result = runtime.block_on(run_one_scaling_test(threads as usize, &args))?;
        let p = Path::new(&args.out_folder).join(format!("{}.json", threads));
        save_result_json(p, &result)?;
    }

    Ok(())
}

async fn run_one_scaling_test(thread_cnt: usize, args: &ScalingArgs) -> Result<Vec<ThreadResult>> {
    let barrier = Arc::new(Barrier::new(thread_cnt));
    let mut threads = Vec::new();

    for thread_id in 0..thread_cnt {
        let host_c = args.host.clone();
        let b = barrier.clone();
        let i_c = args.image.clone();
        let compute = args.compute;
        let isolation = args.isolation;
        let server = args.server;
        let p = args.port;
        let d = args.duration.into();
        let mem = args.memory_mb;
        let a = args.function_args.clone();

        threads.push(tokio::task::spawn(async move {
            scaling_thread(
                host_c, p, d, thread_id, b, i_c, compute, isolation, server, thread_cnt, mem, a,
            )
            .await
        }));
    }

    resolve_handles(threads, ErrorHandling::Print).await
}

async fn scaling_thread(
    host: String,
    port: Port,
    duration: u64,
    thread_id: usize,
    barrier: Arc<Barrier>,
    image: String,
    compute: Compute,
    isolation: Isolation,
    server: ContainerServer,
    thread_cnt: usize,
    memory_mb: MemSizeMb,
    func_args: Option<String>,
) -> Result<ThreadResult> {
    let factory = iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory::boxed();
    barrier.wait().await;

    let name = format!("scaling-{}", thread_cnt);
    let version = format!("{}", thread_id);
    let (reg_result, reg_tid) = match worker_register(
        name.clone(),
        &version,
        image,
        memory_mb,
        host.clone(),
        port,
        &factory,
        isolation,
        compute,
        server,
        None,
    )
    .await
    {
        Ok((s, reg_dur, tid)) => (
            RegistrationResult {
                duration_us: reg_dur.as_micros(),
                result: s,
            },
            tid,
        ),
        Err(e) => {
            error!("thread {} registration failed because {}", thread_id, e);
            std::process::exit(1);
        },
    };
    barrier.wait().await;

    let mut errors = "Prewarm errors:".to_string();
    let mut it = (1..4).peekable();
    while let Some(i) = it.next() {
        let wait = rand::rng().random_range(0..5000);
        tokio::time::sleep(Duration::from_millis(wait)).await;
        match worker_prewarm(&name, &version, &host, port, &reg_tid, &factory, compute).await {
            Ok((_s, _prewarm_dur)) => break,
            Err(e) => {
                errors = format!("{} iteration {}: '{}';\n", errors, i, e);
                if it.peek().is_none() {
                    error!("thread {} prewarm failed because {}", thread_id, errors);
                    std::process::exit(1);
                }
            },
        };
    }
    barrier.wait().await;

    let stopping = Duration::from_secs(duration);
    let start = now();
    let mut data = Vec::new();
    let mut errors = 0;
    let clock = get_global_clock(&gen_tid())?;
    let mut dummy = crate::trace::Function::default();
    loop {
        let tid = format!("{}-{}", thread_id, gen_tid());
        let args = match &func_args {
            Some(arg) => {
                dummy.args = Some(arg.clone());
                args_to_json(&prepare_function_args(&dummy, crate::utils::LoadType::Functions))?
            },
            None => "{\"name\":\"TESTING\"}".to_string(),
        };
        match worker_invoke(&name, &version, &host, port, &tid, Some(args), clock.clone(), &factory).await {
            Ok(worker_invocation) => {
                data.push(worker_invocation);
            },
            Err(_) => {
                errors += 1;
                continue;
            },
        };

        if start.elapsed() > stopping {
            break;
        }
    }
    Ok(ThreadResult {
        thread_id,
        data,
        errors,
        registration: reg_result,
    })
}
