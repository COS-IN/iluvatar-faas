use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
// use clap::{ArgMatches, App, SubCommand, Arg};
use crate::utils::{
    resolve_handles, save_result_json, worker_invoke, worker_prewarm, worker_register, ErrorHandling,
    RegistrationResult, Target, ThreadResult,
};
use anyhow::Result;
use clap::Parser;
use iluvatar_library::{
    logging::LocalTime,
    transaction::gen_tid,
    types::{Compute, ComputeEnum, Isolation, IsolationEnum},
    utils::{file_utils::ensure_dir, port_utils::Port},
};
use rand::prelude::*;
use std::path::Path;
use tokio::runtime::Builder;
use tokio::sync::Barrier;

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
    /// Folder to output results to
    out_folder: String,
    #[arg(long)]
    /// Isolation the image will use
    isolation: IsolationEnum,
    #[arg(long)]
    /// Compute the image will use
    compute: ComputeEnum,
}

pub fn scaling(args: ScalingArgs) -> Result<()> {
    ensure_dir(&std::path::PathBuf::new().join(&args.out_folder))?;

    for threads in args.start..(args.end + 1) {
        println!("\n Running with {} threads", threads);
        let result = run_one_scaling_test(threads as usize, &args)?;
        let p = Path::new(&args.out_folder).join(format!("{}.json", threads));
        save_result_json(p, &result)?;
    }

    Ok(())
}

fn run_one_scaling_test(thread_cnt: usize, args: &ScalingArgs) -> Result<Vec<ThreadResult>> {
    let barrier = Arc::new(Barrier::new(thread_cnt));
    let threaded_rt = Builder::new_multi_thread()
        .worker_threads(thread_cnt)
        .enable_all()
        .build()
        .unwrap();

    let mut threads = Vec::new();

    for thread_id in 0..thread_cnt {
        let host_c = args.host.clone();
        let b = barrier.clone();
        let i_c = args.image.clone();
        let compute = (&args.compute).into();
        let isolation = (&args.isolation).into();
        let p = args.port;
        let d = args.duration.into();

        threads.push(threaded_rt.spawn(async move {
            scaling_thread(host_c, p, d, thread_id, b, i_c, compute, isolation, thread_cnt).await
        }));
    }

    resolve_handles(&threaded_rt, threads, ErrorHandling::Print)
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
    thread_cnt: usize,
) -> Result<ThreadResult> {
    let factory = iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory::boxed();
    barrier.wait().await;

    let name = format!("scaling-{}", thread_cnt);
    let version = format!("{}", thread_id);
    let (reg_result, reg_tid) = match worker_register(
        name.clone(),
        &version,
        image,
        512,
        host.clone(),
        port,
        &factory,
        None,
        isolation,
        compute,
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
            println!("thread {} registration failed because {}", thread_id, e);
            std::process::exit(1);
        }
    };
    barrier.wait().await;

    let mut errors = "Prewarm errors:".to_string();
    let mut it = (1..4).into_iter().peekable();
    while let Some(i) = it.next() {
        let wait = rand::thread_rng().gen_range(0..5000);
        tokio::time::sleep(Duration::from_millis(wait)).await;
        match worker_prewarm(&name, &version, &host, port, &reg_tid, &factory, None, compute).await {
            Ok((_s, _prewarm_dur)) => break,
            Err(e) => {
                errors = format!("{} iteration {}: '{}';\n", errors, i, e);
                if it.peek().is_none() {
                    println!("thread {} prewarm failed because {}", thread_id, errors);
                    std::process::exit(1);
                }
            }
        };
    }
    barrier.wait().await;

    let stopping = Duration::from_secs(duration);
    let start = SystemTime::now();
    let mut data = Vec::new();
    let mut errors = 0;
    let clock = Arc::new(LocalTime::new(&gen_tid())?);
    loop {
        let tid = format!("{}-{}", thread_id, gen_tid());
        match worker_invoke(
            &name,
            &version,
            &host,
            port,
            &tid,
            Some("{\"name\":\"TESTING\"}".to_string()),
            clock.clone(),
            &factory,
            None,
        )
        .await
        {
            Ok(worker_invocation) => {
                data.push(worker_invocation);
            }
            Err(_) => {
                errors = errors + 1;
                continue;
            }
        };

        if start.elapsed()? > stopping {
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
