use std::{time::{Duration, SystemTime}, sync::Arc};
use clap::{ArgMatches, App, SubCommand, Arg};
use anyhow::Result;
use iluvatar_library::utils::{config::get_val, port_utils::Port, file_utils::ensure_dir};
use tokio::sync::Barrier;
use tokio::runtime::Builder;
use crate::utils::{ThreadResult, RegistrationResult, worker_register, worker_invoke, resolve_handles, ErrorHandling, save_result_json};
use std::path::Path;

pub fn trace_args<'a>(app: App<'a>) -> App<'a> {
  app.subcommand(SubCommand::with_name("scaling")
    .about("Test scaling of worker with increasing amount of requests")
    .arg(Arg::with_name("start")
      .short('s')
      .long("start")
      .help("Number of threads to start")
      .required(false)
      .takes_value(true)
      .default_value("1"))
    .arg(Arg::with_name("end")
      .short('e')
      .long("end")
      .help("Number of threads to reach")
      .required(false)
      .takes_value(true)
      .default_value("1"))
    .arg(Arg::with_name("duration")
      .short('d')
      .long("duration")
      .help("Duration in seconds before increasing load")
      .required(false)
      .takes_value(true)
      .default_value("5"))
    .arg(Arg::with_name("image")
      .short('i')
      .long("image")
      .help("The image to use")
      .required(false)
      .takes_value(true)
      .default_value("docker.io/alfuerst/hello-iluvatar-action:latest"))
  )
}

pub fn scaling(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;
  let folder: String = get_val("out", &main_args)?;
  ensure_dir(&std::path::PathBuf::new().join(&folder))?;

  let thread_start: usize = get_val("start", &sub_args)?;
  let thread_end: usize = get_val("end", &sub_args)?;
  let duration_sec: u64 = get_val("duration", &sub_args)?;
  let image: String = get_val("image", &sub_args)?;

  for threads in thread_start..(thread_end+1) {
    println!("\n Running with {} threads", threads);
    let result = run_one_scaling_test(threads, host.clone(), port, duration_sec, image.clone())?;
    let p = Path::new(&folder).join(format!("{}.json", threads));
    save_result_json(p, &result)?;
  }

  Ok(())
}

fn run_one_scaling_test(thread_cnt: usize, host: String, port: Port, duration_sec: u64, image: String) -> Result<Vec<ThreadResult>> {
  let barrier = Arc::new(Barrier::new(thread_cnt));
  let threaded_rt = Builder::new_multi_thread()
                        .worker_threads(thread_cnt)
                        .enable_all()
                        .build().unwrap();

  let mut threads = Vec::new();

  for thread_id in 0..thread_cnt {
    let host_c = host.clone();
    let b = barrier.clone();
    let i_c = image.clone();
    threads.push(threaded_rt.spawn(async move {
      scaling_thread(host_c, port, duration_sec, thread_id, b, i_c).await
    }));
  }

  resolve_handles(&threaded_rt, threads, ErrorHandling::Print)
}

async fn scaling_thread(host: String, port: Port, duration: u64, thread_id: usize, barrier: Arc<Barrier>, image: String) -> Result<ThreadResult> {
  barrier.wait().await;

  let name = format!("scaling-{}", thread_id);
  let version = format!("0.0.{}", thread_id);
  let (reg_result, tid) = match worker_register(name.clone(), &version, image, 512, host.clone(), port).await {
    Ok((s, reg_dur, tid)) => (RegistrationResult {
      duration_us: reg_dur.as_micros(),
      result: s
    }, tid),
    Err(e) => anyhow::bail!("thread {} registration failed because {}", thread_id, e),
  };
  
  barrier.wait().await;

  let stopping = Duration::from_secs(duration);
  let start = SystemTime::now();
  let mut data = Vec::new();
  let mut errors = 0;
  loop {
    match worker_invoke(&name, &version, &host, port, &tid, Some("{\"name\":\"TESTING\"}".to_string())).await {
      Ok( worker_invocation ) => {
        data.push(worker_invocation);
      },
      Err(_) => {
        errors = errors + 1;
        continue;
      },
    };

    if start.elapsed()? > stopping {
      break;
    }
  }
  let ret = ThreadResult {
    thread_id,
    data,
    errors,
    registration: reg_result
  };

  Ok(ret)
}
