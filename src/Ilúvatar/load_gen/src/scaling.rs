use std::{time::{Duration, SystemTime}, sync::Arc};
use clap::{ArgMatches, App, SubCommand, Arg};
use anyhow::Result;
use iluvatar_lib::utils::{config::get_val, port_utils::Port, file_utils::ensure_dir};
use tokio::sync::Barrier;
use tokio::runtime::Builder;
use crate::utils::{InvocationResult, ThreadResult, RegistrationResult, worker_register, worker_invoke};
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub fn trace_args<'a>(app: App<'a, 'a>) -> App<'a, 'a> {
  app.subcommand(SubCommand::with_name("scaling")
    .about("Test scaling of worker with increasing amount of requests")
    .arg(Arg::with_name("start")
      .short("s")
      .long("start")
      .help("Number of threads to start")
      .required(false)
      .takes_value(true)
      .default_value("1"))
    .arg(Arg::with_name("end")
      .short("e")
      .long("end")
      .help("Number of threads to reach")
      .required(false)
      .takes_value(true)
      .default_value("1"))
    .arg(Arg::with_name("duration")
      .short("d")
      .long("duration")
      .help("Duration in seconds before increasing load")
      .required(false)
      .takes_value(true)
      .default_value("5"))
  )
}

pub fn scaling(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  // let worker_name: String = get_val("worker", &main_args)?;
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;
  let folder: String = get_val("out", &main_args)?;
  ensure_dir(&std::path::PathBuf::new().join(&folder))?;
  let iterations: u64 = get_val("iterations", &main_args)?;

  let thread_start: usize = get_val("start", &sub_args)?;
  let thread_end: usize = get_val("end", &sub_args)?;
  let duration_sec: u64 = get_val("duration", &sub_args)?;


  for i in 0..iterations {
    for threads in thread_start..(thread_end+1) {
      println!("\n Running with {} threads", threads);
      let result = run_one_scaling_test(threads, host.clone(), port, duration_sec);
      let to_write = match serde_json::to_string::<Vec<ThreadResult>>(&result) {
        Ok(s) => s,
        Err(e) => {
          println!("Failed to get json of result because {}", e);
          continue;
        }
      };
      let p = Path::new(&folder).join(format!("{}-{}.json", threads, i));
      let mut f = match File::create(p) {
        Ok(f) => f,
        Err(e) => {
          println!("Failed to create output file because {}", e);
          continue;
        }
      };
      match f.write_all(to_write.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
          println!("Failed to write json of result because {}", e);
          continue;
        }
      };
    }
  }

  Ok(())
}

fn run_one_scaling_test(thread_cnt: usize, host: String, port: Port, duration_sec: u64) -> Vec<ThreadResult> {
  let barrier = Arc::new(Barrier::new(thread_cnt));
  let threaded_rt = Builder::new_multi_thread()
                        .worker_threads(thread_cnt)
                        .enable_all()
                        .build().unwrap();

  let mut threads = Vec::new();

  for thread_id in 0..thread_cnt {
    let host_c = host.clone();
    let b = barrier.clone();
    threads.push(threaded_rt.spawn(async move {
      scaling_thread(host_c, port, duration_sec, thread_id, b).await
    }));
  }

  let mut results = Vec::new();

  for t in threads {
    results.push(match threaded_rt.block_on(t) {
      Ok(r) => match r {
        Ok(ret) => {
          println!("Threadid {} had {} invocations and {} errors", ret.thread_id, ret.data.len(), ret.errors);
          ret
        },
        Err(e) => {
          println!("thread error: {}", e);
          continue;
        },
      },
      Err(e) => {
        println!("joining error: {}", e);
        continue;
      },
    });
  }
  results
}

async fn scaling_thread(host: String, port: Port, duration: u64, thread_id: usize, barrier: Arc<Barrier>) -> Result<ThreadResult> {
  barrier.wait().await;

  let name = format!("scaling-{}", thread_id);
  let version = format!("0.0.{}", thread_id);
  let image = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
  let (reg_result, tid) = match worker_register(&name, &version, &image, 512, &host, port).await {
    Ok((s, reg_dur, tid)) => (RegistrationResult {
      duration_ms: reg_dur.as_millis() as u64,
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
      Ok( (_response, invok_out, invok_lat) ) => {
        let res = InvocationResult {
          duration_ms: invok_lat,
          json: invok_out
        };
        data.push(res);
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
