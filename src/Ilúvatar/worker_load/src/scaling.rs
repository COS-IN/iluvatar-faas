use std::{time::{Duration, SystemTime}, sync::Arc};

use clap::ArgMatches;
use anyhow::Result;
use iluvatar_lib::{utils::{config::get_val, port_utils::Port, file_utils::ensure_dir}, rpc::RCPWorkerAPI, ilúvatar_api::WorkerAPI, transaction::{gen_tid, TransactionId}};
use tokio::sync::Barrier;
use tokio::runtime::Builder;
use crate::utils::{self, InvocationResult, ThreadResult, HelloResult, RegistrationResult};
use std::fs::File;
use std::io::Write;
use std::path::Path;

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
  let mut api = RCPWorkerAPI::new(&host, port).await?;

  barrier.wait().await;

  let name = format!("scaling-{}", thread_id);
  let version = format!("0.0.{}", thread_id);
  let image = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
  let tid: TransactionId = gen_tid();

  let (_reg_start, reg_dur, reg_out) = utils::time(api.register(name.clone(), version.clone(), image, 75, 1, 1, tid.clone())
  ).await?;
  let reg_result = match reg_out {
    Ok(s) => RegistrationResult {
      duration_ms: reg_dur,
      result: s
    },
    Err(e) => anyhow::bail!("thread {} registration failed because {}", thread_id, e),
  };
  
  barrier.wait().await;

  let stopping = Duration::from_secs(duration);
  let start = SystemTime::now();
  let mut data = Vec::new();
  let mut errors = 0;
  loop {
    let (_invok_start, invok_dur, invok_out) = match utils::time(api.invoke(name.clone(), version.clone(), "{\"name\":\"TESTING\"}".to_string(), None, tid.clone())
    ).await {
      Ok(r) => r,
      Err(_) => {
        errors = errors + 1;
        continue;
      },
    };
    
    let body = match invok_out {
      Ok(r) => match serde_json::from_str::<HelloResult>(&r) {
        Ok(b) => b,
        Err(_) => {
          errors = errors + 1;
          continue;
        },
      },
      Err(_) => {
        errors = errors + 1;
        continue;
      },
    };

    let res = InvocationResult {
      duration_ms: invok_dur,
      json: body
    };
    data.push(res);

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