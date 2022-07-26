use std::{time::{Duration, SystemTime}, sync::Arc};

use clap::ArgMatches;
use anyhow::Result;
use iluvatar_lib::{utils::{config::get_val, port_utils::Port}, rpc::RCPWorkerAPI, ilúvatar_api::WorkerAPI, transaction::{gen_tid, TransactionId}};
use tokio::sync::Barrier;
use tokio::runtime;
use crate::utils;

pub fn scaling(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  // let worker_name: String = get_val("worker", &main_args)?;
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;

  let thread_cnt: usize = get_val("threads", &sub_args)?;
  let duration_sec: u64 = get_val("duration", &sub_args)?;

  let barrier = Arc::new(Barrier::new(thread_cnt));
  let threaded_rt = runtime::Runtime::new()?;

  let mut threads = Vec::new();

  for thread_id in 0..thread_cnt {
    let host_c = host.clone();
    let b = barrier.clone();
    threads.push(threaded_rt.spawn(async move {
      scaling_thread(host_c, port, duration_sec, thread_id, b).await
    }));
  }

  for t in threads {
    match threaded_rt.block_on(t) {
      Ok(r) => match r {
        Ok( (tid, results) ) => {
          println!("Threadid {} had {} invocations", tid, results.len());
        },
        Err(e) => {
          println!("thread error: {}", e)
        },
      },
      Err(e) => println!("joining error: {}", e),
    }
  }
  Ok(())
}

async fn scaling_thread(host: String, port: Port, duration: u64, thread_id: usize, barrier: Arc<Barrier>) -> Result<(usize, Vec<(SystemTime, u64, Result<String, anyhow::Error >)>)> {
  let mut api = RCPWorkerAPI::new(&host, port).await?;

  barrier.wait().await;

  let name = format!("scaling-{}", thread_id);
  let version = format!("0.0.{}", thread_id);
  let image = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
  let tid: TransactionId = gen_tid();

  let (_reg_start, _reg_dur, reg_out) = utils::time(api.register(name.clone(), version.clone(), image, 75, 1, 1, tid.clone())
  ).await?;
  match reg_out {
    Ok(s) => s,
    Err(e) => anyhow::bail!("thread {} registration failed because {}", thread_id, e),
  };
  
  barrier.wait().await;

  let stopping = Duration::from_secs(duration);
  let start = SystemTime::now();
  let mut data = Vec::new();
  loop {
    let (invok_start, invok_dur, invok_out) = utils::time(api.invoke(name.clone(), version.clone(), "{\"name\":\"TESTING\"}".to_string(), None, tid.clone())
    ).await?;
      
    data.push( (invok_start, invok_dur, invok_out) );

    if start.elapsed()? > stopping {
      break;
    }
  }

  Ok( (thread_id, data) )
}