use std::sync::Arc;
use std::time::{SystemTime, Duration};
use std::{collections::HashMap, path::Path};
use clap::{ArgMatches, App, SubCommand, Arg};
use anyhow::Result;
use iluvatar_library::logging::LocalTime;
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::utils::{config::get_val, port_utils::Port};
use serde::{Serialize, Deserialize};
use tokio::sync::Barrier;
use tokio::runtime::{Builder, Runtime};
use crate::utils::*;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct ToBenchmarkFunction {
  pub name: String,
  pub image_name: String,
}

#[derive(Serialize, Deserialize)]
/// Stores the benchmark data from any number of functions
pub struct BenchmarkStore {
  /// map of function name to data
  pub data: HashMap<String, FunctionStore>,
}
impl BenchmarkStore {
  pub fn new() -> Self {
    BenchmarkStore {
      data: HashMap::new()
    }
  }
}
#[derive(Serialize, Deserialize)]
/// A struct to hold the benchmark results of a single function
pub struct FunctionStore {
  pub function_name: String,
  pub image_name: String,
  /// list of warm latencies
  pub warm_results: Vec<f64>,
  /// list of warm overhead times
  pub warm_over_results: Vec<f64>,
  /// list of cold latencies
  pub cold_results: Vec<f64>,
  /// list of cold overhead times
  pub cold_over_results: Vec<f64>,
  /// warm invocation latency time, including communication time from worker
  ///   if targeting worker, recorded by benchmark
  ///   if targeting controller, recorded by controller 
  pub warm_worker_duration_us: Vec<u128>,
  /// cold invocation latency time, including communication time from worker 
  ///   if targeting worker, recorded by benchmark
  ///   if targeting controller, recorded by controller 
  pub cold_worker_duration_us: Vec<u128>,
  /// warm invocation latency time recorded on worker 
  pub warm_invoke_duration_us: Vec<u128>,
  /// cold invocation latency time recorded on worker 
  pub cold_invoke_duration_us: Vec<u128>,
}
impl FunctionStore {
  pub fn new(image_name: String, function_name: String) -> Self {
    FunctionStore {
      function_name,
      image_name,
      warm_results: Vec::new(),
      warm_over_results: Vec::new(),
      cold_results: Vec::new(),
      cold_over_results: Vec::new(),
      warm_worker_duration_us: Vec::new(),
      cold_worker_duration_us: Vec::new(),
      warm_invoke_duration_us: Vec::new(),
      cold_invoke_duration_us: Vec::new(),
    }
  }
}

pub fn trace_args<'a>(app: App<'a>) -> App<'a> {
  app.subcommand(SubCommand::with_name("benchmark")
    .about("Benchmark functions through the system. Functions will be run by iteratively by themselves (or in parallel with themselves if using threads). All invocations will complete before a new function is run")
    .arg(Arg::with_name("target")
        .short('t')
        .long("target")
        .help("Target for the load, either 'worker' or 'controller'")
        .required(false)
        .takes_value(true)
        .default_value("worker"))
    .arg(Arg::with_name("functions-file")
        .long("functions-file")
        .help("The csv with all the functions to be benchmarked listed inside of it. In the form <f_name>,<f_image>")
        .required(false)
        .takes_value(true))
    .arg(Arg::with_name("functions-dir")
        .long("functions-dir")
        .help("The directory with all the functions to be benchmarked inside it, each in their own folder")
        .required(false)
        .takes_value(true))
    .arg(Arg::with_name("cold-iters")
        .long("cold-iters")
        .help("Number of times to run each function cold")
        .required(false)
        .takes_value(true)
        .default_value("10"))
    .arg(Arg::with_name("warm-iters")
        .long("warm-iters")
        .help("Number of times to run function _after_ each cold start, expecting them to be warm (could vary because of load balancer)")
        .required(false)
        .takes_value(true)
        .default_value("4"))
    .arg(Arg::with_name("run-time")
        .long("run-time")
        .help("Duration in minutes that each function will be run for, being invoked in a closed loop. An alternative to cold/warm-iters. Leaving as 0 will use iters")
        .required(false)
        .takes_value(true)
        .default_value("0"))
    .arg(Arg::with_name("threads")
        .long("threads")
        .help("Number of threads to run the benchmark with. Each thread will run the same function in parallel")
        .required(false)
        .takes_value(true)
        .default_value("1"))
  )
}

pub fn load_functions(sub_args: &ArgMatches) -> Result<Vec<ToBenchmarkFunction>> {
  let mut functions = Vec::new();

  if let Ok(directory) = get_val::<String>("functions-dir", &sub_args) {
    let paths = std::fs::read_dir(&directory).expect(&format!("was unable to read directory '{}'", directory).as_str());
    for path in paths {
      let pth = path.expect("Error reading directory entry");
      let fname = (&pth.file_name().to_str().expect(&format!("Unable to convert file name '{:?}' os_str to String", pth).as_str())).to_string();
      let image = format!("docker.io/alfuerst/{}-iluvatar-action:latest", &fname);
      functions.push( ToBenchmarkFunction{name:fname, image_name:image} );
    }
  } else if let Ok(directory) = get_val::<String>("functions-file", &sub_args) {
    let mut rdr = match csv::Reader::from_path(&directory) {
      Ok(r) => r,
      Err(e) => anyhow::bail!("Unable to open metadata csv file '{}' because of error '{}'", directory, e),
    };
    for result in rdr.deserialize() {
      let func: ToBenchmarkFunction = result.expect("Error deserializing ToBenchmarkFunction");
      functions.push(func);
    }
  } else {
    anyhow::bail!("Neither 'functions-file' nor 'functions-dir' was passed as an argument. Bailing");
  }
  Ok(functions)
}

pub fn benchmark_functions(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
 
  let target: String = get_val("target", &sub_args)?;
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;
  let folder: String = get_val("out", &main_args)?;
  let thread_cnt = get_val("threads", &sub_args)?;
  let cold_repeats: u32 = get_val("cold-iters", &sub_args)?;
  let warm_repeats: u32 = get_val("warm-iters", &sub_args)?;
  let duration = get_val("run-time", &sub_args)?;

  let functions = load_functions(sub_args)?;

  println!("Benchmarking functions: {:?}", functions);

  let threaded_rt = Builder::new_multi_thread()
      .enable_all()
      .build().unwrap();

  match target.as_str() {
    "worker" => {
      benchmark_worker(&threaded_rt, host, port, functions, folder, cold_repeats, warm_repeats, duration, thread_cnt)
    },
    // TODO: implement threads, cold/warm vs timed completion for controller
    "controller" => {
      threaded_rt.block_on(benchmark_controller(host, port, functions, folder, cold_repeats, warm_repeats))
    },
    _ => anyhow::bail!("Unknown benchmark target: {}", target)
  }
}

pub async fn benchmark_controller(host: String, port: Port, functions: Vec<ToBenchmarkFunction>, out_folder: String, cold_repeats: u32, warm_repeats: u32) -> Result<()> {
  let mut full_data = BenchmarkStore::new();

  for function in &functions {
    let mut func_data = FunctionStore::new(function.image_name.clone(), function.name.clone());
    println!("{}", function.name);
    let clock = Arc::new(LocalTime::new(&gen_tid())?);
    for iter in 0..cold_repeats {
      let name = format!("{}-bench-{}", function.name, iter);
      let version = format!("0.0.{}", iter);
      let _reg_dur = match crate::utils::controller_register(&name, &version, &function.image_name, 512, &host, port).await {
        Ok(d) => d,
        Err(e) => {
          println!("{}", e);
          continue;
        }
      };

      'inner: for _ in 0..warm_repeats {
        match crate::utils::controller_invoke(&name, &version, &host, port, None, clock.clone()).await {
          Ok( invoke_result ) => {
            if invoke_result.controller_response.success {
              let func_exec_us = invoke_result.function_output.body.latency * 1000000.0;
              let invoke_lat = invoke_result.client_latency_us as f64;
              if invoke_result.function_output.body.cold {
                func_data.cold_results.push(invoke_result.function_output.body.latency);
                func_data.cold_over_results.push(invoke_lat - func_exec_us);
                func_data.cold_worker_duration_us.push(invoke_result.controller_response.worker_duration_us);
                func_data.cold_invoke_duration_us.push(invoke_result.controller_response.invoke_duration_us);
              } else {
                func_data.warm_results.push(invoke_result.function_output.body.latency);
                func_data.warm_over_results.push(invoke_lat - func_exec_us);
                func_data.warm_worker_duration_us.push(invoke_result.controller_response.worker_duration_us);
                func_data.warm_invoke_duration_us.push(invoke_result.controller_response.invoke_duration_us);
              }
            }
          },
          Err(e) => {
            println!("{}", e);
            break 'inner;
          },
        }
      }
    }
    full_data.data.insert(function.name.clone(), func_data);
  }

  let p = Path::new(&out_folder).join(format!("controller_function_benchmarks.json"));
  save_result_json(p, &full_data)?;
  Ok(())
}

pub fn benchmark_worker(threaded_rt: &Runtime, host: String, port: Port, functions: Vec<ToBenchmarkFunction>, out_folder: String, cold_repeats: u32, warm_repeats: u32, duration_sec: u64, thread_cnt: usize) -> Result<()> {
  let barrier = Arc::new(Barrier::new(thread_cnt));
  let mut handles = Vec::new();
  let mut full_data = BenchmarkStore::new();
  for f in &functions {
    full_data.data.insert(f.name.clone(), FunctionStore::new(f.image_name.clone(), f.name.clone()));
  }

  for thread_id in 0..thread_cnt {
    let h_c = host.clone();
    let f_c = functions.clone();
    let b_c = barrier.clone();
    handles.push(threaded_rt.spawn(async move { benchmark_worker_thread(h_c, port, f_c, cold_repeats, warm_repeats, duration_sec, thread_id, b_c).await }));
  }

  let mut results = resolve_handles(threaded_rt, handles, crate::utils::ErrorHandling::Print)?;
  let mut combined = vec![];
  for thread_result in results.iter_mut() {
    combined.append(thread_result);
  }

  for invoke in &combined {
    let parts = invoke.function_name.split(".").collect::<Vec<&str>>();
    let d = full_data.data.get_mut(parts[0]).expect("Unable to find function in result hash, but it should have been there");
    let invok_lat_f = invoke.client_latency_us as f64;
    let func_exec_us = invoke.function_output.body.latency * 1000000.0;
    if invoke.function_output.body.cold {
      d.cold_results.push(invoke.function_output.body.latency);
      d.cold_over_results.push(invok_lat_f - func_exec_us);
      d.cold_worker_duration_us.push(invoke.worker_response.duration_us as u128);
      d.cold_invoke_duration_us.push(invoke.client_latency_us);
    } else {
      d.warm_results.push(invoke.function_output.body.latency);
      d.warm_over_results.push(invok_lat_f - func_exec_us);
      d.warm_worker_duration_us.push(invoke.worker_response.duration_us as u128);
      d.warm_invoke_duration_us.push(invoke.client_latency_us);
    }
  }

  let p = Path::new(&out_folder).join(format!("worker_function_benchmarks.json"));
  save_result_json(p, &full_data)?;
  let p = Path::new(&out_folder).join(format!("benchmark-full.json"));
  save_result_json(p, &combined)?;
  let p = Path::new(&out_folder).join("benchmark-output.csv".to_string());
  save_worker_result_csv(p, &combined)
}

async fn benchmark_worker_thread(host: String, port: Port, functions: Vec<ToBenchmarkFunction>, mut cold_repeats: u32, warm_repeats: u32, duration_sec: u64, thread_cnt: usize, barrier: Arc<Barrier>) -> Result<Vec<CompletedWorkerInvocation>> {
  let mut ret = vec![];
  let clock = Arc::new(LocalTime::new(&gen_tid())?);

  for function in &functions {
    println!("{}", &function.name);
    match duration_sec {
      0 => (),
      _ => {
        cold_repeats = 1;
      }
    };

    for iter in 0..cold_repeats {
      let name = format!("{}.{}.{}", &function.name, thread_cnt, iter);
      let version = iter.to_string();
      let (_s, _reg_dur, _tid) = match worker_register(name.clone(), &version, function.image_name.clone(), 512, host.clone(), port).await {
        Ok(r) => r,
        Err(e) => {
          println!("{}", e);
          continue;
        },
      };
      barrier.wait().await;

      if duration_sec != 0 {
        let timeout = Duration::from_secs(duration_sec);
        let start = SystemTime::now();
        while start.elapsed()? < timeout {
          match worker_invoke(&name, &version, &host, port, &gen_tid(), None, clock.clone()).await {
            Ok(r) => ret.push(r),
            Err(_) => continue,
          };
        }
      } else {
        for _ in 0..warm_repeats+1 {
          match worker_invoke(&name, &version, &host, port, &gen_tid(), None, clock.clone()).await {
            Ok(r) => ret.push(r),
            Err(_) => continue,
          };
        }  
      }
      barrier.wait().await;
    }
  }
  Ok(ret)
}
