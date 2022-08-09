use std::{collections::HashMap, fs::File, path::Path, io::Write};
use clap::{ArgMatches, App, SubCommand, Arg};
use anyhow::Result;
use iluvatar_lib::utils::{config::get_val, port_utils::Port};
use serde::{Serialize, Deserialize};
use tokio::runtime::Builder;
use crate::utils::*;

#[derive(Serialize, Deserialize)]
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
pub struct FunctionStore {
  /// list of warm latencies
  pub warm_results: Vec<f64>,
  /// list of warm overhead times
  pub warm_over_results: Vec<f64>,
  /// list of cold latencies
  pub cold_results: Vec<f64>,
  /// list of cold overhead times
  pub cold_over_results: Vec<f64>,
  /// warm invocation latency time
  ///   if targeting worker, recorded by benchmark
  ///   if targeting controller, recorded by controller 
  pub warm_worker_duration_ms: Vec<u64>,
  /// cold invocation latency time recorded by benchmark 
  ///   if targeting worker, recorded by benchmark
  ///   if targeting controller, recorded by controller 
  pub cold_worker_duration_ms: Vec<u64>,
  /// warm invocation latency time recorded on worker 
  pub warm_invoke_duration_ms: Vec<u64>,
  /// cold invocation latency time recorded on worker 
  pub cold_invoke_duration_ms: Vec<u64>,
}
impl FunctionStore {
  pub fn new() -> Self {
    FunctionStore {
      warm_results: Vec::new(),
      warm_over_results: Vec::new(),
      cold_results: Vec::new(),
      cold_over_results: Vec::new(),
      warm_worker_duration_ms: Vec::new(),
      cold_worker_duration_ms: Vec::new(),
      warm_invoke_duration_ms: Vec::new(),
      cold_invoke_duration_ms: Vec::new(),
    }
  }
}

pub fn trace_args<'a>(app: App<'a, 'a>) -> App<'a, 'a> {
  app.subcommand(SubCommand::with_name("benchmark")
    .about("Benchmark functions through the system")
    .arg(Arg::with_name("target")
        .short("t")
        .long("target")
        .help("Target for the load, either 'worker' or 'controller'")
        .required(false)
        .takes_value(true)
        .default_value("worker"))
    .arg(Arg::with_name("functions-dir")
        .long("functions-dir")
        .help("The directory with all the functions to be benchmarked inside it, each in their own folder")
        .required(true)
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
  )
}

pub fn benchmark_functions(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let directory: String = get_val("functions-dir", &sub_args)?;
  let target: String = get_val("target", &sub_args)?;
  let cold_repeats: u32 = get_val("cold-iters", &sub_args)?;
  let warm_repeats: u32 = get_val("warm-iters", &sub_args)?;
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;
  let folder: String = get_val("out", &main_args)?;
  let mut functions = Vec::new();

  let paths = std::fs::read_dir(directory)?;
  for path in paths {
    functions.push(path?.file_name().to_str().unwrap().to_string());
  }

  println!("Benchmarking functions: {:?}", functions);

  let threaded_rt = Builder::new_multi_thread()
      .enable_all()
      .build().unwrap();

  match target.as_str() {
    "worker" => threaded_rt.block_on(benchmark_worker(host, port, functions, folder, cold_repeats, warm_repeats)),
    "controller" => threaded_rt.block_on(benchmark_controller(host, port, functions, folder, cold_repeats, warm_repeats)),
    _ => anyhow::bail!("Unknown benchmark target: {}", target)
  }
}

pub async fn benchmark_controller(host: String, port: Port, functions: Vec<String>, out_folder: String, cold_repeats: u32, warm_repeats: u32) -> Result<()> {
  let mut full_data = BenchmarkStore::new();

  for function in &functions {
    let mut func_data = FunctionStore::new();
    println!("{}", function);
    for iter in 0..cold_repeats {
      let name = format!("{}-bench-{}", function, iter);
      let version = format!("0.0.{}", iter);
      let image = format!("docker.io/alfuerst/{}-iluvatar-action:latest", function);

      let _reg_dur = match crate::utils::controller_register(&name, &version, &image, 512, &host, port).await {
        Ok(d) => d,
        Err(e) => {
          println!("{}", e);
          continue;
        }
      };

      'inner: for _ in 0..warm_repeats {
        match crate::utils::controller_invoke(&name, &version, &host, port, None).await {
          Ok( (api_result, invok_lat) ) => match serde_json::from_str::<FunctionExecOutput>(&api_result.json_result) {
            Ok(b) => {
              let func_exec_ms = b.body.latency * 1000.0;
              if b.body.cold {
                func_data.cold_results.push(invok_lat);
                func_data.cold_over_results.push(invok_lat - func_exec_ms);
                func_data.cold_worker_duration_ms.push(api_result.worker_duration_ms);
                func_data.cold_invoke_duration_ms.push(api_result.invoke_duration_ms);
              } else {
                func_data.warm_results.push(invok_lat);
                func_data.warm_over_results.push(invok_lat - func_exec_ms);
                func_data.warm_worker_duration_ms.push(api_result.worker_duration_ms);
                func_data.warm_invoke_duration_ms.push(api_result.invoke_duration_ms);
              }
            },
            Err(e) => {
              println!("RealInvokeResult Deserialization error: {}; {}", e, &api_result.json_result);
              break 'inner;
            },
          },
          Err(e) => {
            println!("{}", e);
            break 'inner;
          },
        }
      }
    }
    full_data.data.insert(function.clone(), func_data);
  }

  let p = Path::new(&out_folder).join(format!("controller_function_benchmarks.json"));
  let mut f = match File::create(p) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };

  let to_write = serde_json::to_string(&full_data)?;
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      println!("Failed to write json of result because {}", e);
    }
  };
  Ok(())
}

pub async fn benchmark_worker(host: String, port: Port, functions: Vec<String>, out_folder: String, cold_repeats: u32, warm_repeats: u32) -> Result<()> {
  let mut full_data = BenchmarkStore::new();

  for function in &functions {
    println!("{}", function);
    let mut func_data = FunctionStore::new();
    for iter in 0..cold_repeats {
      let name = format!("{}-bench-{}", function, iter);
      let version = format!("0.0.{}", iter);
      let image = format!("docker.io/alfuerst/{}-iluvatar-action:latest", function);
      let (_s, _reg_dur, tid) = match worker_register(&name, &version, &image, 512, &host, port).await {
        Ok(r) => r,
        Err(e) => {
          println!("{}", e);
          continue;
        },
      };

      for _ in 0..warm_repeats {
        match worker_invoke(&name, &version, &host, port, &tid, None).await {
          Ok( (response, invok_out, invok_lat) ) => {
            let invok_lat_f = invok_lat as f64;
            let func_exec_ms = invok_out.body.latency * 1000.0;
            if invok_out.body.cold {
              func_data.cold_results.push(invok_lat_f);
              func_data.cold_over_results.push(invok_lat_f - func_exec_ms);
              func_data.cold_worker_duration_ms.push(response.duration_ms);
              func_data.cold_invoke_duration_ms.push(invok_lat);
            } else {
              func_data.warm_results.push(invok_lat_f);
              func_data.warm_over_results.push(invok_lat_f - func_exec_ms);
              func_data.warm_worker_duration_ms.push(response.duration_ms);
              func_data.warm_invoke_duration_ms.push(invok_lat);
            }
          },
          Err(e) => {
            println!("{}", e);
            continue;
          },
        };
      }
    }
    full_data.data.insert(function.clone(), func_data);
  }

  let p = Path::new(&out_folder).join(format!("worker_function_benchmarks.json"));
  let mut f = match File::create(p) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };
  let to_write = serde_json::to_string(&full_data)?;
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      println!("Failed to write json of result because {}", e);
    }
  };
  Ok(())
}