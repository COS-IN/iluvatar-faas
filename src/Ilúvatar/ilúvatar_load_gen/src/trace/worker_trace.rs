use std::{collections::HashMap, sync::Arc, path::Path, fs::File, io::Write, time::Duration};
use anyhow::Result;
use iluvatar_library::{utils::{config::{get_val, args_to_json}, timing::TimedExt, port::Port}, transaction::{TransactionId, gen_tid}};
use iluvatar_worker_library::{worker_api::{create_worker, ilúvatar_worker::IluvatarWorkerImpl}, services::containers::simulation::simstructs::SimulationResult};
use iluvatar_worker_library::{services::containers::simulation::simstructs::SimulationInvocation};
use clap::ArgMatches;
use tokio::{runtime::{Builder, Runtime}, task::JoinHandle};
use std::time::SystemTime;
use iluvatar_worker_library::rpc::{iluvatar_worker_server::IluvatarWorker, InvokeRequest, RegisterRequest, RegisterResponse, InvokeResponse};
use tonic::{Request, Status, Response};
use crate::{utils::{worker_register, VERSION, worker_invoke, FunctionExecOutput}, trace::{match_trace_to_img, prepare_function_args}, benchmark::BenchmarkStore};
use super::{Function, CsvInvocation};

fn sim_register_functions(funcs: &HashMap<u64, Function>, worker: Arc<IluvatarWorkerImpl>, rt: &Runtime) -> Result<()> {
  let mut handles: Vec<JoinHandle<Result<Response<RegisterResponse>, Status>>> = Vec::new();
  for (_k, v) in funcs.into_iter() {
    let r = RegisterRequest {
      function_name: v.function_id.to_string(),
      function_version: "0.0.1".to_string(),
      image_name: "".to_string(),
      memory: v.mem_mb,
      cpus: 1, 
      parallel_invokes: 1,
      transaction_id : gen_tid()
    };
    let cln = worker.clone();
    handles.push(rt.spawn(async move {
      cln.register(Request::new(r)).await
    }));
  }
  for h in handles {
    rt.block_on(h)??;
  }
  Ok(())
}

pub fn trace_worker(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let setup: String = get_val("setup", &sub_args)?;
  match setup.as_str() {
    "simulation" => simulated_worker(main_args, sub_args),
    "live" => live_worker(main_args, sub_args),
    _ => anyhow::bail!("Unknown setup for trace run '{}'; only supports 'simulation' and 'live'", setup)
  }
}

fn simulated_worker(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let config_pth: String = get_val("worker-config", &sub_args)?;
  let server_config = iluvatar_worker_library::worker_api::worker_config::Configuration::boxed(false, &config_pth).unwrap();
  let tid: &TransactionId = &iluvatar_library::transaction::SIMULATION_START_TID;
  let threaded_rt = Builder::new_multi_thread()
                        .enable_all()
                        .build().unwrap();

  let _guard = iluvatar_library::logging::start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name)?;
  let worker = Arc::new(threaded_rt.block_on(create_worker(server_config.clone(), tid))?);

  let trace_pth: String = get_val("input", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;
  let metadata = super::load_metadata(metadata_pth)?;
  sim_register_functions(&metadata, worker.clone(), &threaded_rt)?;

  let mut trace_rdr = csv::Reader::from_path(&trace_pth)?;
  let mut handles: Vec<JoinHandle<Result<(u64, InvokeResponse)>>> = Vec::new();

  println!("starting simulation run");

  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invoke: CsvInvocation = result?;
    let func = metadata.get(&invoke.function_id).unwrap();

    let args = SimulationInvocation {
      warm_dur_ms: func.warm_dur_ms,
      cold_dur_ms: func.cold_dur_ms,
    };

    let req = InvokeRequest {
      function_name: func.function_id.to_string(),
      function_version: "0.0.1".to_string(),
      memory: func.mem_mb,
      json_args: serde_json::to_string(&args)?,
      transaction_id: gen_tid()
    };
    loop {
      match start.elapsed(){
        Ok(t) => {
          if t.as_millis() >= invoke.invoke_time_ms as u128 {
            break;
          }
        },
        Err(_) => (),
      }
    };
    let cln = worker.clone();
    handles.push(threaded_rt.spawn(async move {
      let (reg_out, reg_dur) = cln.invoke(Request::new(req)).timed().await;
      Ok((reg_dur.as_millis() as u64, reg_out?.into_inner()))
    }));
  }

  let pth = Path::new(&trace_pth);
  let output_folder: String = get_val("out", &main_args)?;
  let p = Path::new(&output_folder).join(format!("output-{}", pth.file_name().unwrap().to_str().unwrap()));
  let mut f = match File::create(p) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };
  let to_write = format!("success,function_name,was_cold,worker_duration_ms,code_duration_ms,e2e_duration_ms\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json of result because {}", e);
    }
  };

  for h in handles {
    match threaded_rt.block_on(h) {
      Ok(r) => match r {
        Ok( (e2e_dur, resp) ) => {
          let result = serde_json::from_str::<SimulationResult>(&resp.json_result)?;
          let to_write = format!("{},{},{},{},{},{}\n", resp.success, result.function_name, result.was_cold, resp.duration_ms, result.duration_ms, e2e_dur);
          match f.write_all(to_write.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
              println!("Failed to write result because {}", e);
              continue;
            }
          };
        },
        Err(e) => println!("Status error from invoke: {}", e),
      },
      Err(thread_e) => println!("Joining error: {}", thread_e),
    };
  }

  Ok(())
}

fn live_register_functions(funcs: &HashMap<u64, Function>, host: &String, port: Port, load_type: &str, func_data: Result<String>, rt: &Runtime) -> Result<()> {
  let data = match load_type {
    "lookbusy" => Vec::new(),
    "functions" => {
      let func_data = func_data?;
      let contents = std::fs::read_to_string(func_data).expect("Something went wrong reading the file");
      match serde_json::from_str::<BenchmarkStore>(&contents) {
        Ok(d) => {
          let mut data = Vec::new();
          for (k, v) in d.data.iter() {
            let tot: f64 = v.warm_results.iter().sum();
            let avg_warm = tot / v.warm_results.len() as f64;
            data.push( (k.clone(), avg_warm) );
          }
          data
        },
        Err(e) => anyhow::bail!("Failed to read and parse benchmark data! '{}'", e),
      }
    },
    _ => panic!("Bad invocation load type: {}", load_type),
  };

  let mut func_iter = funcs.into_iter();
  while !wait_reg(&mut func_iter, load_type, rt, port, host, &data)?  {}
  Ok(())
}

fn wait_reg(iterator: &mut std::collections::hash_map::Iter<u64, Function>, load_type: &str, rt: &Runtime, port: Port, host: &String, data: &Vec<(String, f64)>) -> Result<bool> {
  let mut done = false;
  let mut handles: Vec<JoinHandle<Result<(String, Duration, TransactionId)>>> = Vec::new();
  for _ in 0..40 {
    let (_id, func) = match iterator.next() {
      Some(d) => d,
      None => {
        done = true;
        break;
      },
    };
    let image = match load_type {
      "lookbusy" => format!("docker.io/alfuerst/lookbusy-iluvatar-action:latest"),
      "functions" => match_trace_to_img(func, data),
      _ => panic!("Bad invocation load type: {}", load_type),
    };
    let f_c = func.function_id.to_string();
    let h_c = host.clone();
    let mb = func.mem_mb;
    handles.push(rt.spawn(async move { worker_register(f_c, &VERSION, image, mb+50, h_c, port).await }));
  }
  for h in handles {
    let (_s,_d,_s2) = rt.block_on(h)??;
  }
  Ok(done)
}

fn live_worker(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let load_type: String = get_val("load-type", &sub_args)?;
  let func_data: Result<String> = get_val("function-data", &sub_args);
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;

  let threaded_rt = Builder::new_multi_thread()
                        .enable_all()
                        .build().unwrap();

  let trace_pth: String = get_val("input", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;
  let metadata = super::load_metadata(metadata_pth)?;
  live_register_functions(&metadata, &host, port, &load_type, func_data, &threaded_rt)?;

  let mut trace_rdr = csv::Reader::from_path(&trace_pth)?;
  let mut handles: Vec<JoinHandle<(Result<(InvokeResponse, FunctionExecOutput, u64)>, String)>> = Vec::new();

  println!("starting live trace run");
  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invoke: CsvInvocation = result.expect("Error deserializing csv invocation");
    let func = metadata.get(&invoke.function_id).unwrap();
    let h_c = host.clone();
    let f_c = func.function_id.to_string();
    let args = args_to_json(prepare_function_args(func, &load_type));
    loop {
      match start.elapsed(){
        Ok(t) => {
          if t.as_millis() >= invoke.invoke_time_ms as u128 {
            break;
          }
        },
        Err(_) => (),
      }
    };
    
    handles.push(threaded_rt.spawn(async move {
      (worker_invoke(&f_c, &VERSION, &h_c, port, &gen_tid(), Some(args)).await, f_c)
    }));
  }

  let pth = Path::new(&trace_pth);
  let output_folder: String = get_val("out", &main_args)?;
  let p = Path::new(&output_folder).join(format!("output-{}", pth.file_name().unwrap().to_str().unwrap()));
  let mut f = match File::create(p) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };
  let to_write = format!("success,function_name,was_cold,worker_duration_ms,code_duration_sec,e2e_duration_ms\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json of result because {}", e);
    }
  };

  for h in handles {
    match threaded_rt.block_on(h) {
      Ok( (r, fname) ) => match r {
        Ok( (resp, invok_out, e2e_dur) ) => {
          let to_write = format!("{},{},{},{},{},{}\n", resp.success, fname, invok_out.body.cold, resp.duration_ms, invok_out.body.latency, e2e_dur);
          match f.write_all(to_write.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
              println!("Failed to write result because {}", e);
              continue;
            }
          };
        },
        Err(e) => println!("Status error from invoke: {}", e),
      },
      Err(thread_e) => println!("Joining error: {}", thread_e),
    };
  }
  Ok(())
}
