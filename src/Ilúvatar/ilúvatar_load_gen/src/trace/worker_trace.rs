use std::{collections::HashMap, sync::Arc, path::Path, fs::File, io::Write};
use anyhow::Result;
use iluvatar_library::{utils::{config::{get_val, args_to_json}, timing::TimedExt, port::Port}, transaction::{TransactionId, gen_tid}, logging::LocalTime};
use iluvatar_worker_library::{worker_api::{create_worker, ilúvatar_worker::IluvatarWorkerImpl}, services::containers::simulation::simstructs::SimulationResult};
use iluvatar_worker_library::{services::containers::simulation::simstructs::SimulationInvocation};
use clap::ArgMatches;
use tokio::{runtime::{Builder, Runtime}, task::JoinHandle};
use std::time::SystemTime;
use iluvatar_worker_library::rpc::{iluvatar_worker_server::IluvatarWorker, InvokeRequest, RegisterRequest, RegisterResponse, InvokeResponse};
use tonic::{Request, Status, Response};
use crate::{utils::{VERSION, worker_invoke, CompletedWorkerInvocation, resolve_handles, save_worker_result_csv, save_result_json}, trace::trace_utils::{prepare_functions, RegisterTarget}};
use crate::trace::prepare_function_args;
use super::{Function, CsvInvocation};

fn sim_register_functions(funcs: &HashMap<String, Function>, worker: Arc<IluvatarWorkerImpl>, rt: &Runtime) -> Result<()> {
  let mut handles: Vec<JoinHandle<Result<Response<RegisterResponse>, Status>>> = Vec::new();
  for (_k, v) in funcs.into_iter() {
    let r = RegisterRequest {
      function_name: v.func_name.clone(),
      function_version: "0.0.1".to_string(),
      image_name: "".to_string(),
      memory: v.mem_mb,
      cpus: 1, 
      parallel_invokes: 1,
      transaction_id : format!("{}-reg-tid", v.func_name)
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

  let _guard = iluvatar_library::logging::start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name, tid)?;
  let worker = threaded_rt.block_on(create_worker(server_config.clone(), tid))?;
  let worker = Arc::new(worker);

  let trace_pth: String = get_val("input", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;
  let metadata = super::load_metadata(metadata_pth)?;
  sim_register_functions(&metadata, worker.clone(), &threaded_rt)?;

  let mut trace_rdr = csv::Reader::from_path(&trace_pth)?;
  let mut handles: Vec<JoinHandle<Result<(u128, InvokeResponse)>>> = Vec::new();

  println!("starting simulation run");

  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invoke: CsvInvocation = result?;
    let func = metadata.get(&invoke.func_name).unwrap();

    let args = SimulationInvocation {
      warm_dur_ms: func.warm_dur_ms,
      cold_dur_ms: func.cold_dur_ms,
    };

    let req = InvokeRequest {
      function_name: func.func_name.clone(),
      function_version: VERSION.clone(),
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
      Ok((reg_dur.as_micros(), reg_out?.into_inner()))
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
  let to_write = format!("success,function_name,was_cold,worker_duration_us,code_duration_us,e2e_duration_us\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json of result because {}", e);
    }
  };

  for h in handles {
    match threaded_rt.block_on(h) {
      Ok(r) => match r {
        Ok( (e2e_dur_us, resp) ) => {
          let result = serde_json::from_str::<SimulationResult>(&resp.json_result)?;
          let to_write = format!("{},{},{},{},{},{}\n", resp.success, result.function_name, result.was_cold, resp.duration_us, result.duration_us, e2e_dur_us);
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

fn live_worker(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let load_type: String = get_val("load-type", &sub_args)?;
  let func_data: Result<String> = get_val("function-data", &sub_args);
  let prewarm_count: u32 = get_val("prewarm", &sub_args)?;
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;
  let tid: &TransactionId = &iluvatar_library::transaction::LIVE_WORKER_LOAD_TID;

  let threaded_rt = Builder::new_multi_thread()
                        .enable_all()
                        .build().unwrap();

  let trace_pth: String = get_val("input", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;
  let metadata = super::load_metadata(metadata_pth)?;

  prepare_functions(RegisterTarget::LiveWorker, &metadata, &host, port, &load_type, func_data, &threaded_rt, prewarm_count, &trace_pth)?;

  let mut trace_rdr = match csv::Reader::from_path(&trace_pth) {
    Ok(r) => r,
    Err(e) => anyhow::bail!("Unable to open trace csv file '{}' because of error '{}'", trace_pth, e),
  };
  let mut handles: Vec<JoinHandle<Result<CompletedWorkerInvocation>>> = Vec::new();
  let clock = Arc::new(LocalTime::new(tid)?);

  println!("{} starting live trace run", clock.now_str()?);
  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invoke: CsvInvocation = match result {
      Ok(i) => i,
      Err(e) => anyhow::bail!("Error deserializing csv invocation: {}", e),
    };
    let func = metadata.get(&invoke.func_name).unwrap();
    let h_c = host.clone();
    let f_c = func.func_name.clone();
    let args = args_to_json(prepare_function_args(func, &load_type));
    loop {
      match start.elapsed() {
        Ok(t) => {
          if t.as_millis() >= invoke.invoke_time_ms as u128 {
            break;
          }
        },
        Err(_) => (),
      }
    };
    
    let clk_clone = clock.clone();
    handles.push(threaded_rt.spawn(async move {
      worker_invoke(&f_c, &VERSION, &h_c, port, &gen_tid(), Some(args), clk_clone).await
    }));
  }

  let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;

  let pth = Path::new(&trace_pth);
  let output_folder: String = get_val("out", &main_args)?;
  let p = Path::new(&output_folder).join(format!("output-{}", pth.file_name().expect("Could not find a file name").to_str().unwrap()));
  save_worker_result_csv(p, &results)?;

  let p = Path::new(&output_folder).join(format!("output-full-{}.json", pth.file_stem().expect("Could not find a file name").to_str().unwrap()));
  save_result_json(p, &results)
}
