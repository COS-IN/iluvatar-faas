use std::{collections::HashMap, sync::Arc, path::Path, cmp::max};
use anyhow::Result;
use iluvatar_library::{utils::{config::{get_val, args_to_json}, timing::TimedExt, port::Port}, transaction::{TransactionId, gen_tid}, logging::LocalTime};
use iluvatar_worker_library::{worker_api::{create_worker, ilúvatar_worker::IluvatarWorkerImpl}, rpc::PrewarmRequest};
use iluvatar_worker_library::{services::containers::simulation::simstructs::SimulationInvocation};
use clap::ArgMatches;
use tokio::{runtime::{Builder, Runtime}, task::JoinHandle};
use std::time::SystemTime;
use iluvatar_worker_library::rpc::{iluvatar_worker_server::IluvatarWorker, InvokeRequest, RegisterRequest, RegisterResponse};
use tonic::Request;
use crate::{utils::{VERSION, worker_invoke, CompletedWorkerInvocation, resolve_handles, save_worker_result_csv, save_result_json, FunctionExecOutput}, trace::trace_utils::{prepare_functions, RegisterTarget, map_functions_to_prep, fill_largest_wo_going_over}, benchmark::BenchmarkStore};
use crate::trace::prepare_function_args;
use super::{Function, CsvInvocation};

fn sim_register_functions(funcs: &HashMap<String, Function>, worker: Arc<IluvatarWorkerImpl>, rt: &Runtime) -> Result<()> {
  let mut handles: Vec<JoinHandle<Result<RegisterResponse>>> = Vec::new();
  for (_k, v) in funcs.into_iter() {
    let r = RegisterRequest {
      function_name: v.func_name.clone(),
      function_version: "0.0.1".to_string(),
      image_name: "".to_string(),
      memory: max(5, v.mem_mb),
      cpus: 1, 
      parallel_invokes: 1,
      transaction_id : format!("{}-reg-tid", v.func_name)
    };
    let cln = worker.clone();
    handles.push(rt.spawn(async move {
      let resp = cln.register(Request::new(r)).await?;
      let resp = resp.into_inner();
      match &resp.success {
        true => Ok(resp),
        false => anyhow::bail!("Registration failed beceause '{}'", resp.function_json_result),
      }
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
  let load_type: String = get_val("load-type", &sub_args)?;
  let func_data: String = get_val("function-data", &sub_args)?;
  let prewarm_count: u32 = get_val("prewarm", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;

  let mut metadata = super::load_metadata(metadata_pth)?;
  match load_type.as_str() {
    "functions" => {
      let contents = std::fs::read_to_string(&func_data).expect("Something went wrong reading the benchmark file");
      match serde_json::from_str::<BenchmarkStore>(&contents) {
        Ok(d) => {
          fill_largest_wo_going_over(&mut metadata, &d);
        },
        Err(e) => anyhow::bail!("Failed to read and parse benchmark data! '{}'", e),
      }
    },
    "lookbusy" => (),
    _ => panic!("Bad invocation load type: {}", load_type),
  };
  let prep_data = map_functions_to_prep(load_type.as_str(), Ok(func_data), &metadata, prewarm_count, &trace_pth)?;
  sim_register_functions(&metadata, worker.clone(), &threaded_rt)?;

  let mut prewarm_calls= vec![];
  for (func_name, (_image, count)) in prep_data.iter() {
    println!("{} prewarming {} containers for function '{}'", LocalTime::new(&"PREWARM_LOAD_GEN".to_string())?.now_str()?, count, func_name);
    for i in 0..*count {
      let tid = format!("{}-{}-prewarm", i, &func_name);
      let f_c = func_name.clone();
      let mem = metadata.get(func_name).unwrap().mem_mb;
      let w_c = worker.clone();
      prewarm_calls.push(threaded_rt.spawn(async move {
        let req = PrewarmRequest {
            function_name: f_c,
            function_version: "0.0.1".to_string(),
            memory: mem,
            cpu: 1,
            image_name: "".to_string(),
            transaction_id: tid,
        };
        w_c.prewarm(Request::new(req)).await
      }));
    }
  }
  for h in prewarm_calls {
    threaded_rt.block_on(h)??;
  }

  let mut trace_rdr = csv::Reader::from_path(&trace_pth)?;
  let mut handles = Vec::new(); // : Vec<JoinHandle<Result<(u128, InvokeResponse)>>>

  println!("starting simulation run");

  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invoke: CsvInvocation = result?;
    let func = metadata.get(&invoke.func_name).unwrap();

    let args = SimulationInvocation {
      warm_dur_ms: func.warm_dur_ms,
      cold_dur_ms: func.cold_dur_ms,
    };
    let clock = Arc::new(LocalTime::new(tid)?);

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
    let f_c = func.func_name.clone();
    let mb = func.mem_mb;
    let start = clock.now_str()?;
    handles.push(threaded_rt.spawn(async move {
      let tid = gen_tid();
      let req = InvokeRequest {
        function_name: f_c.clone(),
        function_version: VERSION.clone(),
        memory: mb,
        json_args: serde_json::to_string(&args)?,
        transaction_id: tid.clone()
      };
  
      let (reg_out, reg_dur) = cln.invoke(Request::new(req)).timed().await;
      let resp = reg_out?.into_inner();
      let result = serde_json::from_str::<FunctionExecOutput>(&resp.json_result)?;
      let r = CompletedWorkerInvocation {
        function_output: result,
        client_latency_us: reg_dur.as_micros(),
        function_name: f_c,
        function_version: VERSION.clone(),
        tid,
        invoke_start: start,
        worker_response: resp,
      };
      Ok(r)
    }));
  }

  let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;

  let pth = Path::new(&trace_pth);
  let output_folder: String = get_val("out", &main_args)?;
  let p = Path::new(&output_folder).join(format!("output-{}", pth.file_name().expect("Could not find a file name").to_str().unwrap()));
  save_worker_result_csv(p, &results)?;

  let p = Path::new(&output_folder).join(format!("output-full-{}.json", pth.file_stem().expect("Could not find a file name").to_str().unwrap()));
  save_result_json(p, &results)

  // let pth = Path::new(&trace_pth);
  // let output_folder: String = get_val("out", &main_args)?;
  // let p = Path::new(&output_folder).join(format!("output-{}", pth.file_name().unwrap().to_str().unwrap()));
  // let mut f = match File::create(p) {
  //   Ok(f) => f,
  //   Err(e) => {
  //     anyhow::bail!("Failed to create output file because {}", e);
  //   }
  // };
  // let to_write = format!("success,function_name,was_cold,worker_duration_us,code_duration_us,e2e_duration_us\n");
  // match f.write_all(to_write.as_bytes()) {
  //   Ok(_) => (),
  //   Err(e) => {
  //     anyhow::bail!("Failed to write json of result because {}", e);
  //   }
  // };

  // for h in handles {
  //   match threaded_rt.block_on(h) {
  //     Ok(r) => match r {
  //       Ok( (e2e_dur_us, resp) ) => {
  //         let result = serde_json::from_str::<SimulationResult>(&resp.json_result)?;
  //         let to_write = format!("{},{},{},{},{},{}\n", resp.success, result.function_name, result.was_cold, resp.duration_us, result.duration_us, e2e_dur_us);
  //         match f.write_all(to_write.as_bytes()) {
  //           Ok(_) => (),
  //           Err(e) => {
  //             println!("Failed to write result because {}", e);
  //             continue;
  //           }
  //         };
  //       },
  //       Err(e) => println!("Status error from invoke: {}", e),
  //     },
  //     Err(thread_e) => println!("Joining error: {}", thread_e),
  //   };
  // }

  // Ok(())
}

fn live_worker(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let load_type: String = get_val("load-type", &sub_args)?;
  let func_data: Result<String> = get_val("function-data", &sub_args);
  let prewarm_count: u32 = get_val("prewarm", &sub_args)?;
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;
  let tid: &TransactionId = &iluvatar_library::transaction::LIVE_WORKER_LOAD_TID;
  let factory = iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory::boxed();

  let threaded_rt = Builder::new_multi_thread()
                        .enable_all()
                        .build().unwrap();

  let trace_pth: String = get_val("input", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;
  let metadata = super::load_metadata(metadata_pth)?;

  prepare_functions(RegisterTarget::LiveWorker, &metadata, &host, port, &load_type, func_data, &threaded_rt, prewarm_count, &trace_pth, &factory)?;

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
    let fct_cln = factory.clone();
    handles.push(threaded_rt.spawn(async move {
      worker_invoke(&f_c, &VERSION, &h_c, port, &gen_tid(), Some(args), clk_clone, &fct_cln).await
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
