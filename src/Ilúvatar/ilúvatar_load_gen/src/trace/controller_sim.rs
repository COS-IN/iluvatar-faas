use std::{collections::HashMap, time::{SystemTime, Duration}, path::Path, fs::File, io::Write, sync::Arc};
use anyhow::Result;
use iluvatar_library::{utils::{config::get_val, timing::TimedExt}, transaction::{TransactionId, SIMULATION_START_TID}};
use iluvatar_worker_library::{services::containers::simulation::simstructs::SimulationResult};
use iluvatar_controller_library::controller::{controller_structs::json::{ControllerInvokeResult, RegisterFunction}, web_server::register_function, controller::Controller};
use actix_web::{web::{Json, Data}, body::MessageBody};
use iluvatar_controller_library::controller::web_server::{invoke, register_worker};
use iluvatar_controller_library::controller::structs::json::Invoke;
use clap::ArgMatches;
use tokio::{runtime::Builder, task::JoinHandle};
use crate::{trace::CsvInvocation, utils::VERSION};
use super::Function;
use iluvatar_worker_library::worker_api::worker_config::Configuration as WorkerConfig;

async fn register_workers(num_workers: usize, server_data: &Data<Controller>, worker_config_pth: &String, worker_config: &Arc<WorkerConfig>) -> Result<()> {
  for i in 0..num_workers {
    let r = iluvatar_controller_library::controller::controller_structs::json::RegisterWorker {
      name: format!("worker_{}", i),
      backend: "simulation".to_string(),
      communication_method: "simulation".to_string(),
      host: worker_config_pth.clone(),
      port: 0,
      memory: worker_config.container_resources.memory_mb,
      cpus: worker_config.container_resources.cores,
    };
    let response = register_worker(server_data.clone(), Json{0:r}).await;
    if ! response.status().is_success() {
      let text = response.body();
      anyhow::bail!("Registering simulated worker failed with '{:?}' '{:?}", response.headers(), text)
    }
  }
  Ok(())
}

async fn register_functions(metadata: &HashMap<String, Function>, server_data: &Data<Controller>) -> Result<()> {
  for (id, func) in metadata.iter() {
    let r = RegisterFunction {
      function_name: id.to_string(),
      function_version: VERSION.clone(),
      image_name: "".to_string(),
      memory: func.mem_mb,
      cpus: 1,
      parallel_invokes: 1
    };
    let response = register_function(server_data.clone(), Json{0:r}).await;
    if ! response.status().is_success() {
      let text = response.body();
      anyhow::bail!("Registration failed with '{:?}' '{:?}", response.headers(), text)
    }
  }
  Ok(())
}

async fn controller_invoke(func_name: String, server_data: Data<Controller>, warm_dur_ms: u64, cold_dur_ms: u64) -> Result<(ControllerInvokeResult, u64)> {
  let i = Invoke{function_name: func_name.clone(), function_version:VERSION.clone(), 
    args:Some(vec![format!("warm_dur_ms={}", warm_dur_ms), format!("cold_dur_ms={}", cold_dur_ms)])};
  let (response, dur) = invoke(server_data, Json{0:i}).timed().await;
  if ! response.status().is_success() {
    let text = response.body();
    anyhow::bail!("Invocation failed with '{:?}' '{:?}", response.headers(), text)
  }
  let b = response.into_body();
  let bytes = match b.try_into_bytes() {
    Ok(b) => b,
    Err(e) => anyhow::bail!("failed to reat http bytes because {:?}", e),
  };

  match serde_json::from_slice::<ControllerInvokeResult>(&bytes) {
    Ok(r) => Ok( (r, dur.as_millis() as u64) ),
    Err(e) => anyhow::bail!("Deserialization error of ControllerInvokeResult: {}", e),
  }
}

pub fn controller_trace_sim(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let worker_config_pth: String = get_val("worker-config", &sub_args)?;
  let num_workers: usize = get_val("workers", &sub_args)?;
  let controller_config_pth: String = get_val("controller-config", &sub_args)?;
  let trace_pth: String = get_val("input", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;
  let threaded_rt = Builder::new_multi_thread()
      .enable_all()
      .build().unwrap();

  let tid: &TransactionId = &SIMULATION_START_TID;
  let worker_config: Arc<WorkerConfig> = WorkerConfig::boxed(false, &worker_config_pth).unwrap();
  let controller_config = iluvatar_controller_library::controller::controller_config::Configuration::boxed(&controller_config_pth).unwrap();
  let _guard = iluvatar_library::logging::start_tracing(controller_config.logging.clone(), controller_config.graphite.clone(), &controller_config.name, tid)?;

  let server = threaded_rt.block_on(async { Controller::new(controller_config.clone(), tid) });
  let server_data = actix_web::web::Data::new(server);

  threaded_rt.block_on(register_workers(num_workers, &server_data, &worker_config_pth, &worker_config))?;
  let metadata = super::load_metadata(metadata_pth)?;
  threaded_rt.block_on(register_functions(&metadata, &server_data))?;

  let mut trace_rdr = csv::Reader::from_path(&trace_pth)?;
  let mut handles: Vec<JoinHandle<Result<(ControllerInvokeResult, u64)>>> = Vec::new();

  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invocation: CsvInvocation = result?;
    let func = metadata.get(&invocation.func_name).unwrap();
    loop {
      match start.elapsed() {
        Ok(t) => {
          let ms = t.as_millis() as u64;
          if ms >= invocation.invoke_time_ms {
            break;
          }
          std::thread::sleep(Duration::from_millis(ms/2));
        },
        Err(_) => (),
      }
    };
    let warm_dur_ms = func.warm_dur_ms;
    let cold_dur_ms = func.cold_dur_ms;
    let server_data_cln = server_data.clone();
    handles.push(threaded_rt.spawn(async move {
      controller_invoke(invocation.func_name, server_data_cln, warm_dur_ms, cold_dur_ms).await
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
  let to_write = format!("success,function_name,was_cold,worker_duration_ms,invocation_duration_ms,code_duration_ms,e2e_duration_ms\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json of result because {}", e);
    }
  };

  for h in handles {
    match threaded_rt.block_on(h) {
      Ok(r) => match r {
        Ok( (resp, e2e_dur) ) => {
          let result = serde_json::from_str::<SimulationResult>(&resp.json_result)?;
          let to_write = format!("{},{},{},{},{},{}\n", resp.success, result.function_name, result.was_cold, resp.worker_duration_ms, result.duration_ms, e2e_dur);
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
