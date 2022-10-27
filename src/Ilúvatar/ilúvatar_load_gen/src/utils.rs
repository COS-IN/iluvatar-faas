use std::{time::Duration, path::Path, fs::File, io::Write};
use iluvatar_worker_library::{rpc::{RPCWorkerAPI, InvokeResponse}, worker_api::WorkerAPI};
use iluvatar_controller_library::controller::controller_structs::json::{RegisterFunction, Invoke, ControllerInvokeResult};
use iluvatar_library::{utils::{timing::TimedExt, port::Port}, transaction::TransactionId, types::MemSizeMb};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use tokio::{runtime::Runtime, task::JoinHandle};

lazy_static::lazy_static! {
  pub static ref VERSION: String = "0.0.1".to_string();
}

pub struct ThreadError {
  pub thread_id: usize,
  pub error: anyhow::Error
}
#[derive(Serialize,Deserialize)]
pub struct ThreadResult {
  pub thread_id: usize,
  pub data: Vec<SuccessfulWorkerInvocation>,
  pub registration: RegistrationResult,
  pub errors: u64,
}
#[derive(Serialize,Deserialize)]
pub struct RegistrationResult {
  pub duration_ms: u128,
  pub result: String
}

#[derive(Serialize,Deserialize)]
/// This is the output from the python functions
pub struct FunctionExecOutput {
  pub body: Body
}
#[derive(Serialize,Deserialize)]
pub struct Body {
  pub cold: bool,
  pub start: f64,
  pub end: f64,
  /// python runtime latency in seconds
  pub latency: f64,
}
#[derive(Serialize,Deserialize)]
pub struct SuccessfulWorkerInvocation {
  /// The RPC result returned by the worker
  pub worker_response: InvokeResponse,
  /// The deserialized result of the function's execution
  pub function_output: FunctionExecOutput,
  /// The latency experienced by the client, in milliseconds
  pub client_latency_ms: u128,
  pub function_name: String,
  pub function_version: String,
  pub tid: TransactionId,
}

/// Run an invocation against the controller
/// Return the [iluvatar_controller_library::load_balancer_api::lb_structs::json::ControllerInvokeResult] result after parsing
/// also return the latency in milliseconds of the request
pub async fn controller_invoke(name: &String, version: &String, host: &String, port: Port, args: Option<Vec<String>>) -> Result<(ControllerInvokeResult, f64)> {
  let client = reqwest::Client::new();
  let req = Invoke {
    function_name: name.clone(),
    function_version: version.clone(),
    args: args
  };
  let (invok_out, invok_lat) =  client.post(format!("http://{}:{}/invoke", &host, port))
      .json(&req)
      .header("Content-Type", "application/json")
      .send()
      .timed()
      .await;
  let invok_lat = invok_lat.as_millis() as f64;
  match invok_out {
    Ok(r) => 
    {
      let txt = match r.text().await {
          Ok(t) => t,
          Err(e) => {
            anyhow::bail!("Get text error: {};", e);
          },
      };
      match serde_json::from_str::<ControllerInvokeResult>(&txt) {
        Ok(r) => Ok( (r, invok_lat) ),
        Err(e) => {
          anyhow::bail!("InvokeResult Deserialization error: {}; {}", e, &txt);
        },
      }
    },
    Err(e) => {
      anyhow::bail!("Invocation error: {}", e);
    },
  }
}

pub async fn controller_register(name: &String, version: &String, image: &String, memory: MemSizeMb, host: &String, port: Port) -> Result<Duration> {
  let req = RegisterFunction {
    function_name: name.clone(),
    function_version: version.clone(),
    image_name: image.clone(),
    memory,
    cpus: 1,
    parallel_invokes: 1
  };
  let client = reqwest::Client::new();
  let (reg_out, reg_dur) =  client.post(format!("http://{}:{}/register_function", &host, port))
      .json(&req)
      .header("Content-Type", "application/json")
      .send()
      .timed()
      .await;
  match reg_out {
    Ok(r) => {
      let status = r.status();
      if status == reqwest::StatusCode::OK {
        Ok(reg_dur)
      } else {
        let text = r.text().await?;
        anyhow::bail!("Got unexpected HTTP status when registering function with the load balancer '{}'; text: {}", status, text);
      }
    },
    Err(e) =>{
      anyhow::bail!("HTTP error when trying to register function with the load balancer '{}'", e);
    },
  }
}

pub async fn worker_register(name: String, version: &String, image: String, memory: MemSizeMb, host: String, port: Port) -> Result<(String, Duration, TransactionId)> {
  let tid: TransactionId = format!("{}-reg-tid", name);
  let mut api = RPCWorkerAPI::new(&host, port).await?;
  let (reg_out, reg_dur) = api.register(name, version.clone(), image, memory, 1, 1, tid.clone()).timed().await;
  match reg_out {
    Ok(s) => Ok( (s,reg_dur,tid) ),
    Err(e) => anyhow::bail!("registration failed because {}", e),
  }
}

pub async fn worker_prewarm(name: &String, version: &String, host: &String, port: Port, tid: &TransactionId) -> Result<(String, Duration)> {
  let mut api = RPCWorkerAPI::new(&host, port).await?;
  let (res, dur) = api.prewarm(name.clone(), version.clone(), None, None, None, tid.to_string()).timed().await;
  match res {
    Ok(s) => Ok( (s, dur) ),
    Err(e) => anyhow::bail!("registration failed because {}", e),
  }
}

pub async fn worker_invoke(name: &String, version: &String, host: &String, port: Port, tid: &TransactionId, args: Option<String>) -> Result<SuccessfulWorkerInvocation> {
  let args = match args {
    Some(a) => a,
    None => "{}".to_string(),
  };
  let mut api = RPCWorkerAPI::new(&host, port).await?;
  let (invok_out, invok_lat) = api.invoke(name.clone(), version.clone(), args, None, tid.clone()).timed().await;
  match invok_out {
    Ok(r) => match serde_json::from_str::<FunctionExecOutput>(&r.json_result) {
      Ok(b) => Ok( SuccessfulWorkerInvocation {
        worker_response: r,
        function_output: b,
        client_latency_ms: invok_lat.as_millis(),
        function_name: name.clone(),
        function_version: version.clone(),
        tid: tid.clone()
      }),
      Err(e) => anyhow::bail!("Deserialization error: {}; {}", e, r.json_result),
    },
    Err(e) => anyhow::bail!("Invocation error: {}", e),
  }
}

pub fn resolve_handles(runtime: Runtime, run_results: Vec<JoinHandle<Result<SuccessfulWorkerInvocation>>>) -> Vec<Result<SuccessfulWorkerInvocation>> {
  let mut ret = vec![];
  for h in run_results {
    match runtime.block_on(h) {
      Ok( r) => {
        ret.push(r);
      },
      Err(thread_e) => println!("Joining error: {}", thread_e),
    };
  }
  ret
}

pub fn save_worker_result<P: AsRef<Path>>(path: P, run_results: &Vec<Result<SuccessfulWorkerInvocation>>) -> Result<()> {
  let mut f = match File::create(path) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };
  let to_write = format!("success,function_name,was_cold,worker_duration_ms,code_duration_sec,e2e_duration_ms,tid\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json header of result because {}", e);
    }
  };

  for r in run_results {
    match r {
      Ok( worker_invocation ) => {
        let to_write = format!("{},{},{},{},{},{},{}\n", worker_invocation.worker_response.success, worker_invocation.function_name, 
          worker_invocation.function_output.body.cold, worker_invocation.worker_response.duration_ms, 
          worker_invocation.function_output.body.latency, worker_invocation.client_latency_ms, worker_invocation.tid);
        match f.write_all(to_write.as_bytes()) {
          Ok(_) => (),
          Err(e) => {
            println!("Failed to write result because {}", e);
            continue;
          }
        };
      },
      Err(e) => println!("Status error from invoke: {}", e),
    };
  }
  Ok(())
}
