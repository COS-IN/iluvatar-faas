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

#[derive(Serialize,Deserialize)]
pub struct ThreadResult {
  pub thread_id: usize,
  pub data: Vec<SuccessfulWorkerInvocation>,
  pub registration: RegistrationResult,
  pub errors: u64,
}
#[derive(Serialize,Deserialize)]
pub struct RegistrationResult {
  pub duration_us: u128,
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
  /// The latency experienced by the client, in microseconds
  pub client_latency_us: u128,
  pub function_name: String,
  pub function_version: String,
  pub tid: TransactionId,
}

#[derive(Serialize,Deserialize)]
pub struct SuccessfulControllerInvocation {
  /// The RPC result returned by the worker
  pub controller_response: ControllerInvokeResult,
  /// The deserialized result of the function's execution
  pub function_output: FunctionExecOutput,
  /// The latency experienced by the client, in microseconds
  pub client_latency_us: u128,
  pub function_name: String,
  pub function_version: String,
}

/// Run an invocation against the controller
/// Return the [iluvatar_controller_library::load_balancer_api::lb_structs::json::ControllerInvokeResult] result after parsing
/// also return the latency in milliseconds of the request
pub async fn controller_invoke(name: &String, version: &String, host: &String, port: Port, args: Option<Vec<String>>) -> Result<SuccessfulControllerInvocation> {
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
        Ok(r) => match serde_json::from_str::<FunctionExecOutput>(&r.json_result) {
          Ok(feo) => Ok(SuccessfulControllerInvocation {
            controller_response: r,
            function_output: feo,
            client_latency_us: invok_lat.as_micros(),
            function_name: name.clone(),
            function_version: version.clone(),
          }),
          Err(e) => anyhow::bail!("FunctionExecOutput Deserialization error: {}; {}", e, &txt),
        },
        Err(e) => {
          anyhow::bail!("ControllerInvokeResult Deserialization error: {}; {}", e, &txt);
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
        client_latency_us: invok_lat.as_micros(),
        function_name: name.clone(),
        function_version: version.clone(),
        tid: tid.clone()
      }),
      Err(e) => anyhow::bail!("Deserialization error: {}; {}", e, r.json_result),
    },
    Err(e) => anyhow::bail!("Invocation error: {}", e),
  }
}

/// How to handle per-thread errors that appear when joining load workers
pub enum ErrorHandling {
  Raise,
  Print,
  Ignore
}

/// Resolve all the tokio threads and return their results
/// Optionally handle errors from threads
pub fn resolve_handles<T>(runtime: &Runtime, run_results: Vec<JoinHandle<Result<T>>>, eh: ErrorHandling) -> Result<Vec<T>> {
  let mut ret = vec![];
  for h in run_results {
    match runtime.block_on(h) {
      Ok( r) => {
        match r {
          Ok(ok) => ret.push(ok),
          Err(e) => match eh {
            ErrorHandling::Raise => return Err(e),
            ErrorHandling::Print => println!("Error from thread: {}", e),
            ErrorHandling::Ignore => (),
          },
        }
      },
      Err(thread_e) => println!("Joining error: {}", thread_e),
    };
  }
  Ok(ret)
}

/// Save worker load results as a csv
pub fn save_worker_result_csv<P: AsRef<Path> + std::fmt::Debug>(path: P, run_results: &Vec<SuccessfulWorkerInvocation>) -> Result<()> {
  let mut f = match File::create(&path) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create csv output '{:?}' file because {}", &path, e);
    }
  };
  let to_write = format!("success,function_name,was_cold,worker_duration_us,code_duration_sec,e2e_duration_us,tid\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json header to '{:?}' of result because {}", &path, e);
    }
  };

  for worker_invocation in run_results {
    let to_write = format!("{},{},{},{},{},{},{}\n", worker_invocation.worker_response.success, worker_invocation.function_name, 
      worker_invocation.function_output.body.cold, worker_invocation.worker_response.duration_us, 
      worker_invocation.function_output.body.latency, worker_invocation.client_latency_us, worker_invocation.tid);
    match f.write_all(to_write.as_bytes()) {
      Ok(_) => (),
      Err(e) => {
        println!("Failed to write result to '{:?}' because {}", &path, e);
        continue;
      }
    };
  };
  Ok(())
}

pub fn save_result_json<P: AsRef<Path> + std::fmt::Debug, T: Serialize>(path: P, results: &T) -> Result<()> {
  let mut f = match File::create(&path) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create json output '{:?}' file because {}", &path, e);
    }
  };

  let to_write = serde_json::to_string(&results)?;
  f.write_all(to_write.as_bytes())?;
  Ok(())
}
