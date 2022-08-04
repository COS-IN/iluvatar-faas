use std::time::Duration;

use iluvatar_lib::{load_balancer_api::lb_structs::json::{RegisterFunction, Invoke, ControllerInvokeResult}, utils::{timing::TimedExt, port::Port}, transaction::{gen_tid, TransactionId}, types::MemSizeMb, rpc::{RCPWorkerAPI, InvokeResponse}, ilúvatar_api::WorkerAPI};
use serde::{Deserialize, Serialize};
use anyhow::Result;

pub struct ThreadError {
  pub thread_id: usize,
  pub error: anyhow::Error
}
#[derive(Serialize,Deserialize)]
pub struct ThreadResult {
  pub thread_id: usize,
  pub data: Vec<InvocationResult>,
  pub registration: RegistrationResult,
  pub errors: u64,
}
#[derive(Serialize,Deserialize)]
pub struct InvocationResult {
  pub json: FunctionExecOutput,
  pub duration_ms: u64
}
#[derive(Serialize,Deserialize)]
pub struct RegistrationResult {
  pub duration_ms: u64,
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

pub async fn worker_register(name: &String, version: &String, image: &String, memory: MemSizeMb, host: &String, port: Port) -> Result<(String, Duration, TransactionId)> {
  let tid: TransactionId = gen_tid();
  let mut api = RCPWorkerAPI::new(&host, port).await?;
  let (reg_out, reg_dur) = api.register(name.clone(), version.clone(), image.clone(), memory, 1, 1, tid.clone()).timed().await;
  match reg_out {
    Ok(s) => Ok( (s,reg_dur,tid) ),
    Err(e) => anyhow::bail!("registration failed because {}", e),
  }
}

pub async fn worker_invoke(name: &String, version: &String, host: &String, port: Port, tid: &TransactionId, args: Option<String>) -> Result<(InvokeResponse, FunctionExecOutput, u64)> {
  let args = match args {
    Some(a) => a,
    None => "{}".to_string(),
  };
  let mut api = RCPWorkerAPI::new(&host, port).await?;
  let (invok_out, invok_lat) = api.invoke(name.clone(), version.clone(), args, None, tid.clone()).timed().await;
  match invok_out {
    Ok(r) => match serde_json::from_str::<FunctionExecOutput>(&r.json_result) {
      Ok(b) => Ok( (r, b, invok_lat.as_millis() as u64) ),
      Err(e) => anyhow::bail!("Deserialization error: {}; {}", e, r.json_result),
    },
    Err(e) => anyhow::bail!("Invocation error: {}", e),
  }
}
