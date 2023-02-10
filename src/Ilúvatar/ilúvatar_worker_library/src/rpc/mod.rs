tonic::include_proto!("iluvatar_worker");
use tracing::{error, debug, warn};
use tonic::transport::Channel;
use std::error::Error;
use iluvatar_library::bail_error;
use crate::{rpc::iluvatar_worker_client::IluvatarWorkerClient, worker_api::{HealthStatus, WorkerAPI}};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::{MemSizeMb, Isolation, Compute};
use iluvatar_library::utils::port_utils::Port;
use anyhow::{Result, bail};
use tonic::{Status, Code, Request};

#[allow(unused)]
pub struct RPCWorkerAPI {
  client: IluvatarWorkerClient<Channel>
}

impl RPCWorkerAPI {
  /// Open a new connection to a Worker API
  /// Returns a [RPCError] with details if the connection fails
  pub async fn new(address: &String, port: Port, tid: &TransactionId) -> Result<RPCWorkerAPI, RPCError> {
    Self::repeat_try_connection(address, port, 5, tid).await
  }

  /// Try opening a new connection, with several retries
  /// This can be flaky, so if there is an error, the connection is retried several times
  async fn repeat_try_connection(address: &String, port: Port, mut retries: u32, tid: &TransactionId) -> Result<RPCWorkerAPI, RPCError> {
    loop {
      match Self::try_new_connection(address, port).await {
        Ok(api) => {return Ok(api);},
        Err(e) => {
          warn!(error=%e, tid=%tid, "Error opening RPC connection to Worker API");
          retries -= 1;
          if retries <= 0 {
            return Err(e);
          }
        }
      }
    }
  }

  async fn try_new_connection(address: &String, port: Port) -> Result<RPCWorkerAPI, RPCError> {
    let addr = format!("http://{}:{}", address, port);
    match IluvatarWorkerClient::connect(addr).await {
        Ok(c) => Ok(RPCWorkerAPI{client: c}),
        Err(e) => Err(RPCError { message: Status::new(Code::Unknown, format!("Got unexpected error of {:?}", e)), source: "[RCPWorkerAPI:new]".to_string() }),
    }
  }
}

impl Clone for RPCWorkerAPI {
  /// A fast method for duplicating the Worker API
  /// Use this instead of concurently sharing
  fn clone(&self) -> Self {
    RPCWorkerAPI {
      client: self.client.clone()
    }
  }
}

#[derive(Debug)]
pub struct RPCError {
  message: Status,
  source: String
}
impl RPCError {
  pub fn new(message: Status, source: String) -> Self {
    RPCError {
      message,
      source
    }
  }
}
impl std::fmt::Display for RPCError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "{} RPC connection failed because: {:?}", self.source, self.message)?;
    Ok(())
  }
}
impl Error for RPCError {

}

/// An implementation of the worker API that communicates with workers via RPC
#[tonic::async_trait]
impl WorkerAPI for RPCWorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String> {
    let request = Request::new(PingRequest {
      message: "Ping".to_string(),
      transaction_id: tid,
    });
    match self.client.ping(request).await {
      Ok(response) => Ok(response.into_inner().message),
      Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:ping]".to_string())),
    }
  }

  async fn invoke(&mut self, function_name: String, version: String, args: String, tid: TransactionId) -> Result<InvokeResponse> {
    let request = Request::new(InvokeRequest {
      function_name: function_name,
      function_version: version,
      json_args: args,
      transaction_id: tid
    });
    match self.client.invoke(request).await {
      Ok(response) => Ok(response.into_inner()),
      Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:invoke]".to_string())),
    }
  }

  async fn invoke_async(&mut self, function_name: String, version: String, args: String, tid: TransactionId) -> Result<String> {
    let request = Request::new(InvokeAsyncRequest {
      function_name,
      function_version: version,
      json_args: args,
      transaction_id: tid.clone(),
    });
    match self.client.invoke_async(request).await {
      Ok(response) => {
        let response = response.into_inner();
        if response.success {
          debug!(tid=%tid, "Async invoke succeeded");
          Ok(response.lookup_cookie)
        } else {
          error!(tid=%tid, "Async invoke failed");
          anyhow::bail!("Async invoke failed")
        }    
      },
      Err(e) => bail!(RPCError::new( e, "[RCPWorkerAPI:invoke_async]".to_string())),
    }
  }

  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<InvokeResponse> {
    let request = Request::new(InvokeAsyncLookupRequest {
      lookup_cookie: cookie.to_owned(),
      transaction_id: tid,
    });
    match self.client.invoke_async_check(request).await {
      Ok(response) => Ok(response.into_inner()),
      Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:invoke_async_check]".to_string())),
    }
  }

  async fn prewarm(&mut self, function_name: String, version: String, tid: TransactionId) -> Result<String> {
    let request = Request::new(PrewarmRequest {
      function_name: function_name,
      function_version: version,
      transaction_id: tid.clone(),
      compute: Compute::CPU.bits(),
    });
    match self.client.prewarm(request).await {
      Ok(response) => {
        let response = response.into_inner();
        match response.success {
          true => Ok("".to_string()),
          false => bail_error!(tid=%tid, message=%response.message, "Prewarm request failed"),
        }
      },
      Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:prewarm]".to_string())),
    }
  }

  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: MemSizeMb, cpus: u32, parallels: u32, tid: TransactionId, isolate: Isolation) -> Result<String> {
    let request = Request::new(RegisterRequest {
      function_name,
      function_version: version,
      memory,
      cpus,
      image_name,
      parallel_invokes: match parallels {
        i if i <= 0 => 1,
        _ => parallels,
      },
      transaction_id: tid,
      language: LanguageRuntime::Nolang.into(),
      compute: Compute::CPU.bits(),
      isolate: isolate.bits(),
    });
    match self.client.register(request).await {
      Ok(response) => Ok(response.into_inner().function_json_result),
      Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:register]".to_string())),
    }
  }
  
  async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse> {
    let request = Request::new(StatusRequest { transaction_id: tid, });
    match self.client.status(request).await {
      Ok(response) => Ok(response.into_inner()),
      Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:status]".to_string())),
    }
  }

  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus> {
    let request = Request::new(HealthRequest { transaction_id: tid, });
    match self.client.health(request).await {
      Ok(response) => {
        match response.into_inner().status {
          // HealthStatus::Healthy
          0 => Ok(HealthStatus::HEALTHY),
          // HealthStatus::Unhealthy
          1 => Ok(HealthStatus::UNHEALTHY),
          i => anyhow::bail!(RPCError {
            message: Status::new(Code::InvalidArgument, format!("Got unexpected status of {}", i)), source: "[RCPWorkerAPI:health]".to_string()
          }),
        }  
      },
      Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:register]".to_string())),
    }
  }
}
