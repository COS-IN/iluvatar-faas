use std::sync::Arc;
use crate::bail_error;
use crate::ilúvatar_api::WorkerAPI;
use crate::rpc::{InvokeResponse, StatusResponse, RPCError};
use crate::{transaction::TransactionId, ilúvatar_api::HealthStatus, types::MemSizeMb};
use anyhow::{Result, bail};
use tracing::{debug, error};
use super::ilúvatar_worker::IluvatarWorkerImpl;
use crate::rpc::iluvatar_worker_server::IluvatarWorker;
use crate::rpc::{StatusRequest, HealthRequest, PingRequest, InvokeRequest, InvokeAsyncRequest, InvokeAsyncLookupRequest, RegisterRequest, PrewarmRequest};

/// A simulation version of the WOrkerAPI
///   must match [crate::rpc::RPCWorkerAPI] in handling, etc.
pub struct SimWorkerAPI {
  worker: Arc<IluvatarWorkerImpl>
}
impl SimWorkerAPI {
  pub fn new(worker: Arc<IluvatarWorkerImpl>) -> Self {
    SimWorkerAPI {
      worker
    }
  }
}

#[tonic::async_trait]
#[allow(unused)]
impl WorkerAPI for SimWorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String> {
    let request = tonic::Request::new(PingRequest {
      message: "Ping".to_string(),
      transaction_id: tid,
    });
    match self.worker.ping(request).await {
      Ok(response) => Ok(response.into_inner().message),
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:ping]".to_string())),
    }
  }

  async fn invoke(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<InvokeResponse> {
    let request = tonic::Request::new(InvokeRequest {
      function_name: function_name,
      function_version: version,
      memory: match memory {
        Some(x) => x,
        _ => 0,
      },
      json_args: args,
      transaction_id: tid
    });
    match self.worker.invoke(request).await {
      Ok(response) => Ok(response.into_inner()),
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:invoke]".to_string())),
    }
  }

  async fn invoke_async(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<String> {
    let request = tonic::Request::new(InvokeAsyncRequest {
      function_name,
      function_version: version,
      memory: match memory {
        Some(x) => x,
        _ => 0,
      },
      json_args: args,
      transaction_id: tid.clone(),
    });
    match self.worker.invoke_async(request).await {
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
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:invoke_async]".to_string())),
    }
  }

  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<InvokeResponse> {
    let request = tonic::Request::new(InvokeAsyncLookupRequest {
      lookup_cookie: cookie.to_owned(),
      transaction_id: tid,
    });
    match self.worker.invoke_async_check(request).await {
      Ok(response) => Ok(response.into_inner()),
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:invoke_async_check]".to_string())),
    }
  }

  async fn prewarm(&mut self, function_name: String, version: String, memory: Option<MemSizeMb>, cpu: Option<u32>, image: Option<String>, tid: TransactionId) -> Result<String> {
    let request = tonic::Request::new(PrewarmRequest {
      function_name: function_name,
      function_version: version,
      memory: match memory {
        Some(x) => x,
        _ => 0,
      },
      cpu: match cpu {
        Some(x) => x,
        _ => 0,
      },
      image_name: match image {
        Some(x) => x,
        _ => "".into(),
      },
      transaction_id: tid.clone(),
    });
    match self.worker.prewarm(request).await {
      Ok(response) => {
        let response = response.into_inner();
        match response.success {
          true => Ok("".to_string()),
          false => bail_error!(tid=%tid, message=%response.message, "Prewarm request failed"),
        }
      },
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:prewarm]".to_string())),
    }
  }

  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: MemSizeMb, cpus: u32, parallels: u32, tid: TransactionId) -> Result<String> {
    let request = tonic::Request::new(RegisterRequest {
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
    });
    match self.worker.register(request).await {
      Ok(response) => Ok(response.into_inner().function_json_result),
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:register]".to_string())),
    }
  }
  
  async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse> {
    let request = tonic::Request::new(StatusRequest { transaction_id: tid, });
    match self.worker.status(request).await {
      Ok(response) => Ok(response.into_inner()),
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:status]".to_string())),
    }
  }

  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus> {
    let request = tonic::Request::new(HealthRequest { transaction_id: tid, });
    match self.worker.health(request).await {
      Ok(response) => {
        match response.into_inner().status {
          // HealthStatus::Healthy
          0 => Ok(HealthStatus::HEALTHY),
          // HealthStatus::Unhealthy
          1 => Ok(HealthStatus::UNHEALTHY),
          i => anyhow::bail!(RPCError::new(format!("Got unexpected status of {}", i), "[RCPWorkerAPI:health]".to_string())),
        }  
      },
      Err(e) => bail!(RPCError::new(e.to_string(), "[RCPWorkerAPI:register]".to_string())),
    }
  }
}
