use std::sync::Arc;

use tonic::{Request, Response, Status};

use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorker;
use iluvatar_lib::rpc::*;
use crate::config::WorkerConfig;
use crate::containers::containermanager::ContainerManager;
use crate::invocation::invoker::InvokerService;
use log::*;

#[derive(Debug)]
#[allow(unused)]
pub struct IluvatarWorkerImpl {
  container_manager: Arc<ContainerManager>,
  config: WorkerConfig,
  invoker: Arc<InvokerService>,
}

impl IluvatarWorkerImpl {
  pub fn new(config: WorkerConfig, container_manager: Arc<ContainerManager>, invoker: Arc<InvokerService>) -> IluvatarWorkerImpl {
    IluvatarWorkerImpl {
      container_manager,
      config,
      invoker,
    }
  }
}
 
#[tonic::async_trait]
impl IluvatarWorker for IluvatarWorkerImpl {
  async fn ping(
      &self,
      request: Request<PingRequest>,
  ) -> Result<Response<PingResponse>, Status> {
      println!("Got a request: {:?}", request);
      let reply = PingResponse {
          message: format!("Pong").into(),
      };

      Ok(Response::new(reply))
  }

  async fn invoke(&self,
    request: Request<InvokeRequest>) -> Result<Response<InvokeResponse>, Status> {
      let request = request.into_inner();
      let resp = self.invoker.invoke(&request).await;

      match resp {
        Ok( (json, dur) ) => {
          let reply = InvokeResponse {
            json_result: json,
            success: true,
            duration_ms: dur
          };
          Ok(Response::new(reply))    
        },
        Err(e) => {
          error!("Invoke failed with error: {}", e);
          Ok(Response::new(InvokeResponse {
            json_result: format!("{{ \"Error\": \"{}\" }}", e.to_string()),
            success: false,
            duration_ms: 0
          }))
        },
      }
    }

  async fn invoke_async(&self,
    request: Request<InvokeAsyncRequest>) -> Result<Response<InvokeAsyncResponse>, Status> {
      let resp = self.invoker.invoke_async(Arc::new(request.into_inner()));

      match resp {
        Ok( cookie ) => {
          let reply = InvokeAsyncResponse {
            lookup_cookie: cookie,
          };
          Ok(Response::new(reply))
        },
        Err(e) => {
          error!("Failed to launch an async invocation with error '{}'", e);
          Ok(Response::new(InvokeAsyncResponse {
            lookup_cookie: "".to_string()
          }))
        },
      }
    }

  async fn invoke_async_check(&self, request: Request<InvokeAsyncLookupRequest>) -> Result<Response<InvokeResponse>, Status> {
    let resp = self.invoker.invoke_async_check(&request.into_inner().lookup_cookie);
    match resp {
      Ok( resp ) => {
        Ok(Response::new(resp))
      },
      Err(e) => {
        error!("Failed to check async invocation status '{}'", e);
        Ok(Response::new(InvokeResponse {
          json_result: format!("{{ \"Error\": \"{}\" }}", e.to_string()),
          success: false,
          duration_ms: 0
        }))
      },
    }
  }

  async fn prewarm(&self,
    request: Request<PrewarmRequest>) -> Result<Response<PrewarmResponse>, Status> {
      let request = request.into_inner();
      let container_id = self.container_manager.prewarm(&request).await;

      match container_id {
        Ok(_) => {
          let resp = PrewarmResponse {
            success: true,
            message: "".to_string(),
          };
          Ok(Response::new(resp))    
        },
        Err(e) => {
          error!("Prewarm failed with error: {}", e);
          let resp = PrewarmResponse {
            success: false,
            message: format!("{{ \"Error\": \"{}\" }}", e.to_string()),
          };
          Ok(Response::new(resp))  
        }
      }
    }

  async fn register(&self,
    request: Request<RegisterRequest>) -> Result<Response<RegisterResponse>, Status> {
      let request = request.into_inner();

      let reg_result = self.container_manager.register(&request).await;

      match reg_result {
        Ok(_) => {
          let reply = RegisterResponse {
            success: true,
            function_json_result: format!("{{\"Ok\": \"function '{}' registered\"}}", request.function_name).into(),
          };
          Ok(Response::new(reply))        
        },
        Err(msg) => {
          error!("Register failed with error {}", msg);
          let reply = RegisterResponse {
            success: false,
            function_json_result: format!("{{\"Error\": \"Error during registration of '{}': '{:?}\"}}", request.function_name, msg).into(),
          };
          Ok(Response::new(reply))        
        },
      }
    }

  async fn status(&self,
    _: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
      let _reply = StatusResponse {
        json_result: "{\"Error\": \"not implemented\"}".into(),
        queue_len: 0,
        used_mem: 0,
        total_mem: 0,
        load: 0.0,
      };
      Err(Status::aborted("failed"))
    }

  async fn health(&self,
    _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
      let reply = HealthResponse {
        status: HealthStatus::Healthy as i32
      };
      Ok(Response::new(reply))
    }
}
