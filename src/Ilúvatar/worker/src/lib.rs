pub mod config;
pub mod containers;

pub mod iluvatar_worker {

use tonic::{Request, Response, Status};

use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorker;
use iluvatar_lib::rpc::*;
use crate::config::WorkerConfig;
use crate::containers::containermanager::ContainerManager;

#[derive(Debug)]
#[allow(unused)]
pub struct IluvatarWorkerImpl {
  container_manager: ContainerManager,
  config: WorkerConfig,
}

impl IluvatarWorkerImpl {
  pub fn new(config: WorkerConfig) -> IluvatarWorkerImpl {
    IluvatarWorkerImpl {
      container_manager: ContainerManager::new(config.clone()),
      config: config,
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
      let reply = InvokeResponse {
        json_result: format!("'Error': 'invoke for {} not implemented'", request.into_inner().function_name).into(),
        success: false,
        duration_ms: 3
      };
      Ok(Response::new(reply))
    }

  async fn invoke_async(&self,
    request: Request<InvokeAsyncRequest>) -> Result<Response<InvokeAsyncResponse>, Status> {
      let reply = InvokeAsyncResponse {
        lookup_cookie: format!("{}_COOKIE", request.into_inner().function_name).into(),
      };
      Ok(Response::new(reply))
    }

  async fn prewarm(&self,
    request: Request<PrewarmRequest>) -> Result<Response<PrewarmResponse>, Status> {
      let request = request.into_inner();
      // let mut life = containerlife::ContainerLifecycle::new();

      // let container_id = life.run_container(request.function_name, "default").await;
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
          let resp = PrewarmResponse {
            success: false,
            message: format!("{{ 'Error': '{}' }}", e.to_string()),
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
            function_json_result: format!("'Ok': 'function '{}' registered'", request.function_name).into(),
          };
          Ok(Response::new(reply))        
        },
        Err(msg) => {
          let reply = RegisterResponse {
            success: false,
            function_json_result: format!("'Error': 'Error during registration of '{}': '{:?}' ", request.function_name, msg).into(),
          };
          Ok(Response::new(reply))        
        },
      }
    }

  async fn status(&self,
    _: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
      let _reply = StatusResponse {
        json_result: "{'Error': 'not implemented'}".into(),
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
}
 