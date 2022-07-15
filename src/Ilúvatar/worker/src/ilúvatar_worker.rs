use std::sync::Arc;
use iluvatar_lib::services::WorkerHealthService;
use iluvatar_lib::services::invocation::invoker::InvokerService;
use iluvatar_lib::services::status::status_service::StatusService;
use tonic::{Request, Response, Status};
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorker;
use iluvatar_lib::rpc::*;
use iluvatar_lib::worker_api::config::WorkerConfig;
use iluvatar_lib::services::containers::containermanager::ContainerManager;
use log::*;

#[allow(unused)]
pub struct IluvatarWorkerImpl {
  container_manager: Arc<ContainerManager>,
  config: WorkerConfig,
  invoker: Arc<InvokerService>,
  status: Arc<StatusService>,
  health: Arc<WorkerHealthService>,
}

impl IluvatarWorkerImpl {
  pub fn new(config: WorkerConfig, container_manager: Arc<ContainerManager>, invoker: Arc<InvokerService>, status: Arc<StatusService>, health: Arc<WorkerHealthService>) -> IluvatarWorkerImpl {
    IluvatarWorkerImpl {
      container_manager,
      config,
      invoker,
      status,
      health
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
      info!("[{}] Handling invocation request {} {}", request.transaction_id, request.function_name, request.function_version);
      let resp = self.invoker.invoke(request).await;

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
      let request = request.into_inner();
      info!("[{}] Handling async invocation request {} {}", request.transaction_id, request.function_name, request.function_version);
      let resp = self.invoker.invoke_async(request);

      match resp {
        Ok( cookie ) => {
          let reply = InvokeAsyncResponse {
            lookup_cookie: cookie,
            success: true
          };
          Ok(Response::new(reply))
        },
        Err(e) => {
          error!("Failed to launch an async invocation with error '{}'", e);
          Ok(Response::new(InvokeAsyncResponse {
            lookup_cookie: "".to_string(),
            success: false
          }))
        },
      }
    }

  async fn invoke_async_check(&self, request: Request<InvokeAsyncLookupRequest>) -> Result<Response<InvokeResponse>, Status> {
    let request = request.into_inner();
    info!("[{}] Handling invoke async check", request.transaction_id);
    let resp = self.invoker.invoke_async_check(&request.lookup_cookie);
    match resp {
      Ok( resp ) => {
        Ok(Response::new(resp))
      },
      Err(e) => {
        error!("[{}] Failed to check async invocation status '{}'", request.transaction_id, e);
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
      info!("[{}] Handling prewarm request {} {}", request.transaction_id, request.function_name, request.function_version);
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
          error!("[{}] Prewarm failed with error: {}", request.transaction_id, e);
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
      info!("[{}] Handling register request {} {}", request.transaction_id, request.function_name, request.function_version);

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
          error!("[{}] Register failed with error {}", request.transaction_id, msg);
          let reply = RegisterResponse {
            success: false,
            function_json_result: format!("{{\"Error\": \"Error during registration of '{}': '{:?}\"}}", request.function_name, msg).into(),
          };
          Ok(Response::new(reply))        
        },
      }
    }

  async fn status(&self,
    request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
      let request = request.into_inner();
      info!("[{}] Handling status request", request.transaction_id);
      let stat = self.status.get_status(&request.transaction_id);

      let resp = StatusResponse { 
        success: true, 
        queue_len: stat.queue_len,
        used_mem: stat.used_mem,
        total_mem: stat.total_mem,
        cpu_us: stat.cpu_us,
        cpu_sy: stat.cpu_sy,
        cpu_id: stat.cpu_id,
        cpu_wa: stat.cpu_wa,
        load_avg_1minute: stat.load_avg_1minute,
        num_system_cores: stat.num_system_cores
      };
      Ok(Response::new(resp))
    }

  async fn health(&self,
    request: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
      let request = request.into_inner();
      info!("[{}] Handling health request", request.transaction_id);
      let reply = self.health.check_health(&request.transaction_id).await;
      Ok(Response::new(reply))
    }
}
