pub mod config;

pub mod iluvatar_worker {

use tonic::{Request, Response, Status};
 
 use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorker;
 use iluvatar_lib::rpc::{PingRequest, PingResponse, InvokeRequest, InvokeResponse, RegisterRequest, RegisterResponse, StatusRequest, StatusResponse};
 
 #[derive(Debug, Default)]
 pub struct MyPinger {}
 
 #[tonic::async_trait]
 impl IluvatarWorker for MyPinger {
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

  async fn register(&self,
    request: Request<RegisterRequest>) -> Result<Response<RegisterResponse>, Status> {
      let reply = RegisterResponse {
        function_json_result: format!("'Error': 'register for '{}' not not implemented'", request.into_inner().function_name).into(),
      };
      Ok(Response::new(reply))    
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
      // Ok(Response::new(reply))
      Err(Status::aborted("failed"))
    }
 }
}
 