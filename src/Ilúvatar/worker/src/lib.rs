pub mod config;

pub mod iluvatar_worker {
 use tonic::{Request, Response, Status};
 
 use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorker;
 use iluvatar_lib::rpc::{PingRequest, PingResponse};
 
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
 }
}
 