extern crate iluvatar_worker;

use std::time::Duration;

use iluvatar_worker::config::Configuration;
use iluvatar_worker::iluvatar_worker::IluvatarWorkerImpl;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let server_config = Configuration::boxed().unwrap();
  println!("configuration = {:?}", server_config);

  let addr = format!("{}:{}", server_config.address, server_config.port);

  let addr = addr.parse()?;
  let worker = IluvatarWorkerImpl::new(server_config.clone());

  Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr)
      .await?;

  Ok(())
}