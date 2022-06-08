extern crate iluvatar_worker;

use std::time::Duration;

use iluvatar_worker::config::Configuration;
use iluvatar_worker::iluvatar_worker::IluvatarWorkerImpl;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let settings = Configuration::new().unwrap();
  println!("configuration = {:?}", settings);

  let addr = format!("{}:{}", settings.address, settings.port);

  let addr = addr.parse()?;
  let worker = IluvatarWorkerImpl::new();

  Server::builder()
      .timeout(Duration::from_secs(settings.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr)
      .await?;

  Ok(())
}