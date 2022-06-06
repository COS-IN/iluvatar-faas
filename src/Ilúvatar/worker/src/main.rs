extern crate iluvatar_worker;

use iluvatar_worker::config::Configuration;
use iluvatar_worker::iluvatar_worker::MyPinger;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let settings = Configuration::new().unwrap();
  println!("configuration = {:?}", settings);

  let addr = format!("{}:{}", settings.address, settings.port);

  let addr = addr.parse()?;
  let greeter = MyPinger::default();

  Server::builder()
      .add_service(IluvatarWorkerServer::new(greeter))
      .serve(addr)
      .await?;

  Ok(())
}