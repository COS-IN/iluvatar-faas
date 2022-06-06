use crate::cli_config::Worker;
use iluvatar_lib::rpc::RCPWorkerAPI;
use iluvatar_lib::worker_api::WorkerAPI;

pub async fn ping(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.ping().await.unwrap();
  println!("{}", ret)
}

pub async fn invoke(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.invoke().await.unwrap();
  println!("{}", ret)
}

pub async fn register(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.register().await.unwrap();
  println!("{}", ret)
}

pub async fn status(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.status().await.unwrap();
  println!("{}", ret)
}