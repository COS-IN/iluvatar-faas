use crate::cli_config::Worker;
use iluvatar_lib::rpc::RCPWorkerAPI;
use iluvatar_lib::worker_api::WorkerAPI;

pub fn ping(worker: Box<Worker>) {
  let api = RCPWorkerAPI::new(worker.address, worker.port);
  api.ping().unwrap();
}

pub fn invoke(worker: Box<Worker>) {
  let api = RCPWorkerAPI::new(worker.address, worker.port);
  api.invoke().unwrap();
}

pub fn register(worker: Box<Worker>) {
  let api = RCPWorkerAPI::new(worker.address, worker.port);
  api.register().unwrap();
}

pub fn status(worker: Box<Worker>) {
  let api = RCPWorkerAPI::new(worker.address, worker.port);
  api.status().unwrap();
}