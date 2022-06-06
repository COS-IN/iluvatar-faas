use crate::cli_config::Worker;
use crate::cli_config::args::get_val;
use clap::ArgMatches;
use iluvatar_lib::rpc::RCPWorkerAPI;
use iluvatar_lib::worker_api::WorkerAPI;

pub async fn ping(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.ping().await.unwrap();
  println!("{}", ret)
}

pub async fn invoke(worker: Box<Worker>, args: &ArgMatches<'static>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();

  let function_name = get_val("name", &args);
  let version = get_val("version", &args);

  let ret = api.invoke(function_name, version).await.unwrap();
  println!("{}", ret)
}

pub async fn register(worker: Box<Worker>, args: &ArgMatches<'static>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let function_name = get_val("name", &args);
  let version = get_val("version", &args);

  let ret = api.register(function_name, version).await.unwrap();
  println!("{}", ret)
}

pub async fn status(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.status().await.unwrap();
  println!("{}", ret)
}