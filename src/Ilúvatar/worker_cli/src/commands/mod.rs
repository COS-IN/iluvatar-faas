use crate::cli_config::Worker;
use crate::cli_config::args::{get_val, get_val_opt, get_val_mult, args_to_json};
use clap::ArgMatches;
use iluvatar_lib::rpc::RCPWorkerAPI;
use iluvatar_lib::worker_api::{WorkerAPI, HealthStatus};

pub async fn ping(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.ping().await.unwrap();
  println!("{}", ret)
}

pub async fn invoke(worker: Box<Worker>, args: &ArgMatches<'static>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();

  let function_name = get_val("name", &args);
  let version = get_val("version", &args);
  let arguments = args_to_json(get_val_mult("arguments", &args));
  let memory = get_val_opt("memory", &args);

  let ret = api.invoke(function_name, version, arguments, memory).await.unwrap();
  println!("{}", ret)
}

pub async fn invoke_async(worker: Box<Worker>, args: &ArgMatches<'static>) {

  let function_name = get_val("name", &args);
  let version = get_val("version", &args);
  let arguments = args_to_json(get_val_mult("arguments", &args));
  let memory = get_val_opt("memory", &args);

  println!("subargs: {:?} \n\n", args);
  println!("function arguments: {:?}", arguments);

  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.invoke_async(function_name, version, arguments, memory).await.unwrap();
  println!("{}", ret)
}

pub async fn invoke_async_check(worker: Box<Worker>, args: &ArgMatches<'static>) {
  let cookie = get_val("cookie", &args);

  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.invoke_async_check(&cookie).await.unwrap();
  println!("{}", ret)
}

pub async fn prewarm(worker: Box<Worker>, args: &ArgMatches<'static>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let function_name = get_val("name", &args);
  let version = get_val("version", &args);
  let memory = get_val_opt("memory", &args);
  let cpu = get_val_opt("cpu", &args);
  let image = get_val_opt("image", &args);

  let result = api.prewarm(function_name, version, memory, cpu, image).await;
  match result {
    Ok(string) => println!("{}", string),
    Err(err) => println!("{}", err),
  }
}

pub async fn register(worker: Box<Worker>, args: &ArgMatches<'static>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let function_name = get_val("name", &args);
  let version = get_val("version", &args);
  let memory = get_val("memory", &args);
  let cpu = get_val("cpu", &args);
  let image = get_val("image", &args);
  let parallels = get_val("parallel-invokes", &args);

  let ret = api.register(function_name, version, image, memory, cpu, parallels).await.unwrap();
  println!("{}", ret)
}

pub async fn status(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.status().await.unwrap();
  println!("{}", ret)
}

pub async fn health(worker: Box<Worker>) {
  let mut api = RCPWorkerAPI::new(worker.address, worker.port).await.unwrap();
  let ret = api.health().await.unwrap();
  match ret {
    HealthStatus::HEALTHY => println!("Worker is healthy"),
    HealthStatus::UNHEALTHY => println!("Worker is unhealthy"),
  }
}