use crate::args::Worker;
use iluvatar_library::utils::config::{get_val, get_val_opt, get_val_mult, args_to_json};
use clap::ArgMatches;
use iluvatar_worker_library::{rpc::RPCWorkerAPI, worker_api::{WorkerAPI, HealthStatus}};
use iluvatar_library::transaction::gen_tid;
use anyhow::Result;

pub async fn ping(worker: Box<Worker>) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;
  let ret = api.ping(gen_tid()).await.unwrap();
  println!("{}", ret);
  Ok(())
}

pub async fn invoke(worker: Box<Worker>, args: &ArgMatches) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;

  let function_name = get_val("name", &args)?;
  let version = get_val("version", &args)?;
  let arguments = args_to_json(get_val_mult("arguments", &args));
  let memory = get_val_opt("memory", &args);

  let ret = api.invoke(function_name, version, arguments, memory, gen_tid()).await.unwrap();
  println!("{:?}", ret);
  Ok(())
}

pub async fn invoke_async(worker: Box<Worker>, args: &ArgMatches) -> Result<()> {

  let function_name = get_val("name", &args)?;
  let version = get_val("version", &args)?;
  let arguments = args_to_json(get_val_mult("arguments", &args));
  let memory = get_val_opt("memory", &args);

  println!("subargs: {:?} \n\n", args);
  println!("function arguments: {:?}", arguments);

  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;
  let ret = api.invoke_async(function_name, version, arguments, memory, gen_tid()).await.unwrap();
  println!("{}", ret);
  Ok(())
}

pub async fn invoke_async_check(worker: Box<Worker>, args: &ArgMatches) -> Result<()> {
  let cookie = get_val("cookie", &args)?;

  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;
  let ret = api.invoke_async_check(&cookie, gen_tid()).await.unwrap();
  println!("{}", ret.json_result);
  Ok(())
}

pub async fn prewarm(worker: Box<Worker>, args: &ArgMatches) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;
  let function_name = get_val("name", &args)?;
  let version = get_val("version", &args)?;
  let memory = get_val_opt("memory", &args);
  let cpu = get_val_opt("cpu", &args);
  let image = get_val_opt("image", &args);

  let result = api.prewarm(function_name, version, memory, cpu, image, gen_tid()).await;
  match result {
    Ok(string) => println!("{}", string),
    Err(err) => println!("{}", err),
  };
  Ok(())
}

pub async fn register(worker: Box<Worker>, args: &ArgMatches) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;
  let function_name = get_val("name", &args)?;
  let version = get_val("version", &args)?;
  let memory = get_val("memory", &args)?;
  let cpu = get_val("cpu", &args)?;
  let image = get_val("image", &args)?;
  let parallels = get_val("parallel-invokes", &args)?;

  let ret = api.register(function_name, version, image, memory, cpu, parallels, gen_tid()).await.unwrap();
  println!("{}", ret);
  Ok(())
}

pub async fn status(worker: Box<Worker>) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;
  let ret = api.status(gen_tid()).await.unwrap();
  println!("{:?}", ret);
  Ok(())
}

pub async fn health(worker: Box<Worker>) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port).await?;
  let ret = api.health(gen_tid()).await.unwrap();
  match ret {
    HealthStatus::HEALTHY => println!("Worker is healthy"),
    HealthStatus::UNHEALTHY => println!("Worker is unhealthy"),
  };
  Ok(())
}