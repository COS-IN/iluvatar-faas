use crate::args::{Worker, InvokeArgs, AsyncCheck, PrewarmArgs, RegisterArgs};
use iluvatar_library::utils::config::args_to_json;
use iluvatar_worker_library::{rpc::RPCWorkerAPI, worker_api::{WorkerAPI, HealthStatus}};
use iluvatar_library::transaction::gen_tid;
use anyhow::Result;

pub async fn ping(worker: Box<Worker>) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &gen_tid()).await?;
  let ret = api.ping(gen_tid()).await.unwrap();
  println!("{}", ret);
  Ok(())
}

pub async fn invoke(worker: Box<Worker>, args: InvokeArgs) -> Result<()> {
  let tid = gen_tid();
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &tid).await?;

  let arguments = match args.arguments.as_ref() {
    Some(a) => args_to_json(a),
    None => "{}".to_string(),
  };

  let ret = api.invoke(args.name, args.version, arguments, tid).await.unwrap();
  println!("{:?}", ret);
  Ok(())
}

pub async fn invoke_async(worker: Box<Worker>, args: InvokeArgs) -> Result<()> {
  let tid = gen_tid();
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &tid).await?;

  let arguments = match args.arguments.as_ref() {
    Some(a) => args_to_json(a),
    None => "{}".to_string(),
  };
  let ret = api.invoke_async(args.name, args.version, arguments, tid).await.unwrap();
  println!("{}", ret);
  Ok(())
}

pub async fn invoke_async_check(worker: Box<Worker>, args: AsyncCheck) -> Result<()> {
  // let cookie = get_val("cookie", &args)?;
  let tid = gen_tid();

  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &tid).await?;
  let ret = api.invoke_async_check(&args.cookie, gen_tid()).await.unwrap();
  println!("{}", ret.json_result);
  Ok(())
}

pub async fn prewarm(worker: Box<Worker>, args: PrewarmArgs) -> Result<()> {
  let tid = gen_tid();
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &tid).await?;

  let result = api.prewarm(args.name, args.version, tid).await;
  match result {
    Ok(string) => println!("{}", string),
    Err(err) => println!("{}", err),
  };
  Ok(())
}

pub async fn register(worker: Box<Worker>, args: RegisterArgs) -> Result<()> {
  let tid = gen_tid();
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &tid).await?;

  let iso = args.isolation.into();
  let ret = api.register(args.name, args.version, args.image, args.memory, args.cpu, 1, tid, iso).await.unwrap();
  println!("{}", ret);
  Ok(())
}

pub async fn status(worker: Box<Worker>) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &gen_tid()).await?;
  let ret = api.status(gen_tid()).await.unwrap();
  println!("{:?}", ret);
  Ok(())
}

pub async fn health(worker: Box<Worker>) -> Result<()> {
  let mut api = RPCWorkerAPI::new(&worker.address, worker.port, &gen_tid()).await?;
  let ret = api.health(gen_tid()).await.unwrap();
  match ret {
    HealthStatus::HEALTHY => println!("Worker is healthy"),
    HealthStatus::UNHEALTHY => println!("Worker is unhealthy"),
  };
  Ok(())
}