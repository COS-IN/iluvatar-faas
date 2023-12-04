use crate::args::{AsyncCheck, InvokeArgs, PrewarmArgs, RegisterArgs};
use anyhow::Result;
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::utils::{config::args_to_json, port::Port};
use iluvatar_worker_library::{
    rpc::RPCWorkerAPI,
    worker_api::{HealthStatus, WorkerAPI},
};

pub async fn ping(host: String, port: Port) -> Result<()> {
    let mut api = RPCWorkerAPI::new(&host, port, &gen_tid()).await?;
    let ret = api.ping(gen_tid()).await.unwrap();
    println!("{}", ret);
    Ok(())
}

pub async fn invoke(host: String, port: Port, args: InvokeArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;

    let arguments = match args.arguments.as_ref() {
        Some(a) => args_to_json(a)?,
        None => "{}".to_string(),
    };

    let ret = api.invoke(args.name, args.version, arguments, tid).await.unwrap();
    println!("{}", serde_json::to_string(&ret)?);
    Ok(())
}

pub async fn invoke_async(host: String, port: Port, args: InvokeArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;

    let arguments = match args.arguments.as_ref() {
        Some(a) => args_to_json(a)?,
        None => "{}".to_string(),
    };
    let ret = api.invoke_async(args.name, args.version, arguments, tid).await.unwrap();
    println!("{}", ret);
    Ok(())
}

pub async fn invoke_async_check(host: String, port: Port, args: AsyncCheck) -> Result<()> {
    let tid = gen_tid();

    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;
    let ret = api.invoke_async_check(&args.cookie, gen_tid()).await.unwrap();
    println!("{}", serde_json::to_string(&ret)?);
    Ok(())
}

pub async fn prewarm(host: String, port: Port, args: PrewarmArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;
    let c = match &args.compute {
        Some(c) => c.into(),
        None => iluvatar_library::types::Compute::CPU,
    };
    let result = api.prewarm(args.name, args.version, tid, c).await;
    match result {
        Ok(string) => println!("{}", string),
        Err(err) => println!("{}", err),
    };
    Ok(())
}

pub async fn register(host: String, port: Port, args: RegisterArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;
    let iso = args.isolation.into();
    let compute = args.compute.into();
    let ret = api
        .register(
            args.name,
            args.version,
            args.image,
            args.memory,
            args.cpu,
            1,
            tid,
            iso,
            compute,
            None,
        )
        .await
        .unwrap();
    println!("{}", ret);
    Ok(())
}

pub async fn status(host: String, port: Port) -> Result<()> {
    let mut api = RPCWorkerAPI::new(&host, port, &gen_tid()).await?;
    let ret = api.status(gen_tid()).await.unwrap();
    println!("{:?}", ret);
    Ok(())
}

pub async fn health(host: String, port: Port) -> Result<()> {
    let mut api = RPCWorkerAPI::new(&host, port, &gen_tid()).await?;
    let ret = api.health(gen_tid()).await.unwrap();
    match ret {
        HealthStatus::HEALTHY => println!("Worker is healthy"),
        HealthStatus::UNHEALTHY => println!("Worker is unhealthy"),
    };
    Ok(())
}
