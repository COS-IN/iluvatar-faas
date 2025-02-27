use crate::args::{AsyncCheck, InvokeArgs, PrewarmArgs, RegisterArgs};
use anyhow::Result;
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::types::HealthStatus;
use iluvatar_library::utils::{config::args_to_json, port::Port};
use iluvatar_worker_library::worker_api::{rpc::RPCWorkerAPI, WorkerAPI};
use tracing::{error, info};

pub async fn ping(host: String, port: Port) -> Result<()> {
    let mut api = RPCWorkerAPI::new(&host, port, &gen_tid()).await?;
    let ret = api.ping(gen_tid()).await?;
    info!("{}", ret);
    Ok(())
}

pub async fn invoke(host: String, port: Port, args: InvokeArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;

    let arguments = match args.arguments.as_ref() {
        Some(a) => args_to_json(a)?,
        None => "{}".to_string(),
    };

    let ret = api.invoke(args.name, args.version, arguments, tid).await?;
    info!("{}", serde_json::to_string(&ret)?);
    Ok(())
}

pub async fn invoke_async(host: String, port: Port, args: InvokeArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;

    let arguments = match args.arguments.as_ref() {
        Some(a) => args_to_json(a)?,
        None => "{}".to_string(),
    };
    let ret = api.invoke_async(args.name, args.version, arguments, tid).await?;
    info!("{}", ret);
    Ok(())
}

pub async fn invoke_async_check(host: String, port: Port, args: AsyncCheck) -> Result<()> {
    let tid = gen_tid();

    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;
    let ret = api.invoke_async_check(&args.cookie, gen_tid()).await?;
    info!("{}", serde_json::to_string(&ret)?);
    Ok(())
}

pub async fn prewarm(host: String, port: Port, args: PrewarmArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;
    let result = api.prewarm(args.name, args.version, tid, args.compute).await;
    match result {
        Ok(string) => info!("{}", string),
        Err(err) => error!("{}", err),
    };
    Ok(())
}

pub async fn register(host: String, port: Port, args: RegisterArgs) -> Result<()> {
    let tid = gen_tid();
    let mut api = RPCWorkerAPI::new(&host, port, &tid).await?;
    let ret = api
        .register(
            args.name,
            args.version,
            args.image,
            args.memory,
            args.cpu,
            1,
            tid,
            args.isolation.into(),
            args.compute.into(),
            args.server,
            None,
        )
        .await?;
    info!("{}", ret);
    Ok(())
}

pub async fn list_registered_funcs(host: String, port: Port) -> Result<()> {
    let mut api = RPCWorkerAPI::new(&host, port, &gen_tid()).await?;
    let ret = api.list_registered_funcs(gen_tid()).await?;
    if let Ok(pretty_json) = serde_json::from_str::<serde_json::Value>(&ret.function_list) {
        info!("{}", serde_json::to_string_pretty(&pretty_json).unwrap());
    } else {
        info!("Failed to parse function_list JSON");
    }
    Ok(())
}

pub async fn status(host: String, port: Port) -> Result<()> {
    let mut api = RPCWorkerAPI::new(&host, port, &gen_tid()).await?;
    let ret = api.status(gen_tid()).await?;
    info!("{:?}", ret);
    Ok(())
}

pub async fn health(host: String, port: Port) -> Result<()> {
    let mut api = RPCWorkerAPI::new(&host, port, &gen_tid()).await?;
    let ret = api.health(gen_tid()).await?;
    match ret {
        HealthStatus::HEALTHY => info!("Worker is healthy"),
        HealthStatus::UNHEALTHY => info!("Worker is unhealthy"),
        HealthStatus::OFFLINE => info!("Worker is unresponsive"),
    };
    Ok(())
}
