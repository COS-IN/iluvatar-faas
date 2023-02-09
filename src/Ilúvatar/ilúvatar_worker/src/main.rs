use std::time::Duration;
use clap::Parser;
use iluvatar_library::{logging::start_tracing, nproc, bail_error, utils::wait_for_exit_signal};
use iluvatar_worker_library::{services::containers::LifecycleFactory, worker_api::config::WorkerConfig};
use iluvatar_library::transaction::{TransactionId, STARTUP_TID};
use iluvatar_worker_library::worker_api::config::Configuration;
use iluvatar_worker_library::worker_api::create_worker;
use tokio::runtime::Runtime;
use utils::Args;
use crate::utils::register_rpc_to_controller;
use iluvatar_worker_library::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use anyhow::Result;
use tonic::transport::Server;
use tracing::{debug, info};

pub mod utils;

async fn run(server_config: WorkerConfig, tid: &TransactionId) -> Result<()> {
  debug!(tid=tid.as_str(), config=?server_config, "loaded configuration");

  let worker = match create_worker(server_config.clone(), tid, false).await {
    Ok(w) => w,
    Err(e) => bail_error!(tid=%tid, error=%e, "Error creating worker on startup"),
  };
  let addr = format!("{}:{}", server_config.address, server_config.port);

  register_rpc_to_controller(server_config.clone(), tid.clone());
  info!(tid=%tid, "Starting RPC server");
  let _j = tokio::spawn(Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr.parse()?));

  wait_for_exit_signal(tid).await?;
  Ok(())
}

async fn clean(server_config: WorkerConfig, tid: &TransactionId) -> Result<()> {
  debug!(tid=?tid, config=?server_config, "loaded configuration");

  let factory = LifecycleFactory::new(server_config.container_resources.clone(), server_config.networking.clone(), server_config.limits.clone());
  let lifecycles = factory.get_lifecycle_services(tid, false, false).await?;

  for (_, lifecycle) in lifecycles.iter() {
    lifecycle.clean_containers("default", lifecycle.clone(), tid).await?;
  }
  Ok(())
}

fn build_runtime(server_config: WorkerConfig, tid: &TransactionId) -> Result<Runtime> {
  match tokio::runtime::Builder::new_multi_thread()
      .worker_threads(nproc(tid, false)? as usize)
      .enable_all()
      .event_interval(server_config.tokio_event_interval)
      .global_queue_interval(server_config.tokio_queue_interval)
      .build() {
    Ok(rt) => Ok(rt),
    Err(e) => { 
      anyhow::bail!(format!("Tokio thread runtime for main failed to start because: {}", e));
    },
  }
}

fn main() -> Result<()> {
  iluvatar_library::utils::file::ensure_temp_dir()?;
  let tid: &TransactionId = &STARTUP_TID;

  let cli = Args::parse();

  match cli.command {
    Some(c) => match c {
        utils::Commands::Clean => {
          let server_config = Configuration::boxed(true, &cli.config.as_ref()).unwrap();
          let _guard = start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name, tid)?;
          let worker_rt = build_runtime(server_config.clone(), tid)?;
          worker_rt.block_on(clean(server_config, tid))?;
        },
    },
    None => {
      let server_config = Configuration::boxed(false, &cli.config.as_ref()).unwrap();
      let _guard = start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name, tid)?;
      let worker_rt = build_runtime(server_config.clone(), tid)?;
      worker_rt.block_on(run(server_config, tid))?;
    },
  }
  Ok(())
}
