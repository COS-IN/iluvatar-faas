use std::sync::Arc;
use std::time::Duration;
use iluvatar_library::{logging::start_tracing, nproc};
use iluvatar_worker_library::services::{invocation::invoker::InvokerService, containers::LifecycleFactory};
use iluvatar_library::transaction::{TransactionId, STARTUP_TID};
use iluvatar_worker_library::worker_api::config::Configuration;
use iluvatar_worker_library::services::containers::containermanager::ContainerManager;
use iluvatar_worker_library::worker_api::create_worker;
use tokio::runtime::Runtime;
use crate::utils::{parse, register_rpc_to_controller};
use iluvatar_worker_library::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_library::utils::config::get_val;
use anyhow::Result;
use tonic::transport::Server;
use tracing::debug;
use signal_hook::{consts::signal::{SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGQUIT}, iterator::Signals};

pub mod utils;

async fn run(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  debug!(tid=tid.as_str(), config=?server_config, "loaded configuration");

  let sigs = vec![SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGQUIT];
  let mut signals = Signals::new(&sigs)?;

  let worker = create_worker(server_config.clone(), tid).await?;
  let addr = format!("{}:{}", server_config.address, server_config.port);

  register_rpc_to_controller(server_config.clone(), tid.clone());
  let _j = tokio::spawn(Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr.parse()?));

  for signal in &mut signals {
    match signal {
      _term_sig => { // got a termination signal
        break;
      }
    }
  }
  iluvatar_library::continuation::GLOB_CONT_CHECK.signal_application_exit(tid);
  Ok(())
}

async fn clean(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  debug!(tid=?tid, config=?server_config, "loaded configuration");

  let factory = LifecycleFactory::new(server_config.container_resources.clone(), server_config.networking.clone(), server_config.limits.clone());
  let lifecycle = factory.get_lifecycle_service(tid, false).await?;

  let container_man = ContainerManager::boxed(server_config.limits.clone(), server_config.container_resources.clone(), lifecycle.clone(), tid).await?;
  let _invoker = InvokerService::boxed(container_man.clone(), tid, server_config.limits.clone(), server_config.invocation.clone());
  lifecycle.clean_containers("default", tid).await?;
  Ok(())
}

fn build_runtime(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<Runtime> {
  match tokio::runtime::Builder::new_multi_thread()
      .worker_threads(nproc(tid)? as usize)
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

  let args = parse();
  let config_pth = get_val("config", &args)?;


  match args.subcommand() {
    Some(("clean", _)) => {
      let server_config = Configuration::boxed(true, &config_pth).unwrap();
      let _guard = start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name)?;
      let worker_rt = build_runtime(server_config.clone(), tid)?;
      worker_rt.block_on(clean(server_config, tid))?;
    },
    None => { 
      let server_config = Configuration::boxed(false, &config_pth).unwrap();
      let _guard = start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name)?;
      let worker_rt = build_runtime(server_config.clone(), tid)?;
      worker_rt.block_on(run(server_config, tid))?;
    },
    Some((cmd, _)) => panic!("Unknown command {}, try --help", cmd),
  };
  Ok(())
}
