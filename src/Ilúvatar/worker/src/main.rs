extern crate iluvatar_worker;

use std::sync::Arc;
use std::time::Duration;
use iluvatar_lib::services::invocation::invoker::InvokerService;
use iluvatar_lib::services::status::status_service::StatusService;
use iluvatar_lib::transaction::{TransactionId, STARTUP_TID};
use iluvatar_lib::worker_api::config::Configuration;
use iluvatar_lib::services::containers::containermanager::ContainerManager;
use iluvatar_worker::args::parse;
use iluvatar_worker::ilúvatar_worker::IluvatarWorkerImpl;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_lib::utils::config::get_val;
use log::*;
use anyhow::Result;
use tonic::transport::Server;
use iluvatar_lib::services::{LifecycleFactory, WorkerHealthService};
use tracing::{info, Level};
use tracing_subscriber;
use tracing_subscriber::FmtSubscriber;
use tracing_flame::FlameLayer;
use tracing_subscriber::{prelude::*, fmt};
use tracing_subscriber::fmt::format::FmtSpan;

async fn run(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  debug!("[{}] loaded configuration = {:?}", tid, server_config);

  let factory = LifecycleFactory::new(server_config.container_resources.clone(), server_config.networking.clone());
  let lifecycle = factory.get_lifecycle_service(tid, true).await?;

  let container_man = ContainerManager::boxed(server_config.limits.clone(), server_config.container_resources.clone(), lifecycle.clone()).await?;
  let invoker = InvokerService::boxed(container_man.clone(), tid, server_config.limits.clone());
  let status = StatusService::boxed(container_man.clone(), invoker.clone(), server_config.graphite.clone(), server_config.name.clone()).await;
  let health = WorkerHealthService::boxed(invoker.clone(), container_man.clone(), tid).await?;

  let worker = IluvatarWorkerImpl::new(server_config.clone(), container_man, invoker, status, health);
  let addr = format!("{}:{}", server_config.address, server_config.port);

  iluvatar_worker::register_rpc_to_controller(server_config.clone(), tid.clone());
  Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr.parse()?)
      .await?;
  Ok(())
}

async fn clean(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  //let _logger = iluvatar_worker::logging::make_logger(&server_config, tid, WriteMode::Direct);
  debug!("[{}] loaded configuration = {:?}", tid, server_config);

  let factory = LifecycleFactory::new(server_config.container_resources.clone(), server_config.networking.clone());
  let lifecycle = factory.get_lifecycle_service(tid, false).await?;

  let container_man = ContainerManager::boxed(server_config.limits.clone(), server_config.container_resources.clone(), lifecycle.clone()).await?;
  let _invoker = InvokerService::boxed(container_man.clone(), tid, server_config.limits.clone());
  lifecycle.clean_containers("default", tid).await?;
  Ok(())
}

// fn setup_global_subscriber() -> impl Drop {
//     let fmt_layer = fmt::Layer::default();

//     let (flame_layer, _guard) = FlameLayer::with_file("./tracing.folded").unwrap();

//     tracing_subscriber::registry()
//         .with(fmt_layer)
//         .with(flame_layer)
//         .init();
//     _guard
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  iluvatar_lib::utils::file::ensure_temp_dir()?;
  let tid: &TransactionId = &STARTUP_TID;

  let args = parse();
  let config_pth = get_val("config", &args)?;

//  setup_global_subscriber();
    
  // let subscriber = FmtSubscriber::builder()
  //   // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
  //   // will be written to stdout.
  //   .with_max_level(Level::TRACE)
  //   .with_span_events(FmtSpan::FULL)
  //   // completes the builder.
  //   .finish();

    tracing_subscriber::fmt().with_max_level(Level::DEBUG).with_span_events(FmtSpan::FULL).init();  

 //   tracing::subscriber::set_global_default(subscriber)     .expect("setting default subscriber failed");
    
  //tracing_subscriber::fmt::init();
    
  match args.subcommand() {
    ("clean", Some(_)) => {
      let server_config = Configuration::boxed(true, &config_pth).unwrap();
      clean(server_config, tid).await?;      
    },
    (_,_) => { 
      let server_config = Configuration::boxed(false, &config_pth).unwrap();
      run(server_config, tid).await?;
     },
  };
  Ok(())
}
