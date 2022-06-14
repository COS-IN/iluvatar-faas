extern crate iluvatar_worker;

use std::time::Duration;
use flexi_logger::{Logger, FileSpec, WriteMode};
use iluvatar_worker::config::Configuration;
use iluvatar_worker::iluvatar_worker::IluvatarWorkerImpl;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_worker::network::namespace_manager::NamespaceManagerInternal;
use log::*;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let server_config = Configuration::boxed().unwrap();

  let _logger = Logger::try_with_str(server_config.logging.level.as_str())?
        .log_to_file(FileSpec::default()
                        .directory(server_config.logging.directory.as_str())
                        .basename(server_config.logging.basename.as_str())
                      )
        .write_mode(WriteMode::Async)
        .create_symlink("iluvitar_worker.log")
        .print_message()
        .start()?;

  debug!("loaded configuration = {:?}", server_config);

  let addr = format!("{}:{}", server_config.address, server_config.port);
  let addr = addr.parse()?;

  let netm = NamespaceManagerInternal::new(server_config.clone());
  netm.ensure_bridge()?;

  let worker = IluvatarWorkerImpl::new(server_config.clone(), netm);

  Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr)
      .await?;

  Ok(())
}