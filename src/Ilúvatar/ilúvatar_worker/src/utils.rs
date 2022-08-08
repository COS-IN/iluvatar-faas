use std::sync::Arc;
use iluvatar_lib::{worker_api::worker_config::Configuration, transaction::TransactionId, load_balancer_api::register_worker};
use tracing::{debug, info, error};
use clap::{ArgMatches, App, SubCommand, Arg};

pub fn parse() -> ArgMatches<'static> {
  App::new("ilúvatar_worker")
    .version("0.1.0")
    .about("Ilúvatar worker")
    .arg(Arg::with_name("config")
      .short("c")
      .long("config")
      .help("Path to a configuration file to use")
      .required(false)
      .default_value("/tmp/foo/bar")
      .takes_value(true))   
    .arg(Arg::with_name("direct-logs")
      .long("direct-logs")
      .help("Use direct mode for writing logs, rather than async version. Helpful for debugging")
      .required(false))  
    .subcommand(SubCommand::with_name("clean")
                .about("Clean up the system from possible previous executions"))
    .get_matches()
}

pub fn register_rpc_to_controller(server_config: Arc<Configuration>, tid: TransactionId) {
  let _ = tokio::spawn(async move {
      debug!(tid=%tid, "Controller registration thread started");

      // allow RPC server time to start up
      tokio::time::sleep(std::time::Duration::from_secs(5)).await;
      let comm_method = "RPC".to_string();

      let result = register_worker(&server_config.name, &comm_method, &server_config.container_resources.backend,
      &server_config.address, server_config.port, server_config.container_resources.memory_mb, server_config.container_resources.cores,
        &server_config.load_balancer_url, &tid).await;

      match result {
        Ok(_) => info!(tid=%tid, "Worker successfully registered with controller"),
        Err(e) => error!(tid=%tid, error=%e, "Worker registration failed"),
      }
    }
  );
}
