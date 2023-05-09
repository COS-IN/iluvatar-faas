use actix_web::{web::Data, App, HttpServer};
use clap::{Parser, command};
use iluvatar_library::logging::start_tracing;
use iluvatar_library::transaction::{LOAD_BALANCER_TID, TransactionId};
use iluvatar_library::utils::wait_for_exit_signal;
use tracing::info;
use iluvatar_controller_library::controller::{controller::Controller, config::Configuration, web_server::*};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
  #[arg(short, long)]
  /// Path to a configuration file to use
  pub config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  iluvatar_library::utils::file::ensure_temp_dir().unwrap();
  let tid: &TransactionId = &LOAD_BALANCER_TID;
  let args = Args::parse();

  let config = Configuration::boxed(&args.config).unwrap();
  let _guard = start_tracing(config.logging.clone(), config.graphite.clone(), &config.name, tid).unwrap();

  let server = Controller::new(config.clone(), tid).await?;
  let server_data = Data::new(server);

  info!(tid=%tid, "Controller started!");

  let _handle = tokio::spawn(HttpServer::new(move || {
      App::new()
      .app_data(server_data.clone())
          .service(ping)
          .service(invoke_api)
          .service(invoke_async_api)
          .service(invoke_async_check_api)
          .service(prewarm_api)
          .service(register_function_api)
          .service(register_worker_api)
  })
  .bind((config.address.clone(), config.port))?
  .run());

  wait_for_exit_signal(tid).await?;
  Ok(())
}
