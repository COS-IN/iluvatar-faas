use std::sync::Arc;
use actix_web::{web::Data, App, HttpServer};
use iluvatar_lib::transaction::{LOAD_BALANCER_TID, TransactionId};
use iluvatar_lib::utils::config_utils::get_val;
use anyhow::Result;
use tracing::info;
use tracing::metadata::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;
use crate::args::parse;
use crate::controller::Controller;
use crate::web_server::*;
use iluvatar_lib::load_balancer_api::config::Configuration;

pub mod web_server;
pub mod controller;
pub mod services;
pub mod args;

fn start_tracing(server_config: Arc<Configuration>) -> Result<WorkerGuard> {
  let file_appender = tracing_appender::rolling::never(server_config.logging.directory.clone(), server_config.logging.basename.clone());
  let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

  tracing_subscriber::fmt()
    .json()
    .with_max_level(server_config.logging.level.parse::<LevelFilter>()?)
    .with_span_events(FmtSpan::FULL)
    .with_writer(non_blocking)
    .init();
  Ok(_guard)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  iluvatar_lib::utils::file::ensure_temp_dir().unwrap();
  let tid: &TransactionId = &LOAD_BALANCER_TID;
  let args = parse();
  let config_pth = get_val("config", &args).unwrap();

  let config = Configuration::boxed(&config_pth).unwrap();
  let _guard = start_tracing(config.clone()).unwrap();

  let server = Controller::new(config.clone(), tid);
  let server_data = Data::new(server);

  info!("[{}] Load balancer started!", tid);

  HttpServer::new(move || {
      App::new()
      .app_data(server_data.clone())
          .service(ping)
          .service(invoke)
          .service(invoke_async)
          .service(invoke_async_check)
          .service(prewarm)
          .service(register_function)
          .service(register_worker)
  })
  .bind((config.address.clone(), config.port))?
  .run()
  .await
}
