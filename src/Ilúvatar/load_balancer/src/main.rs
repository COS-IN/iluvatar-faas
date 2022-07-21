use actix_web::{web::Data, App, HttpServer};
use iluvatar_lib::logging::start_tracing;
use iluvatar_lib::transaction::{LOAD_BALANCER_TID, TransactionId};
use iluvatar_lib::utils::config_utils::get_val;
use tracing::info;
use crate::args::parse;
use crate::controller::Controller;
use crate::web_server::*;
use iluvatar_lib::load_balancer_api::config::Configuration;

pub mod web_server;
pub mod controller;
pub mod services;
pub mod args;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  iluvatar_lib::utils::file::ensure_temp_dir().unwrap();
  let tid: &TransactionId = &LOAD_BALANCER_TID;
  let args = parse();
  let config_pth = get_val("config", &args).unwrap();

  let config = Configuration::boxed(&config_pth).unwrap();
  let _guard = start_tracing(config.logging.clone()).unwrap();

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
