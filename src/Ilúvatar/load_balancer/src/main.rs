use actix_web::{web::Data, App, HttpServer};
use iluvatar_lib::transaction::{LOAD_BALANCER_TID, TransactionId};
use iluvatar_lib::utils::config_utils::get_val;

pub mod web_server;
pub mod controller;
pub mod logging;
pub mod services;
pub mod args;

use crate::args::parse;
use crate::controller::Controller;
use crate::web_server::*;
use crate::logging::make_logger;
use log::info;
use iluvatar_lib::load_balancer_api::config::Configuration;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  iluvatar_lib::utils::file::ensure_temp_dir().unwrap();
  let tid: &TransactionId = &LOAD_BALANCER_TID;
  let args = parse();
  let config_pth = get_val("config", &args).unwrap();

  let config = Configuration::boxed(&config_pth).unwrap();
  make_logger(config.logging.clone(), tid, flexi_logger::WriteMode::Direct);

  let server = Controller::new(config.clone());
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
