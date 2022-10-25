use actix_web::{web::Data, App, HttpServer};
use iluvatar_library::logging::start_tracing;
use iluvatar_library::transaction::{LOAD_BALANCER_TID, TransactionId};
use iluvatar_library::utils::config_utils::get_val;
use tracing::info;
use crate::args::parse;
use iluvatar_controller_library::controller::{controller::Controller, config::Configuration, web_server::*};
use signal_hook::{consts::signal::{SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGQUIT}, iterator::Signals};

pub mod args;

#[tokio::main]
async fn main() -> std::io::Result<()> {
  iluvatar_library::utils::file::ensure_temp_dir().unwrap();
  let tid: &TransactionId = &LOAD_BALANCER_TID;
  let args = parse();
  let config_pth = get_val("config", &args).unwrap();

  let config = Configuration::boxed(&config_pth).unwrap();
  let _guard = start_tracing(config.logging.clone(), config.graphite.clone(), &config.name, tid).unwrap();

  let server = Controller::new(config.clone(), tid);
  let server_data = Data::new(server);

  let sigs = vec![SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGQUIT];
  let mut signals = Signals::new(&sigs)?;

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

  'outer: for signal in &mut signals {
    match signal {
      _term_sig => { // got a termination signal
        break 'outer;
      }
    }
  }
  iluvatar_library::continuation::GLOB_CONT_CHECK.signal_application_exit(tid);
  Ok(())
}
