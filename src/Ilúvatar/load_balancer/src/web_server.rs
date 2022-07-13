use crate::controller::Controller;
use actix_web::{HttpRequest, HttpResponse, get, post};
use actix_web::web::{Data, Json};
use iluvatar_lib::load_balancer_api::structs::{Invoke, RegisterWorker, Prewarm, RegisterFunction};
use iluvatar_lib::transaction::gen_tid;
use iluvatar_lib::utils::calculate_fqdn;
use log::*;

#[get("/ping")]
pub async fn ping(server: Data<Controller>, _req: HttpRequest) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new ping", tid);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/invoke")]
pub async fn invoke(server: Data<Controller>, req: Json<Invoke>) -> HttpResponse {
  let tid = gen_tid();
  let req = req.into_inner();
  info!("[{}] new invoke {:?}", tid, req);

  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/invoke_async")]
pub async fn invoke_async(server: Data<Controller>, req: Json<Invoke>) -> HttpResponse {
  let tid = gen_tid();
  let req = req.into_inner();
  info!("[{}] new invoke_async {:?}", tid, req);

  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[get("/invoke_async_check")]
pub async fn invoke_async_check(server: Data<Controller>, req: Json<Invoke>) -> HttpResponse {
  let tid = gen_tid();
  let req = req.into_inner();
  info!("[{}] new invoke_async_check {:?}", tid, req);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/prewarm")]
pub async fn prewarm(server: Data<Controller>, req: Json<Prewarm>) -> HttpResponse {
  let tid = gen_tid();
  let req = req.into_inner();
  info!("[{}] new prewar, {:?}", tid, req);

  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/register_function")]
pub async fn register_function(server: Data<Controller>, req: Json<RegisterFunction>) -> HttpResponse {
  let tid = gen_tid();
  let req = req.into_inner();
  let fqdn = calculate_fqdn(&req.function_name, &req.function_version);

  info!("[{}] new register_function {:?}", tid, req);
  match server.register_function(req, &tid).await {
    Ok(_) => HttpResponse::Ok().finish(),
    Err(e) => {
      error!("[{}] the web server got an error trying to register function {} {}", tid, fqdn, e);
      HttpResponse::Ok().finish()
    },
  }
}

#[post("/register_worker")]
pub async fn register_worker(server: Data<Controller>, req: Json<RegisterWorker>) -> HttpResponse {
  let tid = gen_tid();
  let req = req.into_inner();
  let name = req.name.clone();
  info!("[{}] new register_worker {:?}", tid, req);

  match server.register_worker(req, &tid).await {
    Ok(_) => HttpResponse::Ok().finish(),
    Err(e) => {
      error!("[{}] the web server got an error trying to register worker {} {}", tid, name, e);
      HttpResponse::Ok().finish()
    },
  }
}