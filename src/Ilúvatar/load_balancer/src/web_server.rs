use crate::load_balancer::LoadBalancer;
use actix_web::{HttpRequest, HttpResponse, get, post};
use actix_web::web::{Data, Json};
use iluvatar_lib::load_balancer_api::structs::Invoke;
use iluvatar_lib::transaction::gen_tid;
use log::*;

#[get("/ping")]
pub async fn ping(server: Data<LoadBalancer>, _req: HttpRequest) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new ping", tid);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/invoke")]
pub async fn invoke(server: Data<LoadBalancer>, req: Json<Invoke>) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new invoke {:?}", tid, req);
  // println!("{req:?}");
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/invoke_async")]
pub async fn invoke_async(server: Data<LoadBalancer>, req: Json<Invoke>) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new invoke_async {:?}", tid, req);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[get("/invoke_async_check")]
pub async fn invoke_async_check(server: Data<LoadBalancer>, req: HttpRequest) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new invoke_async_check {:?}", tid, req);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/prewarm")]
pub async fn prewarm(server: Data<LoadBalancer>, req: HttpRequest) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new prewar, {:?}", tid, req);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/register_function")]
pub async fn register_function(server: Data<LoadBalancer>, req: HttpRequest) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new register_function {:?}", tid, req);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}

#[post("/register_worker")]
pub async fn register_worker(server: Data<LoadBalancer>, req: HttpRequest) -> HttpResponse {
  let tid = gen_tid();
  info!("[{}] new register_worker {:?}", tid, req);
  server.index();
  let body = format!(
      "OK",
  );
  HttpResponse::Ok().body(body)
}