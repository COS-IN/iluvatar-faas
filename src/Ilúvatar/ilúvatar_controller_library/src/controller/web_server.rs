use crate::controller::controller::Controller;
use crate::controller::controller_errors::MissingAsyncCookieError;
use crate::controller::controller_structs::json::{AsyncInvokeResult, ControllerInvokeResult};
use crate::controller::structs::json::{Invoke, InvokeAsyncLookup, Prewarm, RegisterFunction};
use actix_web::web::{Data, Json};
use actix_web::{get, post, HttpRequest, HttpResponse};
use iluvatar_library::api_register::RegisterWorker;
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::utils::calculate_fqdn;
use tracing::{error, info};

#[get("/ping")]
pub async fn ping(_server: Data<Controller>, _req: HttpRequest) -> HttpResponse {
    let tid = gen_tid();
    info!(tid=%tid, "New ping");
    HttpResponse::Ok().body("PONG\n")
}
#[post("/invoke")]
pub async fn invoke_api(server: Data<Controller>, req: Json<Invoke>) -> HttpResponse {
    invoke(server, req).await
}
pub async fn invoke(server: Data<Controller>, req: Json<Invoke>) -> HttpResponse {
    let tid = gen_tid();
    let req = req.into_inner();
    info!(tid=%tid, request=?req, "new invoke");

    let (result, duration) = match server.invoke(req, &tid).await {
        Ok(d) => d,
        Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
    };
    let ret = ControllerInvokeResult {
        worker_duration_us: duration.as_micros(),
        success: result.success,
        result,
        tid,
    };
    HttpResponse::Ok().json(ret)
}

#[post("/invoke_async")]
pub async fn invoke_async_api(server: Data<Controller>, req: Json<Invoke>) -> HttpResponse {
    invoke_async(server, req).await
}
pub async fn invoke_async(server: Data<Controller>, req: Json<Invoke>) -> HttpResponse {
    let tid = gen_tid();
    let req = req.into_inner();
    info!(tid=%tid, request=?req, "New invoke_async");
    match server.invoke_async(req, &tid).await {
        Ok((cookie, duration)) => HttpResponse::Created().json(AsyncInvokeResult {
            cookie,
            worker_duration_us: duration.as_micros(),
            tid,
        }),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("/invoke_async_check")]
pub async fn invoke_async_check_api(server: Data<Controller>, req: Json<InvokeAsyncLookup>) -> HttpResponse {
    invoke_async_check(server, req).await
}
pub async fn invoke_async_check(server: Data<Controller>, req: Json<InvokeAsyncLookup>) -> HttpResponse {
    let tid = gen_tid();
    let req = req.into_inner();
    info!(tid=%tid, request=?req, "New invoke_async_check");
    match server.check_async_invocation(req.lookup_cookie, &tid).await {
        Ok(some) => {
            if let Some(json) = some {
                HttpResponse::Ok().json(json)
            } else {
                HttpResponse::Accepted().finish()
            }
        }
        Err(cause) => {
            if let Some(_core_err) = cause.downcast_ref::<MissingAsyncCookieError>() {
                HttpResponse::NotFound().body("Unable to find async inovcation matching cookie")
            } else {
                HttpResponse::InternalServerError().body(cause.to_string())
            }
        }
    }
}

#[post("/prewarm")]
pub async fn prewarm_api(server: Data<Controller>, req: Json<Prewarm>) -> HttpResponse {
    prewarm(server, req).await
}
pub async fn prewarm(server: Data<Controller>, req: Json<Prewarm>) -> HttpResponse {
    let tid = gen_tid();
    let req = req.into_inner();
    info!(tid=%tid, request=?req, "New prewarm");

    match server.prewarm(req, &tid).await {
        Ok(_) => HttpResponse::Accepted().body("OK"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[post("/register_function")]
pub async fn register_function_api(server: Data<Controller>, req: Json<RegisterFunction>) -> HttpResponse {
    register_function(server, req).await
}
pub async fn register_function(server: Data<Controller>, req: Json<RegisterFunction>) -> HttpResponse {
    let tid = gen_tid();
    let req = req.into_inner();
    let fqdn = calculate_fqdn(&req.function_name, &req.function_version);
    info!(tid=%tid, request=?req, "New register_function");

    match server.register_function(req, &tid).await {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => {
            error!(tid=%tid, fqdn=%fqdn, error=%e, "The web server got an error trying to register function");
            HttpResponse::InternalServerError().body(e.to_string())
        }
    }
}

#[post("/register_worker")]
pub async fn register_worker_api(server: Data<Controller>, req: Json<RegisterWorker>) -> HttpResponse {
    register_worker(server, req).await
}
pub async fn register_worker(server: Data<Controller>, req: Json<RegisterWorker>) -> HttpResponse {
    let tid = gen_tid();
    let req = req.into_inner();
    let name = req.name.clone();
    info!(tid=%tid, request=?req, "New register_worker");

    match server.register_worker(req, &tid).await {
        Ok(_) => HttpResponse::Accepted().finish(),
        Err(e) => {
            error!(tid=%tid, worker=%name, error=%e, "The web server got an error trying to register worker");
            HttpResponse::InternalServerError().body(e.to_string())
        }
    }
}
