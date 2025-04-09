use axum::{
    extract::{Extension, Json, Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::types::{Compute, ContainerServer, Isolation};
use iluvatar_rpc::rpc::iluvatar_worker_server::IluvatarWorker;
use iluvatar_rpc::rpc::{
    InvokeAsyncLookupRequest, InvokeAsyncRequest, InvokeRequest, LanguageRuntime, PingRequest, RegisterRequest,
};
use serde_json::{json, to_string};
use std::collections::HashMap;
use tonic::Request;
use tracing::debug;

use crate::http::http_server::{HttpServer, RegisterParams};

/// Error types for the HTTP handlers.
/// Bad request errors are returned when the request is invalid.
/// Internal errors are returned when the worker func call fails.
#[derive(Debug)]
pub enum AppError {
    BadRequest(String),
    InternalError(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        let body = axum::Json(json!({ "error": message }));
        (status, body).into_response()
    }
}

/// Handler for the /ping route.
/// returns 'pong', IntoResponse just converts the string to a response.
/// Returns a 500 Internal Server Error if the worker func call fails.
pub async fn handle_ping(Extension(server): Extension<HttpServer>) -> Result<impl IntoResponse, AppError> {
    let tid = gen_tid();
    let request = Request::new(PingRequest {
        message: "Ping".to_string(),
        transaction_id: tid,
    });
    let ret = server
        .worker
        .ping(request)
        .await
        .map_err(|e| AppError::InternalError(format!("Ping RPC failed: {}", e.message())))?;
    let response = ret.into_inner();
    debug!("Ping RPC returned: {}", response.message);
    Ok(response.message)
}

/// Handler for the /register route.
/// This handler is a PUT request
/// - Validation errors return 400 Bad Request.
/// - worker func call failures return 500 Internal Server Error.
/// - If the RPC response is received but indicates failure,
///   a 400 Bad Request is returned.
pub async fn handle_register(
    Extension(server): Extension<HttpServer>,
    Json(params): Json<RegisterParams>,
) -> Result<impl IntoResponse, AppError> {
    // Validate isolation.
    let isolation_str = params.isolate.to_lowercase();
    let isolation = match isolation_str.as_str() {
        "docker" => Isolation::DOCKER,
        "containerd" => Isolation::CONTAINERD,
        _ => {
            return Err(AppError::BadRequest(
                "isolation must be either 'docker' or 'containerd'".to_string(),
            ))
        },
    };

    // Validate compute.
    let compute_str = params.compute.to_uppercase();
    let compute = match compute_str.as_str() {
        "CPU" => Compute::CPU,
        "GPU" => Compute::GPU,
        _ => {
            return Err(AppError::BadRequest(
                "compute must be either 'CPU' or 'GPU'".to_string(),
            ))
        },
    };

    let tid = gen_tid();
    let mem_size_mb: i64 = params.memory as i64;
    let server_type = ContainerServer::HTTP as u32;
    let register_request = RegisterRequest {
        function_name: params.function_name,
        function_version: params.version,
        memory: mem_size_mb,
        cpus: params.cpu,
        image_name: params.image,
        parallel_invokes: 1,
        transaction_id: tid,
        language: LanguageRuntime::Nolang.into(),
        compute: compute.bits(),
        isolate: isolation.bits(),
        container_server: server_type,
        resource_timings_json: "{}".to_string(),
        system_function: false,
    };
    let request = Request::new(register_request);

    let ret = server
        .worker
        .register(request)
        .await
        .map_err(|e| AppError::InternalError(format!("Register RPC failed: {}", e.message())))?;
    let response = ret.into_inner();

    if response.success {
        let json_response = "{\"Ok\": \"function registered\"}".to_string();
        Ok(json_response)
    } else {
        // if we get response, but it indicates failure, return a 400 Bad Request.
        Err(AppError::BadRequest(format!(
            "Register request failed: {:?}",
            response.error
        )))
    }
}

/// Handler for the /invoke/:func_name/:version route.
/// a simple GET request, example: /invoke/my_func/1?arg1=1&arg2=2
pub async fn handle_invoke(
    Extension(server): Extension<HttpServer>,
    Path((func_name, version)): Path<(String, String)>,
    Query(query_params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let arguments = to_string(&query_params)
        .map_err(|e| AppError::BadRequest(format!("Error converting query parameters to JSON: {:?}", e)))?;

    let tid = gen_tid();

    let invoke_req = InvokeRequest {
        function_name: func_name,
        function_version: version,
        json_args: arguments,
        transaction_id: tid,
    };

    let request = Request::new(invoke_req);

    let ret = server
        .worker
        .invoke(request)
        .await
        .map_err(|e| AppError::InternalError(format!("Invoke RPC failed: {:?}", e)))?;

    let response = ret.into_inner();

    if response.success {
        Ok(response.json_result)
    } else {
        Err(AppError::BadRequest(format!(
            "Invoke request failed: {}",
            response.json_result
        )))
    }
}

/// Handler for the /async_invoke/:func_name/:version route.
/// returns cookie to track result of async call.
pub async fn handle_async_invoke(
    Extension(server): Extension<HttpServer>,
    Path((func_name, version)): Path<(String, String)>,
    Query(query_params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let arguments = to_string(&query_params)
        .map_err(|e| AppError::BadRequest(format!("Error converting query parameters to JSON: {:?}", e)))?;

    let tid = gen_tid();

    let async_invoke_req = InvokeAsyncRequest {
        function_name: func_name,
        function_version: version,
        json_args: arguments,
        transaction_id: tid,
    };

    let request = Request::new(async_invoke_req);

    let ret = server
        .worker
        .invoke_async(request)
        .await
        .map_err(|e| AppError::InternalError(format!("Async invoke RPC failed: {:?}", e)))?;
    let response = ret.into_inner();

    if response.success {
        let json_response = format!("{{\"cookie\": \"{}\"}}", response.lookup_cookie).to_string();
        Ok(json_response)
    } else {
        Err(AppError::BadRequest(
            "Async invoke failed: function not registered or invocation error".to_string(),
        ))
    }
}

/// Handler for the /invoke_async_check/:cookie route.
/// example /invoke_async_check/<cookie_value>
pub async fn handle_async_invoke_check(
    Extension(server): Extension<HttpServer>,
    Path(cookie): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let tid = gen_tid();

    let lookup_req = InvokeAsyncLookupRequest {
        lookup_cookie: cookie,
        transaction_id: tid,
    };

    let request = Request::new(lookup_req);

    let ret = server
        .worker
        .invoke_async_check(request)
        .await
        .map_err(|e| AppError::InternalError(format!("Async invoke check RPC failed: {:?}", e)))?;

    let response = ret.into_inner();

    if response.success {
        Ok(response.json_result)
    } else {
        Err(AppError::BadRequest("Async invoke check failed".to_string()))
    }
}
