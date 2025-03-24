use axum::{
    extract::{Extension, Json, Path, Query},
    response::IntoResponse,
};
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::types::{Compute, ContainerServer, Isolation};
use iluvatar_worker_library::worker_api::WorkerAPI;
use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

use crate::http_server::{HttpServer, RegisterParams};

/// Handler for the /ping route.
/// returns 'pong', IntoResponse just converts the string to a response.
pub async fn handle_ping(Extension(server): Extension<HttpServer>) -> impl IntoResponse {
    let tid = gen_tid();
    let mut api = match server.create_rpc_client(&tid).await {
        Ok(api) => api,
        Err(e) => return e,
    };
    let ret = api.ping(tid).await.unwrap();
    debug!("Ping RPC returned: {}", ret);
    ret
}

/// Handler for the /register route.
/// This handler is a POST request
/// Extention is a way to pass data/config to the handler. we are passing the HttpServer instance.
/// Json is a way to parse the request body into a struct.
pub async fn handle_register(
    Extension(server): Extension<HttpServer>,
    Json(params): Json<RegisterParams>,
) -> impl IntoResponse {
    // some validation before we make a request to the rpc server.

    // Validate isolation.
    let isolation_str = params.isolate.to_lowercase();
    let isolation = match isolation_str.as_str() {
        "docker" => vec![Isolation::DOCKER],
        "containerd" => vec![Isolation::CONTAINERD],
        _ => return format!("Error: isolation must be either 'docker' or 'containerd'"),
    };

    // Validate compute.
    let compute_str = params.compute.to_uppercase();
    let compute = match compute_str.as_str() {
        "CPU" => vec![Compute::CPU],
        "GPU" => vec![Compute::GPU],
        _ => return format!("Error: compute must be either 'CPU' or 'GPU'"),
    };

    let tid = gen_tid();
    let image = params.image;
    let mem_size_mb: i64 = params.memory as i64;
    let server_type = ContainerServer::HTTP;

    let mut api = match server.create_rpc_client(&tid).await {
        Ok(api) => api,
        Err(e) => return e,
    };

    let ret = match api
        .register(
            params.function_name,
            params.version,
            image,
            mem_size_mb,
            params.cpu,
            1,
            tid,
            isolation.into(),
            compute.into(),
            server_type,
            None,
        )
        .await
    {
        Ok(result) => result,
        Err(e) => return format!("Error during RPC register call: {:?}", e),
    };

    debug!("Register RPC returned: {}", ret);
    ret
}

/// Handler for the /invoke/:func_name/:version route.
/// a simple GET request, example: /invoke/my_func/1?arg1=1&arg2=2
pub async fn handle_invoke(
    Extension(server): Extension<HttpServer>,
    Path((func_name, version)): Path<(String, String)>,
    Query(query_params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let arguments = match serde_json::to_string(&query_params) {
        Ok(json_str) => json_str,
        Err(e) => return format!("Error converting query parameters to JSON: {:?}", e),
    };

    let tid = gen_tid();
    let mut api = match server.create_rpc_client(&tid).await {
        Ok(api) => api,
        Err(e) => return e,
    };

    let ret = match api.invoke(func_name, version, arguments, tid).await {
        Ok(result) => result,
        Err(e) => return format!("Error during RPC invoke call: {:?}", e),
    };

    debug!("RPC invoke returned: {:?}", ret);
    serde_json::to_string(&ret).unwrap()
}

/// Handler for the /async_invoke/:func_name/:version route.
/// returns cookie to track result of async call.
pub async fn handle_async_invoke(
    Extension(server): Extension<HttpServer>,
    Path((func_name, version)): Path<(String, String)>,
    Query(query_params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let arguments = match serde_json::to_string(&query_params) {
        Ok(json_str) => json_str,
        Err(e) => return format!("Error converting query parameters to JSON: {:?}", e),
    };

    let tid = gen_tid();
    let mut api = match server.create_rpc_client(&tid).await {
        Ok(api) => api,
        Err(e) => return e,
    };

    let ret = match api.invoke_async(func_name, version, arguments, tid).await {
        Ok(cookie) => cookie,
        Err(e) => return format!("Error during RPC invoke_async call: {:?}", e),
    };

    debug!("RPC async_invoke returned: {}", ret);
    let json_response = json!({ "cookie": ret });
    serde_json::to_string(&json_response).unwrap()
    // ret
}

/// Handler for the /invoke_async_check/:cookie route.
/// example /invoke_async_check/<cookie_value>
pub async fn handle_async_invoke_check(
    Extension(server): Extension<HttpServer>,
    Path(cookie): Path<String>,
) -> impl IntoResponse {
    let tid = gen_tid();
    let mut api = match server.create_rpc_client(&tid).await {
        Ok(api) => api,
        Err(e) => return e,
    };

    let ret = match api.invoke_async_check(&cookie, gen_tid()).await {
        Ok(result) => result,
        Err(e) => return format!("Error during RPC invoke_async_check call: {:?}", e),
    };

    serde_json::to_string(&ret).unwrap()
}

/// Handler for the /list_registered_func route.
/// returns a list of registered functions.
pub async fn handle_list_registered_funcs(Extension(server): Extension<HttpServer>) -> impl IntoResponse {
    let tid = gen_tid();
    let mut api = match server.create_rpc_client(&tid).await {
        Ok(api) => api,
        Err(e) => return e,
    };

    let ret = match api.list_registered_funcs(gen_tid()).await {
        Ok(result) => result,
        Err(e) => return format!("Error during RPC call: {:?}", e),
    };

    let functions = ret
        .functions
        .into_iter()
        .map(|func| {
            json!({
                "function_name": func.function_name,
                "function_version": func.function_version,
                "image_name": func.image_name,
            })
        })
        .collect::<Vec<_>>();

    let output = json!({ "functions": functions });
    serde_json::to_string(&output).unwrap()
}
