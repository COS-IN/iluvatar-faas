use axum::{
    routing::{get, post},
    Extension, Router,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

use crate::handlers::{
    handle_async_invoke, handle_async_invoke_check, handle_invoke, handle_list_registered_funcs, handle_ping,
    handle_register,
};

use anyhow::Result;
use iluvatar_worker_library::worker_api::rpc::RPCWorkerAPI;
use serde::Deserialize;

#[derive(Clone)]
pub struct HttpServer {
    pub addr: SocketAddr,
    pub rpc_host: String,
    pub rpc_port: u16,
}

// register is a POST request, this struct defines what the request body should look like.
#[derive(Debug, Deserialize)]
pub struct RegisterParams {
    pub function_name: String,
    pub image: String,
    pub version: String,
    pub memory: u32,
    pub cpu: u32,
    pub isolate: String,
    pub compute: String,
}

impl HttpServer {
    /// Creates a new HttpServer instance.
    pub fn new(addr: SocketAddr, rpc_host: String, rpc_port: u16) -> Self {
        Self {
            addr,
            rpc_host,
            rpc_port,
        }
    }

    /// connects to the RPC server.
    pub async fn create_rpc_client(&self, tid: &str) -> Result<RPCWorkerAPI, String> {
        RPCWorkerAPI::new(&self.rpc_host, self.rpc_port, &tid.to_string())
            .await
            .map_err(|e| format!("Error initializing RPC client: {:?}", e))
    }

    /// Runs the HTTP server.
    /// use handlers defined in handers.rs crate.
    /// the http server is just calling the rpc api to handle the requests.
    pub async fn run(self) -> Result<()> {
        let app = Router::new()
            .route("/ping", get(handle_ping))
            .route("/register", post(handle_register))
            .route("/invoke/:func_name/:version", get(handle_invoke))
            .route("/async_invoke/:func_name/:version", get(handle_async_invoke))
            .route("/invoke_async_check/:cookie", get(handle_async_invoke_check))
            .route("/list_registered_func", get(handle_list_registered_funcs))
            // add extension to the app so we can access the server from the handlers(to get rpc_host and port).
            .layer(Extension(self.clone()));

        info!(address = %self.addr, "Starting HTTP listener");
        let listener = TcpListener::bind(self.addr).await?;
        axum::serve(listener, app.into_make_service()).await?;
        Ok(())
    }
}
