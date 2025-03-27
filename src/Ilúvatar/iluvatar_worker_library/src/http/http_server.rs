use crate::http::handlers::{
    handle_async_invoke, handle_async_invoke_check, handle_invoke, handle_ping, handle_register,
};
use crate::worker_api::iluvatar_worker::IluvatarWorkerImpl;
use anyhow::Result;
use axum::{
    routing::{get, put},
    Extension, Router,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Clone)]
pub struct HttpServer {
    pub addr: SocketAddr,
    pub worker: Arc<IluvatarWorkerImpl>,
}

// register is a PUT request, this struct defines what the request body should look like.
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
    pub fn new(addr: SocketAddr, worker: Arc<IluvatarWorkerImpl>) -> Self {
        Self { addr, worker }
    }

    pub async fn run(self) -> Result<()> {
        info!("Starting HTTP server on: {}", self.addr);
        let app = Router::new()
            .route("/ping", get(handle_ping))
            .route("/register", put(handle_register))
            .route("/invoke/:func_name/:version", get(handle_invoke))
            .route("/async_invoke/:func_name/:version", get(handle_async_invoke))
            .route("/async_invoke_check/:cookie", get(handle_async_invoke_check))
            .layer(Extension(self.clone()));
        let listener = TcpListener::bind(self.addr).await?;
        axum::serve(listener, app.into_make_service()).await?;
        Ok(())
    }
}
