use clap::{command, Parser, Subcommand};
use iluvatar_library::{
    api_register::register_worker,
    nproc,
    transaction::TransactionId,
    types::{Compute, Isolation},
};
use iluvatar_worker_library::worker_api::worker_config::Configuration;
use std::sync::Arc;
use tracing::{debug, error, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    /// Sets a custom config file
    pub config: Option<String>,
    /// Use direct mode for writing logs, rather than async version. Helpful for debugging
    #[arg(short, long)]
    pub direct_logs: Option<bool>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the worker
    Clean,
}

pub fn register_rpc_to_controller(server_config: Arc<Configuration>, tid: TransactionId) {
    let _ = tokio::spawn(async move {
        debug!(tid=%tid, "Controller registration thread started");

        // allow RPC server time to start up
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let cores = match nproc(&tid, false) {
            Ok(c) => c,
            Err(e) => {
                error!(tid=%tid, error=%e, "Failed to get CPU core count in `register_rpc_to_controller`");
                0
            }
        };

        let result = register_worker(
            &server_config.name,
            iluvatar_library::types::CommunicationMethod::RPC,
            &server_config.address,
            server_config.port,
            server_config.container_resources.memory_mb,
            cores,
            &server_config.load_balancer_url,
            &tid,
            Compute::CPU,
            Isolation::CONTAINERD,
            0,
        )
        .await;

        match result {
            Ok(_) => info!(tid=%tid, "Worker successfully registered with controller"),
            Err(e) => error!(tid=%tid, error=%e, "Worker registration failed"),
        }
    });
}
