use std::time::Duration;

// use actix_web::{web::Data, App, HttpServer};
use clap::{command, Parser};
use iluvatar_controller_library::server::{config::Configuration, controller::Controller};
use iluvatar_library::logging::start_tracing;
use iluvatar_library::transaction::{TransactionId, LOAD_BALANCER_TID};
use iluvatar_library::utils::wait_for_exit_signal;
use iluvatar_rpc::rpc::iluvatar_controller_server::IluvatarControllerServer;
use tonic::transport::Server;
use tracing::{debug, info};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    /// Path to a configuration file to use
    pub config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    iluvatar_library::utils::file::ensure_temp_dir()?;
    let tid: &TransactionId = &LOAD_BALANCER_TID;
    let args = Args::parse();

    let config = Configuration::boxed(&args.config)?;
    let _guard = start_tracing(&config.logging, tid)?;

    let controller = Controller::new(config.clone(), tid).await?;

    info!(tid = tid, "Controller started!");
    debug!(config=?config, "Controller configuration");
    let addr = std::net::SocketAddr::new(config.address.clone().parse()?, config.port);
    let _j = tokio::spawn(
        Server::builder()
            .timeout(Duration::from_secs(config.timeout_sec))
            .add_service(IluvatarControllerServer::new(controller))
            .serve(addr),
    );

    wait_for_exit_signal(tid).await?;
    Ok(())
}
