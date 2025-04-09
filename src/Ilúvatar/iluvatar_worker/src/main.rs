use anyhow::Result;
use clap::Parser;
use iluvatar_controller_library::server::controller_comm::ControllerAPIFactory;
use iluvatar_library::char_map::worker_char_map;
use iluvatar_library::tokio_utils::build_tokio_runtime;
use iluvatar_library::transaction::{TransactionId, STARTUP_TID};
use iluvatar_library::{bail_error, logging::start_tracing, utils::wait_for_exit_signal};
use iluvatar_rpc::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_rpc::rpc::RegisterWorkerRequest;
use iluvatar_worker_library::http::create_http_server;
use iluvatar_worker_library::worker_api::config::Configuration;
use iluvatar_worker_library::worker_api::create_worker;
use iluvatar_worker_library::{services::containers::IsolationFactory, worker_api::config::WorkerConfig};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use tracing::{debug, error, info};
use utils::Args;

pub mod utils;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

async fn run(server_config: WorkerConfig, tid: &TransactionId) -> Result<()> {
    debug!(tid=tid.as_str(), config=?server_config, "loaded configuration");

    let worker = match create_worker(server_config.clone(), tid).await {
        Ok(w) => w,
        Err(e) => bail_error!(tid=tid, error=%e, "Error creating worker on startup"),
    };
    let worker = Arc::new(worker);
    let compute = worker.supported_compute().bits();
    let isolation = worker.supported_isolation().bits();

    let addr = format!("{}:{}", server_config.address, server_config.port);
    info!(tid=tid, address=%addr, "Starting RPC server");
    debug!(tid=tid, config=?server_config, "Worker configuration");
    let _j = tokio::spawn(
        Server::builder()
            .timeout(Duration::from_secs(server_config.timeout_sec))
            .add_service(IluvatarWorkerServer::from_arc(worker.clone()))
            .serve(addr.parse()?),
    );

    match server_config.http_server.as_ref() {
        Some(c) if c.enabled => {
            info!(tid = tid, "HTTP server enabled, starting the server");
            let http_server = match create_http_server(&c.address, c.port, worker.clone()).await {
                Ok(s) => s,
                Err(e) => bail_error!(tid = tid, error = %e, "Error creating HTTP server on startup"),
            };
            tokio::spawn(async move {
                if let Err(e) = http_server.run().await {
                    error!("HTTP server error: {}", e);
                }
            });
        },
        _ => {
            info!(
                tid = tid,
                "Skipping HTTP server; either no config provided or server disabled"
            );
        },
    }

    match &server_config.load_balancer_host {
        Some(host) if !host.is_empty() => {
            match ControllerAPIFactory::boxed()
                .get_controller_api(
                    host,
                    server_config
                        .load_balancer_port
                        .ok_or_else(|| anyhow::anyhow!("Load balancer host provided, but not port"))?,
                    tid,
                )
                .await
            {
                Ok(c) => {
                    c.register_worker(RegisterWorkerRequest {
                        name: server_config.name.clone(),
                        host: server_config.address.clone(),
                        port: server_config.port.into(),
                        memory: server_config.container_resources.memory_mb,
                        cpus: server_config.container_resources.cpu_resource.count,
                        compute,
                        isolation,
                        gpus: server_config
                            .container_resources
                            .gpu_resource
                            .as_ref()
                            .map_or(0, |g| g.count),
                    })
                    .await?
                },
                Err(e) => {
                    bail_error!(tid=tid, error=%e, controller_host=host, port=server_config.load_balancer_port, "Failed to connect to load balancer")
                },
            }
        },
        _ => info!(tid = tid, "Skipping controller registration"),
    };

    wait_for_exit_signal(tid).await?;
    Ok(())
}

async fn clean(server_config: WorkerConfig, tid: &TransactionId) -> Result<()> {
    debug!(tid=?tid, config=?server_config, "loaded configuration");

    let factory = IsolationFactory::new(server_config.clone(), worker_char_map());
    let lifecycles = factory.get_isolation_services(tid, false).await?;

    for (_, lifecycle) in lifecycles.iter() {
        lifecycle.clean_containers("default", lifecycle.clone(), tid).await?;
    }
    Ok(())
}

fn main() -> Result<()> {
    iluvatar_library::utils::file::ensure_temp_dir()?;
    let tid: &TransactionId = &STARTUP_TID;
    let cli = Args::parse();

    match cli.command {
        Some(c) => match c {
            utils::Commands::Clean => {
                let overrides = vec![("networking.use_pool".to_string(), "false".to_string())];
                let server_config = Configuration::boxed(cli.config.as_deref(), Some(overrides))?;
                let _guard = start_tracing(&server_config.logging, tid)?;
                let worker_rt = build_tokio_runtime(
                    &Some(server_config.tokio_event_interval),
                    &Some(server_config.tokio_queue_interval),
                    &None,
                    tid,
                )?;
                worker_rt.block_on(clean(server_config, tid))?;
            },
        },
        None => {
            let server_config = Configuration::boxed(cli.config.as_deref(), None)?;
            let _guard = start_tracing(&server_config.logging, tid)?;
            let worker_rt = build_tokio_runtime(
                &Some(server_config.tokio_event_interval),
                &Some(server_config.tokio_queue_interval),
                &None,
                tid,
            )?;
            worker_rt.block_on(run(server_config, tid))?;
        },
    }
    Ok(())
}
