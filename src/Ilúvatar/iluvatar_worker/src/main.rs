use crate::utils::register_rpc_to_controller;
use anyhow::Result;
use clap::Parser;
use iluvatar_library::transaction::{TransactionId, STARTUP_TID};
use iluvatar_library::{bail_error, logging::start_tracing, nproc, utils::wait_for_exit_signal, utils::execute_cmd_nonblocking};
use iluvatar_worker_library::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_worker_library::worker_api::config::Configuration;
use iluvatar_worker_library::worker_api::create_worker;
use iluvatar_worker_library::{services::containers::IsolationFactory, worker_api::config::WorkerConfig};
use iluvatar_worker_library::worker_api::Channels;
use iluvatar_library::characteristics_map::CharacteristicsPacket;
use iluvatar_worker_library::services::containers::containerd::PidsPacket;
use iluvatar_worker_library::SCHED_CHANNELS;

use std::time::Duration;
use std::thread;
use std::io::Read;
use tokio::runtime::Runtime;
use tonic::transport::Server;
use tracing::{debug, info};
use utils::Args;
use ipc_channel::ipc::{IpcOneShotServer, IpcSender, IpcReceiver};

pub mod utils;

// static mut channels: Option<Channels> = None;

async fn run(server_config: WorkerConfig, tid: &TransactionId) -> Result<()> {
    debug!(tid=tid.as_str(), config=?server_config, "loaded configuration");
    match &server_config.finescheduling {
        Some(fconfig) => {
            let bname = fconfig.binary.clone();
            if std::path::Path::new(&bname).exists() {

                // create a oneshot server 
                let (server, name) = IpcOneShotServer::new().unwrap();
                debug!(tid=tid.as_str(), name=%name, status="server waiting", "ipc debugs");

                // launch a process 
                let args = vec![
                    "--server-name",
                    &name,
                    "--characteristics-file", 
                    &fconfig.characteristics_file,
                    "--pids-file",
                    &fconfig.pids_file,
                ];
                let mut _child = execute_cmd_nonblocking(&fconfig.binary, 
                    &args, None, &String::from("none")).unwrap();

                // wait for the channel to establish with a timeout 
                let (_, channels): (_, Channels) = server.accept().unwrap();
                debug!(tid=tid.as_str(), name=%name, status="channels established", "ipc debugs");

                channels.tx_chr.send( 
                    CharacteristicsPacket{ 
                        fqdn: "func_name".to_string(),
                        e2e: 2.5_f64,
                    }).unwrap();               

                unsafe {
                    SCHED_CHANNELS = Some(channels);
                }
            }
        }
        None => (),
    };

    let worker = match create_worker(server_config.clone(), tid).await {
        Ok(w) => w,
        Err(e) => bail_error!(tid=%tid, error=%e, "Error creating worker on startup"),
    };
    let addr = format!("{}:{}", server_config.address, server_config.port);

    register_rpc_to_controller(server_config.clone(), tid.clone());
    info!(tid=%tid, "Starting RPC server");
    debug!(config=?server_config, "Worker configuration");
    let _j = tokio::spawn(
        Server::builder()
            .timeout(Duration::from_secs(server_config.timeout_sec))
            .add_service(IluvatarWorkerServer::new(worker))
            .serve(addr.parse()?),
    );

    wait_for_exit_signal(tid).await?;
    Ok(())
}

async fn clean(server_config: WorkerConfig, tid: &TransactionId) -> Result<()> {
    debug!(tid=?tid, config=?server_config, "loaded configuration");

    let factory = IsolationFactory::new(server_config.clone());
    let lifecycles = factory.get_isolation_services(tid, false).await?;

    for (_, lifecycle) in lifecycles.iter() {
        lifecycle.clean_containers("default", lifecycle.clone(), tid).await?;
    }
    Ok(())
}

fn build_runtime(server_config: WorkerConfig, tid: &TransactionId) -> Result<Runtime> {
    match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(nproc(tid, false)? as usize)
        .enable_all()
        .event_interval(server_config.tokio_event_interval)
        .global_queue_interval(server_config.tokio_queue_interval)
        .build()
    {
        Ok(rt) => Ok(rt),
        Err(e) => {
            anyhow::bail!(format!("Tokio thread runtime for main failed to start because: {}", e));
        }
    }
}

fn main() -> Result<()> {
    iluvatar_library::utils::file::ensure_temp_dir()?;

    let tid: &TransactionId = &STARTUP_TID;
    let cli = Args::parse();

    match cli.command {
        Some(c) => match c {
            utils::Commands::Clean => {
                let overrides = vec![("networking.use_pool".to_string(), "false".to_string())];
                let server_config = Configuration::boxed(&cli.config.as_deref(), Some(overrides)).unwrap();
                let _guard = start_tracing(server_config.logging.clone(), &server_config.name, tid)?;
                let worker_rt = build_runtime(server_config.clone(), tid)?;
                worker_rt.block_on(clean(server_config, tid))?;
            }
        },
        None => {
            let server_config = Configuration::boxed(&cli.config.as_deref(), None).unwrap();
            let _guard = start_tracing(server_config.logging.clone(), &server_config.name, tid)?;
            let worker_rt = build_runtime(server_config.clone(), tid)?;
            worker_rt.block_on(run(server_config, tid))?;
        }
    }
    Ok(())
}
