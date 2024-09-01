use self::worker_config::WorkerConfig;
use crate::rpc::{CleanResponse, InvokeResponse, StatusResponse};
use crate::services::containers::{containermanager::ContainerManager, IsolationFactory};
use crate::services::influx_updater::InfluxUpdater;
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::invocation::InvokerFactory;
use crate::services::registration::RegistrationService;
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::services::status::status_service::StatusService;
use crate::services::worker_health::WorkerHealthService;
use crate::worker_api::iluvatar_worker::IluvatarWorkerImpl;
use anyhow::Result;
use iluvatar_library::influx::InfluxClient;
use iluvatar_library::types::{Compute, Isolation, ResourceTimings};
use iluvatar_library::{bail_error, characteristics_map::CharacteristicsMap};
use iluvatar_library::{characteristics_map::AgExponential, energy::energy_logging::EnergyLogger};
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb};
use iluvatar_library::characteristics_map::CharacteristicsPacket;
use iluvatar_library::{utils::execute_cmd_nonblocking};

use iluvatar_bpf_library::bpf::func_characs::*;
use std::mem::MaybeUninit;

use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver};
use std::path::Path;
use std::fs::File;
use serde::{Deserialize, Serialize};
use ipc_channel::ipc::{IpcOneShotServer, IpcSender, IpcReceiver};

pub mod worker_config;
pub use worker_config as config;
pub mod iluvatar_worker;
pub mod sim_worker;
pub mod worker_comm;

use crate::SCHED_CHANNELS;
use crate::services::containers::containerd::PidsPacket;
use std::sync::RwLock;
use std::thread;
use std::io::prelude::*;
use std::io::Read;
use tracing::{debug, info};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Channels {
    pub tx_chr: IpcSender<CharacteristicsPacket>,
    pub tx_pids: IpcSender<PidsPacket>,
}

fn launch_scheduler( worker_config: &WorkerConfig ){

    match &worker_config.finescheduling {

        Some(fconfig) => {

            let fconfig = fconfig.clone();

            if Path::new(&fconfig.binary).exists() {

                // create a oneshot IPC server 
                let (server, name) = IpcOneShotServer::new().unwrap();
                debug!(name=%name, status="server waiting", "fine_scheduler");

                let mut args = Vec::<String>::new(); 

                // default args for all the policies 
                args.push( "--server-name".to_string() );
                args.push( name.clone() );

                // construct args for different policies as needed 
                match (&fconfig.binary).as_str() {
                    "/tmp/iluvatar/bin/fs_policy_constrained" 
                        =>  {               
                            if let Some(&ref cores) = fconfig.cores.as_ref() {
                                for c in cores {
                                    args.push( "-c".to_string() );
                                    args.push( c.to_string() );
                                }
                            } 
                        }
                    _ => {} 
                }

                // launch the policy in a separate thread 
                let bname = fconfig.binary.clone();
                thread::spawn( move || {
                    let mut _child = execute_cmd_nonblocking(
                        &bname, 
                        &args, 
                        None, 
                        &String::from("none") 
                    ).unwrap();

                    let mut buffer = [0; 1024];
                    let mut log = File::create("/tmp/iluvatar/bin/sched.log").expect("failed to open log");
                    let mut elog = File::create("/tmp/iluvatar/bin/sched.elog").expect("failed to open log");

                    loop {
                        let read = _child.stdout.as_mut().unwrap().read(&mut buffer).unwrap_or(0);
                        if read > 0 {
                            log.write(&buffer[..read]);
                            log.flush();
                        }
                        match _child.try_wait() {
                            Ok(Some(status)) => {
                                let read = _child.stderr.as_mut().unwrap().read(&mut buffer).unwrap_or(0);
                                if read > 0 {
                                    elog.write(&buffer[..read]);
                                    elog.flush();
                                }
                            },
                            Ok(None) => {},
                            Err(e) => {},
                        }
                    }
                });

                // wait for the channel to establish with a timeout 
                let (_, channels): (_, Channels) = server.accept().unwrap();
                debug!(name=%name, status="channels established", "fine_scheduler");

                // save it in the global variable 
                unsafe {
                    SCHED_CHANNELS = Some(RwLock::new(channels));
                }
            }
        }
        None => (), // no binary config found 
    }; // end of config match 
}

pub async fn create_worker(worker_config: WorkerConfig, tid: &TransactionId) -> Result<IluvatarWorkerImpl> {

    // launch the fine grained scheduler  
    launch_scheduler( &worker_config );

    // create a multi-producer and single consumer async channel 
    let (tx, rx) = channel::<(u64,CharVal)>();

    // move the consumer end to a separate thread 
    // where data is pushed to the map 
    thread::spawn(move|| {
        // build the bpf characteristics map 
        let mut open_object = MaybeUninit::uninit();
        let mut skel = build_and_load( &mut open_object ).unwrap(); 
        let mut fcmap = skel
            .maps
            .func_characs;

        // Unbounded receiver waiting for all senders to complete.
        while let Ok((key, val)) = rx.recv() {
            update_map( &mut fcmap, key, &val );
        }
    });

    // charateristics map 
    // push the producer end of the channel to the characteristics map 
    let cmap = Arc::new( 
        CharacteristicsMap::new(
            AgExponential::new( 0.6 ),
            Some(tx) 
        )
    );

    let factory = IsolationFactory::new(worker_config.clone());
    let cpu = CpuResourceTracker::new(&worker_config.container_resources.cpu_resource, tid)
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make cpu resource tracker"))?;

    let isos = factory
        .get_isolation_services(tid, true)
        .await
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make lifecycle(s)"))?;
    let gpu_resource = GpuResourceTracker::boxed(
        &worker_config.container_resources.gpu_resource,
        &worker_config.container_resources,
        tid,
        &isos.get(&Isolation::DOCKER),
        &worker_config.status,
    )
    .await
    .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make GPU resource tracker"))?;

    let container_man = ContainerManager::boxed(
        worker_config.container_resources.clone(),
        isos.clone(),
        gpu_resource.clone(),
        tid,
    )
    .await
    .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make container manger"))?;

    let reg = RegistrationService::new(
        container_man.clone(),
        isos.clone(),
        worker_config.limits.clone(),
        cmap.clone(),
        worker_config.container_resources.clone(),
    );

    let energy = EnergyLogger::boxed(worker_config.energy.as_ref(), tid)
        .await
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make energy logger"))?;

    #[cfg(feature = "power_cap")]
    let energy_limit = EnergyLimiter::boxed(&worker_config.energy_cap, energy.clone())
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make worker energy limiter"))?;

    let invoker_fact = InvokerFactory::new(
        container_man.clone(),
        worker_config.limits.clone(),
        worker_config.clone(),
        worker_config.invocation.clone(),
        cmap.clone(),
        cpu,
        gpu_resource.clone(),
        worker_config.container_resources.gpu_resource.clone(),
        #[cfg(feature = "power_cap")]
        energy_limit.clone(),
    );
    let invoker = invoker_fact
        .get_invoker_service(tid)
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to get invoker service"))?;
    let health = WorkerHealthService::boxed(worker_config.clone(), invoker.clone(), reg.clone(), tid)
        .await
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make worker health service"))?;
    let status = StatusService::boxed(
        container_man.clone(),
        worker_config.name.clone(),
        tid,
        worker_config.status.clone(),
        invoker.clone(),
        gpu_resource.clone(),
    )
    .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make status service"))?;

    let influx_updater = match &worker_config.influx {
        Some(i_config) => {
            let client = InfluxClient::new(i_config.clone(), tid)
                .await
                .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make influx client"))?;
            InfluxUpdater::boxed(
                client,
                i_config.clone(),
                status.clone(),
                worker_config.name.clone(),
                tid,
            )
            .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make influx updater"))?
        }
        None => None,
    };

    Ok(IluvatarWorkerImpl::new(
        worker_config.clone(),
        container_man,
        invoker,
        status,
        health,
        energy,
        cmap,
        reg,
        influx_updater,
    ))
}

#[derive(Debug, PartialEq, Eq)]
pub enum HealthStatus {
    HEALTHY,
    UNHEALTHY,
}

#[tonic::async_trait]
pub trait WorkerAPI {
    async fn ping(&mut self, tid: TransactionId) -> Result<String>;
    async fn invoke(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<InvokeResponse>;
    async fn invoke_async(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<String>;
    async fn invoke_async_check(&mut self, cookie: &str, tid: TransactionId) -> Result<InvokeResponse>;
    async fn prewarm(
        &mut self,
        function_name: String,
        version: String,
        tid: TransactionId,
        compute: Compute,
    ) -> Result<String>;
    async fn register(
        &mut self,
        function_name: String,
        version: String,
        image_name: String,
        memory: MemSizeMb,
        cpus: u32,
        parallels: u32,
        tid: TransactionId,
        isolate: Isolation,
        compute: Compute,
        timings: Option<&ResourceTimings>,
    ) -> Result<String>;
    async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse>;
    async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus>;
    async fn clean(&mut self, tid: TransactionId) -> Result<CleanResponse>;
}
