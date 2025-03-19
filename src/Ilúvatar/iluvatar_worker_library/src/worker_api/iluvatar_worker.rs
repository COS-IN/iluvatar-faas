use crate::services::containers::containermanager::{ContainerManager, ContainerMgrStat, CTR_MGR_WORKER_TID};
use crate::services::containers::ContainerIsolationCollection;
use crate::services::influx_updater::InfluxUpdater;
use crate::services::invocation::dispatching::queueing_dispatcher::DISPATCHER_INVOKER_LOG_TID;
use crate::services::invocation::{Invoker, InvokerLoad};
use crate::services::resources::cpu::{CpuMonitor, CpuUtil, CPU_MON_TID};
use crate::services::resources::gpu::GpuResourceTracker;
use crate::services::{registration::RegistrationService, worker_health::WorkerHealthService};
use crate::worker_api::config::WorkerConfig;
use iluvatar_library::char_map::{Chars, IatTracker, WorkerCharMap};
use iluvatar_library::ring_buff::RingBuffer;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_library::{energy::energy_logging::EnergyLogger, utils::calculate_fqdn};
use iluvatar_rpc::rpc::iluvatar_worker_server::IluvatarWorker;
use iluvatar_rpc::rpc::{
    CleanRequest, HealthRequest, InvokeAsyncLookupRequest, InvokeAsyncRequest, InvokeRequest, PingRequest,
    PrewarmRequest, RegisterRequest, RegisteredFunction, StatusRequest,
};
use iluvatar_rpc::rpc::{
    CleanResponse, HealthResponse, InvokeAsyncResponse, InvokeResponse, ListFunctionRequest, ListFunctionResponse,
    PingResponse, PrewarmResponse, RegisterResponse, StatusResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

#[allow(unused)]
/// Public members are _only_ for use in testing
pub struct IluvatarWorkerImpl {
    pub container_manager: Arc<ContainerManager>,
    pub config: WorkerConfig,
    pub invoker: Arc<dyn Invoker>,
    health: Arc<WorkerHealthService>,
    energy: Arc<EnergyLogger>,
    pub cmap: WorkerCharMap,
    pub reg: Arc<RegistrationService>,
    updater: Option<Arc<InfluxUpdater>>,
    pub gpu: Option<Arc<GpuResourceTracker>>,
    isolations: ContainerIsolationCollection,
    iats: IatTracker,
    cpu_mon: CpuMonitor,
    ring_buff: Arc<RingBuffer>,
}

impl IluvatarWorkerImpl {
    pub fn new(
        config: WorkerConfig,
        container_manager: Arc<ContainerManager>,
        invoker: Arc<dyn Invoker>,
        health: Arc<WorkerHealthService>,
        energy: Arc<EnergyLogger>,
        cmap: WorkerCharMap,
        reg: Arc<RegistrationService>,
        updater: Option<Arc<InfluxUpdater>>,
        gpu: Option<Arc<GpuResourceTracker>>,
        isolations: ContainerIsolationCollection,
        cpu_mon: CpuMonitor,
        ring_buff: Arc<RingBuffer>,
    ) -> IluvatarWorkerImpl {
        IluvatarWorkerImpl {
            container_manager,
            config,
            invoker,
            health,
            energy,
            cmap,
            reg,
            updater,
            gpu,
            isolations,
            iats: IatTracker::new(),
            cpu_mon,
            ring_buff,
        }
    }

    pub fn supported_compute(&self) -> Compute {
        Compute::CPU
            | match self.gpu {
                Some(_) => Compute::GPU,
                _ => Compute::empty(),
            }
    }
    pub fn supported_isolation(&self) -> Isolation {
        self.isolations
            .iter()
            .map(|(iso, _svc)| iso)
            .fold(Isolation::empty(), |acc, item| acc | *item)
    }
}

#[tonic::async_trait]
impl IluvatarWorker for IluvatarWorkerImpl {
    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let reply = PingResponse { message: "Pong".into() };
        let request = request.into_inner();
        info!(message = request.message, tid = request.transaction_id, "in ping");
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn invoke(&self, request: Request<InvokeRequest>) -> Result<Response<InvokeResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, "Handling invocation request");
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        let reg = match self.reg.get_registration(&fqdn) {
            Some(r) => r,
            None => return Ok(Response::new(InvokeResponse::error("Function was not registered"))),
        };
        if let Some(iat) = self.iats.track(&fqdn) {
            self.cmap.update(&fqdn, Chars::IAT, iat);
        }
        debug!(tid=%request.transaction_id, "Sending invocation to invoker");
        let resp = self
            .invoker
            .sync_invocation(reg, request.json_args, request.transaction_id)
            .await;

        match resp {
            Ok(result_ptr) => {
                let result = result_ptr.lock();
                let reply = InvokeResponse {
                    json_result: result.result_json.clone(),
                    success: true,
                    duration_us: result.duration.as_micros() as u64,
                    compute: result.compute.bits(),
                    container_state: result.container_state.into(),
                };
                Ok(Response::new(reply))
            },
            Err(e) => {
                error!("Invoke failed with error: {}", e);
                Ok(Response::new(InvokeResponse::error(&e.to_string())))
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn invoke_async(
        &self,
        request: Request<InvokeAsyncRequest>,
    ) -> Result<Response<InvokeAsyncResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, function_name=%request.function_name, function_version=%request.function_version, "Handling async invocation request");
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        let reg = match self.reg.get_registration(&fqdn) {
            Some(r) => r,
            None => {
                return Ok(Response::new(InvokeAsyncResponse {
                    lookup_cookie: "".to_string(),
                    success: false,
                }));
            },
        };
        if let Some(iat) = self.iats.track(&fqdn) {
            self.cmap.update(&fqdn, Chars::IAT, iat);
        }
        let resp = self
            .invoker
            .async_invocation(reg, request.json_args, request.transaction_id);

        match resp {
            Ok(cookie) => {
                let reply = InvokeAsyncResponse {
                    lookup_cookie: cookie,
                    success: true,
                };
                Ok(Response::new(reply))
            },
            Err(e) => {
                error!("Failed to launch an async invocation with error '{}'", e);
                Ok(Response::new(InvokeAsyncResponse {
                    lookup_cookie: "".to_string(),
                    success: false,
                }))
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn invoke_async_check(
        &self,
        request: Request<InvokeAsyncLookupRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, "Handling invoke async check");
        let resp = self
            .invoker
            .invoke_async_check(&request.lookup_cookie, &request.transaction_id);
        match resp {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => {
                error!(tid=%request.transaction_id, error=%e, "Failed to check async invocation status");
                Ok(Response::new(InvokeResponse::error(&e.to_string())))
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn prewarm(&self, request: Request<PrewarmRequest>) -> Result<Response<PrewarmResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, function_name=%request.function_name, function_version=%request.function_version, "Handling prewarm request");

        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        let reg = match self.reg.get_registration(&fqdn) {
            Some(r) => r,
            None => {
                let resp = PrewarmResponse {
                    success: false,
                    message: "{ \"Error\": \"Function was not registered\" }".into(),
                };
                return Ok(Response::new(resp));
            },
        };
        let compute: Compute = request.compute.into();
        if !reg.supported_compute.intersects(compute) {
            let resp = PrewarmResponse {
                success: false,
                message: "{ \"Error\": \"Function was not registered with the specified compute\" }".to_string(),
            };
            return Ok(Response::new(resp));
        }
        let container_id = self
            .container_manager
            .prewarm(&reg, &request.transaction_id, compute)
            .await;

        match container_id {
            Ok(_) => {
                let resp = PrewarmResponse {
                    success: true,
                    message: "".to_string(),
                };
                Ok(Response::new(resp))
            },
            Err(e) => {
                error!(tid=%request.transaction_id, error=%e, "Container prewarm failed");
                let resp = PrewarmResponse {
                    success: false,
                    message: format!("{{ \"Error\": \"{}\" }}", e),
                };
                Ok(Response::new(resp))
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<RegisterResponse>, Status> {
        let request = request.into_inner();
        let tid: TransactionId = request.transaction_id.clone();
        info!(tid=%request.transaction_id, function_name=%request.function_name, function_version=%request.function_version, image=%request.image_name, "Handling register request");
        let reg_result = self.reg.register(request, &tid).await;

        match reg_result {
            Ok(_) => {
                let reply = RegisterResponse {
                    success: true,
                    function_json_result: "{\"Ok\": \"function registered\"}".into(),
                };
                Ok(Response::new(reply))
            },
            Err(msg) => {
                error!(tid=tid, error=%msg, "Registration failed");
                let reply = RegisterResponse {
                    success: false,
                    function_json_result: format!("{{\"Error\": \"Error during registration: '{:?}\"}}", msg),
                };
                Ok(Response::new(reply))
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn status(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        let request = request.into_inner();
        debug!(tid=%request.transaction_id, "Handling status request");
        let (load_avg, cpu_us, cpu_sy, cpu_id, cpu_wa, num_core) =
            self.ring_buff
                .latest(CPU_MON_TID)
                .map_or((0.0, 0.0, 0.0, 0.0, 0.0, 0), |cpu| {
                    match iluvatar_library::downcast!(cpu.1, CpuUtil) {
                        None => (0.0, 0.0, 0.0, 0.0, 0.0, 0),
                        Some(cpu) => (
                            cpu.load_avg_1minute,
                            cpu.cpu_us,
                            cpu.cpu_sy,
                            cpu.cpu_id,
                            cpu.cpu_wa,
                            cpu.num_system_cores,
                        ),
                    }
                });
        let queue_len = self.ring_buff.latest(DISPATCHER_INVOKER_LOG_TID).map_or(0, |que| {
            match iluvatar_library::downcast!(que.1, InvokerLoad) {
                None => 0,
                Some(que) => {
                    que.0.get(&Compute::CPU).map_or(0, |q| q.len) + que.0.get(&Compute::GPU).map_or(0, |q| q.len)
                },
            }
        }) as i64;
        let (used_mem, total_mem) = self.ring_buff.latest(&CTR_MGR_WORKER_TID).map_or((0, 0), |que| {
            match iluvatar_library::downcast!(que.1, ContainerMgrStat) {
                None => (0, 0),
                Some(que) => (que.used_mem, que.total_mem),
            }
        });
        Ok(Response::new(StatusResponse {
            success: true,
            queue_len,
            used_mem,
            total_mem,
            cpu_us,
            cpu_sy,
            cpu_id,
            cpu_wa,
            load_avg_1minute: load_avg,
            num_system_cores: num_core,
            num_running_funcs: self.invoker.running_funcs(),
        }))
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn health(&self, request: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let request = request.into_inner();
        debug!(tid=%request.transaction_id, "Handling health request");
        let reply = self.health.check_health(&request.transaction_id).await;
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn clean(&self, request: Request<CleanRequest>) -> Result<Response<CleanResponse>, Status> {
        let request = request.into_inner();
        debug!(tid=%request.transaction_id, "Handling clean request");
        match self
            .container_manager
            .remove_idle_containers(&request.transaction_id)
            .await
        {
            Ok(_) => Ok(Response::new(CleanResponse {})),
            Err(e) => Err(Status::internal(format!("{:?}", e))),
        }
    }
    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn list_registered_funcs(
        &self,
        request: Request<ListFunctionRequest>,
    ) -> Result<Response<ListFunctionResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, "Handling list registered functions request");
        let funcs: Vec<Arc<crate::services::registration::RegisteredFunction>> =
            self.reg.get_all_registered_functions();
        let rpc_funcs = funcs
            .iter()
            .map(|func| RegisteredFunction {
                function_name: func.function_name.clone(),
                function_version: func.function_version.clone(),
                image_name: func.image_name.clone(),
            })
            .collect();

        let reply = ListFunctionResponse { functions: rpc_funcs };

        Ok(Response::new(reply))
    }
}
