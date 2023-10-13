use crate::rpc::iluvatar_worker_server::IluvatarWorker;
use crate::rpc::*;
use crate::services::containers::containermanager::ContainerManager;
use crate::services::influx_updater::InfluxUpdater;
use crate::services::invocation::Invoker;
use crate::services::status::status_service::StatusService;
use crate::services::{registration::RegistrationService, worker_health::WorkerHealthService};
use crate::worker_api::config::WorkerConfig;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use iluvatar_library::{
    characteristics_map::CharacteristicsMap, energy::energy_logging::EnergyLogger, utils::calculate_fqdn,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info};

#[allow(unused)]
pub struct IluvatarWorkerImpl {
    container_manager: Arc<ContainerManager>,
    config: WorkerConfig,
    invoker: Arc<dyn Invoker>,
    status: Arc<StatusService>,
    health: Arc<WorkerHealthService>,
    energy: Arc<EnergyLogger>,
    cmap: Arc<CharacteristicsMap>,
    reg: Arc<RegistrationService>,
    updater: Option<Arc<InfluxUpdater>>,
}

impl IluvatarWorkerImpl {
    pub fn new(
        config: WorkerConfig,
        container_manager: Arc<ContainerManager>,
        invoker: Arc<dyn Invoker>,
        status: Arc<StatusService>,
        health: Arc<WorkerHealthService>,
        energy: Arc<EnergyLogger>,
        cmap: Arc<CharacteristicsMap>,
        reg: Arc<RegistrationService>,
        updater: Option<Arc<InfluxUpdater>>,
    ) -> IluvatarWorkerImpl {
        IluvatarWorkerImpl {
            container_manager,
            config,
            invoker,
            status,
            health,
            energy,
            cmap,
            reg,
            updater,
        }
    }
}

#[tonic::async_trait]
impl IluvatarWorker for IluvatarWorkerImpl {
    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        println!("Got a request: {:?}", request);
        let reply = PingResponse {
            message: format!("Pong").into(),
        };
        info!("in ping");
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id, function_name=%request.get_ref().function_name, function_version=%request.get_ref().function_version))]
    async fn invoke(&self, request: Request<InvokeRequest>) -> Result<Response<InvokeResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, function_name=%request.function_name, function_version=%request.function_version, "Handling invocation request");
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        let reg = match self.reg.get_registration(&fqdn) {
            Some(r) => r,
            None => return Ok(Response::new(InvokeResponse::error("Function was not registered"))),
        };
        self.cmap.add_iat(&fqdn);
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
            }
            Err(e) => {
                error!("Invoke failed with error: {}", e);
                Ok(Response::new(InvokeResponse::error(&e.to_string())))
            }
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
            }
        };
        self.cmap.add_iat(&fqdn);
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
            }
            Err(e) => {
                error!("Failed to launch an async invocation with error '{}'", e);
                Ok(Response::new(InvokeAsyncResponse {
                    lookup_cookie: "".to_string(),
                    success: false,
                }))
            }
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
                // Ok(Response::new(InvokeResponse {
                //   json_result: format!("{{ \"Error\": \"{}\" }}", e.to_string()),
                //   success: false,
                //   duration_us: 0
                // }))
            }
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
                    message: format!("{{ \"Error\": \"Function was not registered\" }}"),
                };
                return Ok(Response::new(resp));
            }
        };
        let compute: Compute = request.compute.into();
        if !reg.supported_compute.intersects(compute) {
            let resp = PrewarmResponse {
                success: false,
                message: format!("{{ \"Error\": \"Function was not registered with the specified compute\" }}"),
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
            }
            Err(e) => {
                error!(tid=%request.transaction_id, error=%e, "Container prewarm failed");
                let resp = PrewarmResponse {
                    success: false,
                    message: format!("{{ \"Error\": \"{}\" }}", e.to_string()),
                };
                Ok(Response::new(resp))
            }
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
                    function_json_result: format!("{{\"Ok\": \"function registered\"}}").into(),
                };
                Ok(Response::new(reply))
            }
            Err(msg) => {
                error!(tid=%tid, error=%msg, "Registration failed");
                let reply = RegisterResponse {
                    success: false,
                    function_json_result: format!("{{\"Error\": \"Error during registration: '{:?}\"}}", msg).into(),
                };
                Ok(Response::new(reply))
            }
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn status(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, "Handling status request");

        let stat = self.status.get_status(&request.transaction_id);

        let resp = StatusResponse {
            success: true,
            queue_len: stat.cpu_queue_len + stat.gpu_queue_len,
            used_mem: stat.used_mem,
            total_mem: stat.total_mem,
            cpu_us: stat.cpu_us,
            cpu_sy: stat.cpu_sy,
            cpu_id: stat.cpu_id,
            cpu_wa: stat.cpu_wa,
            load_avg_1minute: stat.load_avg_1minute,
            num_system_cores: stat.num_system_cores,
            num_running_funcs: stat.num_running_funcs,
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn health(&self, request: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, "Handling health request");
        let reply = self.health.check_health(&request.transaction_id).await;
        Ok(Response::new(reply))
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn clean(&self, request: Request<CleanRequest>) -> Result<Response<CleanResponse>, Status> {
        let request = request.into_inner();
        info!(tid=%request.transaction_id, "Handling clean request");
        match self
            .container_manager
            .remove_idle_containers(&request.transaction_id)
            .await
        {
            Ok(_) => Ok(Response::new(CleanResponse {})),
            Err(e) => Err(Status::internal(format!("{:?}", e))),
        }
    }
}
