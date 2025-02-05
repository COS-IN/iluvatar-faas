use crate::server::controller_config::ControllerConfig;
use crate::services::async_invoke::AsyncService;
use crate::services::controller_health::{ControllerHealthService, HealthService, SimHealthService};
use crate::services::load_balance::{get_balancer, LoadBalancer};
use crate::services::load_reporting::LoadService;
use crate::services::registration::RegistrationService;
use crate::services::ControllerAPITrait;
use anyhow::Result;
use iluvatar_library::influx::InfluxClient;
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::utils::calculate_fqdn;
use iluvatar_library::{bail_error, transaction::TransactionId};
use iluvatar_rpc::rpc::iluvatar_controller_server::IluvatarController;
use iluvatar_rpc::rpc::{
    InvokeAsyncLookupRequest, InvokeAsyncRequest, InvokeRequest, PingRequest, PrewarmRequest, PrewarmResponse,
    RegisterRequest, RegisterWorkerRequest, RegisterWorkerResponse,
};
use iluvatar_rpc::rpc::{InvokeAsyncResponse, InvokeResponse, PingResponse, RegisterResponse};
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::{sync::Arc, time::Duration};
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

#[allow(unused)]
pub struct Controller {
    config: ControllerConfig,
    lb: LoadBalancer,
    async_svc: Arc<AsyncService>,
    health_svc: Arc<dyn ControllerHealthService>,
    load_svc: Arc<LoadService>,
    registration_svc: Arc<RegistrationService>,
}
unsafe impl Send for Controller {}

impl Controller {
    pub async fn new(config: ControllerConfig, tid: &TransactionId) -> Result<Self> {
        let worker_fact = WorkerAPIFactory::boxed();
        let health_svc: Arc<dyn ControllerHealthService> = match iluvatar_library::utils::is_simulation() {
            true => SimHealthService::boxed(),
            false => HealthService::boxed(worker_fact.clone()),
        };

        let influx = match InfluxClient::new(config.influx.clone(), tid).await {
            Ok(i) => i,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to create InfluxClient"),
        };
        let load_svc = match LoadService::boxed(influx, config.load_balancer.clone(), tid, worker_fact.clone()) {
            Ok(l) => l,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to create LoadService"),
        };
        let lb: LoadBalancer =
            match get_balancer(&config, health_svc.clone(), tid, load_svc.clone(), worker_fact.clone()) {
                Ok(lb) => lb,
                Err(e) => bail_error!(tid=%tid, error=%e, "Failed to create load balancer"),
            };
        let reg_svc = RegistrationService::boxed(lb.clone(), worker_fact.clone());
        let async_svc = AsyncService::boxed(worker_fact.clone());
        Ok(Controller {
            config,
            lb,
            async_svc,
            health_svc,
            load_svc,
            registration_svc: reg_svc,
        })
    }
}

#[tonic::async_trait]
impl IluvatarController for Controller {
    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        match ControllerAPITrait::ping(self, request.into_inner()).await {
            Ok(r) => Ok(Response::new(PingResponse { message: r })),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, prewarm), fields(tid=%prewarm.get_ref().transaction_id))]
    async fn prewarm(&self, prewarm: Request<PrewarmRequest>) -> Result<Response<PrewarmResponse>, Status> {
        match ControllerAPITrait::prewarm(self, prewarm.into_inner()).await {
            Ok(r) => Ok(Response::new(r)),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id, function_name=%request.get_ref().function_name, function_version=%request.get_ref().function_version))]
    async fn invoke(&self, request: Request<InvokeRequest>) -> Result<Response<InvokeResponse>, Status> {
        match ControllerAPITrait::invoke(self, request.into_inner()).await {
            Ok(r) => Ok(Response::new(r)),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn invoke_async(
        &self,
        request: Request<InvokeAsyncRequest>,
    ) -> Result<Response<InvokeAsyncResponse>, Status> {
        match ControllerAPITrait::invoke_async(self, request.into_inner()).await {
            Ok(r) => Ok(Response::new(InvokeAsyncResponse {
                success: true,
                lookup_cookie: r,
            })),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn invoke_async_check(
        &self,
        request: Request<InvokeAsyncLookupRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        match ControllerAPITrait::invoke_async_check(self, request.into_inner()).await {
            Ok(r) => match r {
                Some(r) => Ok(Response::new(r)),
                None => Err(Status::unavailable("NOT READY")),
            },
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().transaction_id))]
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<RegisterResponse>, Status> {
        match ControllerAPITrait::register(self, request.into_inner()).await {
            Ok(r) => Ok(Response::new(RegisterResponse {
                success: true,
                function_json_result: r,
            })),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.get_ref().name))]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        match ControllerAPITrait::register_worker(self, request.into_inner()).await {
            Ok(_) => Ok(Response::new(RegisterWorkerResponse {})),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }
}

#[tonic::async_trait]
impl ControllerAPITrait for Controller {
    #[tracing::instrument(skip(self, request), fields(tid=%request.transaction_id))]
    async fn ping(&self, request: PingRequest) -> Result<String> {
        info!(tid=%request.transaction_id, "in ping");
        Ok("Pong".into())
    }

    #[tracing::instrument(skip(self, prewarm), fields(tid=%prewarm.transaction_id))]
    async fn prewarm(&self, prewarm: PrewarmRequest) -> Result<PrewarmResponse> {
        let fqdn = calculate_fqdn(&prewarm.function_name, &prewarm.function_version);
        match self.registration_svc.get_function(&fqdn) {
            Some(func) => {
                info!(tid=%prewarm.transaction_id, fqdn=%fqdn, "Sending function to load balancer for invocation");
                match self.lb.prewarm(func, &prewarm.transaction_id).await {
                    Ok(_dur) => Ok(PrewarmResponse {
                        success: true,
                        message: "".to_owned(),
                    }),
                    Err(e) => Err(e),
                }
            },
            None => {
                let msg = "Function was not registered; could not prewarm";
                warn!(tid=%prewarm.transaction_id, fqdn=%fqdn, msg);
                anyhow::bail!(msg)
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.transaction_id, function_name=%request.function_name, function_version=%request.function_version))]
    async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse> {
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        match self.registration_svc.get_function(&fqdn) {
            Some(func) => {
                info!(tid=%request.transaction_id, fqdn=%fqdn, "Sending function to load balancer for invocation");
                match self
                    .lb
                    .send_invocation(func, request.json_args, &request.transaction_id)
                    .await
                {
                    Ok((result, _dur)) => Ok(result),
                    Err(e) => Err(e),
                }
            },
            None => {
                let msg = "Function was not registered; could not invoke";
                warn!(tid=%request.transaction_id, fqdn=%fqdn, msg);
                anyhow::bail!(msg)
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.transaction_id))]
    async fn invoke_async(&self, request: InvokeAsyncRequest) -> Result<String> {
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        match self.registration_svc.get_function(&fqdn) {
            Some(func) => {
                info!(tid=%request.transaction_id, fqdn=%fqdn, "Sending function to load balancer for async invocation");
                match self
                    .lb
                    .send_async_invocation(func, request.json_args, &request.transaction_id)
                    .await
                {
                    Ok((cookie, worker, _duration)) => {
                        self.async_svc
                            .register_async_invocation(cookie.clone(), worker, &request.transaction_id);
                        Ok(cookie)
                    },
                    Err(e) => {
                        error!(tid=%request.transaction_id, error=%e, "async invocation failed");
                        Err(e)
                    },
                }
            },
            None => {
                let msg = "Function was not registered; could not invoke async";
                warn!(tid=%request.transaction_id, fqdn=%fqdn, msg);
                anyhow::bail!(msg);
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.transaction_id))]
    async fn invoke_async_check(&self, request: InvokeAsyncLookupRequest) -> Result<Option<InvokeResponse>> {
        match self
            .async_svc
            .check_async_invocation(request.lookup_cookie, &request.transaction_id)
            .await
        {
            Ok(r) => match r {
                Some(r) => Ok(Some(r)),
                None => Ok(None),
            },
            Err(e) => Err(e),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.transaction_id))]
    async fn register(&self, request: RegisterRequest) -> Result<String> {
        let tid = request.transaction_id.clone();
        match self.registration_svc.register_function(request, &tid).await {
            Ok(_) => Ok("{}".to_owned()),
            Err(e) => Err(e),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=%request.name))]
    async fn register_worker(&self, request: RegisterWorkerRequest) -> Result<()> {
        let tid = gen_tid();
        let worker = match self.registration_svc.register_worker(request, &tid).await {
            Ok(w) => w,
            Err(e) => return Err(e),
        };
        self.health_svc
            .schedule_health_check(self.health_svc.clone(), worker, &tid, Some(Duration::from_secs(5)));
        Ok(())
    }
}
