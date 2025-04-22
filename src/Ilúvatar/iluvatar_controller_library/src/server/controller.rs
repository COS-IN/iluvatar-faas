use crate::server::controller_config::ControllerConfig;
use crate::services::async_invoke::AsyncService;
use crate::services::controller_health::{ControllerHealthService, HealthService, SimHealthService};
use crate::services::load_balance::{get_balancer, LoadBalancer};
use crate::services::registration::{FunctionRegistration, WorkerRegistration};
use crate::services::ControllerAPITrait;
use anyhow::Result;
use iluvatar_library::char_map::{worker_char_map, Chars, IatTracker, WorkerCharMap};
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
    worker_reg: Arc<WorkerRegistration>,
    func_reg: Arc<FunctionRegistration>,
    iats: IatTracker,
    worker_cmap: WorkerCharMap,
}
unsafe impl Send for Controller {}

impl Controller {
    pub async fn new(config: ControllerConfig, tid: &TransactionId) -> Result<Self> {
        let worker_fact = WorkerAPIFactory::boxed();
        let worker_cmap = worker_char_map();
        let health_svc: Arc<dyn ControllerHealthService> = match iluvatar_library::utils::is_simulation() {
            true => SimHealthService::boxed(),
            false => HealthService::boxed(worker_fact.clone()),
        };

        let func_reg = FunctionRegistration::boxed(&worker_cmap);
        let lb: LoadBalancer = match get_balancer(
            &config,
            health_svc.clone(),
            tid,
            worker_fact.clone(),
            &worker_cmap,
            &func_reg,
        )
        .await
        {
            Ok(lb) => lb,
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to create load balancer"),
        };
        let worker_reg = WorkerRegistration::boxed(lb.clone(), worker_fact.clone(), &func_reg);
        let async_svc = AsyncService::boxed(worker_fact.clone());
        Ok(Controller {
            config,
            lb,
            async_svc,
            health_svc,
            worker_reg,
            func_reg,
            iats: IatTracker::new(),
            worker_cmap,
        })
    }
}

#[tonic::async_trait]
impl IluvatarController for Controller {
    #[tracing::instrument(skip(self, request), fields(tid=request.get_ref().transaction_id))]
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        match ControllerAPITrait::ping(self, request.into_inner()).await {
            Ok(r) => Ok(Response::new(PingResponse { message: r })),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, prewarm), fields(tid=prewarm.get_ref().transaction_id))]
    async fn prewarm(&self, prewarm: Request<PrewarmRequest>) -> Result<Response<PrewarmResponse>, Status> {
        match ControllerAPITrait::prewarm(self, prewarm.into_inner()).await {
            Ok(r) => Ok(Response::new(r)),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=request.get_ref().transaction_id))]
    async fn invoke(&self, request: Request<InvokeRequest>) -> Result<Response<InvokeResponse>, Status> {
        match ControllerAPITrait::invoke(self, request.into_inner()).await {
            Ok(r) => Ok(Response::new(r)),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=request.get_ref().transaction_id))]
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

    #[tracing::instrument(skip(self, request), fields(tid=request.get_ref().transaction_id))]
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

    #[tracing::instrument(skip(self, request), fields(tid=request.get_ref().transaction_id))]
    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<RegisterResponse>, Status> {
        match ControllerAPITrait::register(self, request.into_inner()).await {
            Ok(r) => Ok(Response::new(r)),
            Err(e) => Err(Status::from_error(e.into())),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=request.get_ref().name))]
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
    #[tracing::instrument(skip(self, request), fields(tid=request.transaction_id))]
    async fn ping(&self, request: PingRequest) -> Result<String> {
        info!(tid = request.transaction_id, "in ping");
        Ok("Pong".into())
    }

    #[tracing::instrument(skip(self, prewarm), fields(tid=prewarm.transaction_id))]
    async fn prewarm(&self, prewarm: PrewarmRequest) -> Result<PrewarmResponse> {
        let fqdn = calculate_fqdn(&prewarm.function_name, &prewarm.function_version);
        match self.func_reg.get_function(&fqdn) {
            Some(func) => {
                info!(tid=prewarm.transaction_id, fqdn=%fqdn, "Sending function to load balancer for prewarm");
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
                warn!(tid=prewarm.transaction_id, fqdn=%fqdn, msg);
                anyhow::bail!(msg)
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=request.transaction_id))]
    async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse> {
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        match self.func_reg.get_function(&fqdn) {
            Some(func) => {
                let mut iats = 0.0;
                if let Some(iat) = self.iats.track(&fqdn) {
                    self.worker_cmap.update(&fqdn, Chars::IAT, iat);
                    iats = iat;
                }
                info!(tid=request.transaction_id, iat=iats, fqdn=%fqdn, "Sending function to load balancer for invocation");
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
                warn!(tid=request.transaction_id, fqdn=%fqdn, msg);
                anyhow::bail!(msg)
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=request.transaction_id))]
    async fn invoke_async(&self, request: InvokeAsyncRequest) -> Result<String> {
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        match self.func_reg.get_function(&fqdn) {
            Some(func) => {
                if let Some(iat) = self.iats.track(&fqdn) {
                    self.worker_cmap.update(&fqdn, Chars::IAT, iat);
                }
                info!(tid=request.transaction_id, fqdn=%fqdn, "Sending function to load balancer for async invocation");
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
                        error!(tid=request.transaction_id, error=%e, "async invocation failed");
                        Err(e)
                    },
                }
            },
            None => {
                let msg = "Function was not registered; could not invoke async";
                warn!(tid=request.transaction_id, fqdn=%fqdn, msg);
                anyhow::bail!(msg);
            },
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=request.transaction_id))]
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

    #[tracing::instrument(skip(self, request), fields(tid=request.transaction_id))]
    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        if request.system_function {
            anyhow::bail!("Cannot register a system function, these are internal only!");
        }
        let tid = request.transaction_id.clone();
        match self.func_reg.register_function(request, &tid).await {
            Ok(reg) => match self.worker_reg.new_function(reg, &tid).await {
                Ok(reg) => Ok(RegisterResponse {
                    success: true,
                    fqdn: reg.fqdn.clone(),
                    error: "".to_string(),
                }),
                Err(e) => Ok(RegisterResponse {
                    success: false,
                    fqdn: "".to_string(),
                    error: format!("{:?}", e),
                }),
            },
            Err(e) => Ok(RegisterResponse {
                success: false,
                fqdn: "".to_string(),
                error: format!("{:?}", e),
            }),
        }
    }

    #[tracing::instrument(skip(self, request), fields(tid=request.name))]
    async fn register_worker(&self, request: RegisterWorkerRequest) -> Result<()> {
        let tid = gen_tid();
        let worker = match self.worker_reg.register_worker(request, &tid).await {
            Ok(w) => w,
            Err(e) => return Err(e),
        };
        self.health_svc
            .schedule_health_check(self.health_svc.clone(), worker, &tid, Some(Duration::from_secs(5)));
        Ok(())
    }
}
