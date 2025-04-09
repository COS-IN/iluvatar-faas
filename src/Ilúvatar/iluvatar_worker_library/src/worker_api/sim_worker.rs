use super::iluvatar_worker::IluvatarWorkerImpl;
use super::WorkerAPI;
use anyhow::{bail, Result};
use iluvatar_library::bail_error;
use iluvatar_library::types::{Compute, ContainerServer, HealthStatus, Isolation};
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb};
use iluvatar_rpc::rpc::iluvatar_worker_server::IluvatarWorker;
use iluvatar_rpc::rpc::{
    CleanRequest, CleanResponse, HealthRequest, InvokeAsyncLookupRequest, InvokeAsyncRequest, InvokeRequest,
    LanguageRuntime, ListFunctionRequest, PingRequest, PrewarmRequest, RegisterRequest, StatusRequest,
};
use iluvatar_rpc::rpc::{InvokeResponse, ListFunctionResponse, StatusResponse};
use iluvatar_rpc::RPCError;
use std::sync::Arc;
#[cfg(feature = "full_spans")]
use tracing::Instrument;
use tracing::{debug, error};

/// A simulation version of the WorkerAPI
///   must match [crate::rpc::RPCWorkerAPI] in handling, etc.
pub struct SimWorkerAPI {
    worker: Arc<IluvatarWorkerImpl>,
}
impl SimWorkerAPI {
    pub fn new(worker: Arc<IluvatarWorkerImpl>) -> Self {
        SimWorkerAPI { worker }
    }
}

#[tonic::async_trait]
impl WorkerAPI for SimWorkerAPI {
    async fn ping(&mut self, tid: TransactionId) -> Result<String> {
        let request = tonic::Request::new(PingRequest {
            message: "Ping".to_string(),
            transaction_id: tid,
        });
        let inv = self.worker.ping(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => Ok(response.into_inner().message),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:ping]".to_string())),
        }
    }

    async fn invoke(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<InvokeResponse> {
        let request = tonic::Request::new(InvokeRequest {
            function_name,
            function_version: version,
            json_args: args,
            transaction_id: tid,
        });
        let inv = self.worker.invoke(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:invoke]".to_string())),
        }
    }

    async fn invoke_async(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<String> {
        let request = tonic::Request::new(InvokeAsyncRequest {
            function_name,
            function_version: version,
            json_args: args,
            transaction_id: tid.clone(),
        });
        let inv = self.worker.invoke_async(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => {
                let response = response.into_inner();
                if response.success {
                    debug!(tid = tid, "Async invoke succeeded");
                    Ok(response.lookup_cookie)
                } else {
                    error!(tid = tid, "Async invoke failed");
                    bail!("Async invoke failed")
                }
            },
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:invoke_async]".to_string())),
        }
    }

    async fn invoke_async_check(&mut self, cookie: &str, tid: TransactionId) -> Result<InvokeResponse> {
        let request = tonic::Request::new(InvokeAsyncLookupRequest {
            lookup_cookie: cookie.to_owned(),
            transaction_id: tid,
        });
        let inv = self.worker.invoke_async_check(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:invoke_async_check]".to_string())),
        }
    }

    async fn prewarm(
        &mut self,
        function_name: String,
        version: String,
        tid: TransactionId,
        compute: Compute,
    ) -> Result<String> {
        let request = tonic::Request::new(PrewarmRequest {
            function_name,
            function_version: version,
            transaction_id: tid.clone(),
            compute: compute.bits(),
        });
        let inv = self.worker.prewarm(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => {
                let response = response.into_inner();
                match response.success {
                    true => Ok("".to_string()),
                    false => {
                        bail_error!(tid=tid, message=%response.message, "Prewarm request failed")
                    },
                }
            },
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:prewarm]".to_string())),
        }
    }

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
        server: ContainerServer,
        timings: Option<&iluvatar_library::types::ResourceTimings>,
        system_function: bool,
    ) -> Result<String> {
        let request = tonic::Request::new(RegisterRequest::new(
            &function_name,
            &version,
            &image_name,
            cpus,
            memory,
            timings,
            LanguageRuntime::Nolang,
            compute,
            isolate,
            server,
            parallels,
            &tid,
            system_function,
        )?);
        let inv = self.worker.register(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => {
                let response = response.into_inner();
                match response.success {
                    true => Ok(response.fqdn),
                    false => Err(anyhow::anyhow!(response.error)),
                }
            },
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:register]".to_string())),
        }
    }

    async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse> {
        let request = tonic::Request::new(StatusRequest { transaction_id: tid });
        let inv = self.worker.status(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:status]".to_string())),
        }
    }

    async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus> {
        let request = tonic::Request::new(HealthRequest { transaction_id: tid });
        let inv = self.worker.health(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => {
                match response.into_inner().status {
                    // HealthStatus::Healthy
                    0 => Ok(HealthStatus::HEALTHY),
                    // HealthStatus::Unhealthy
                    1 => Ok(HealthStatus::UNHEALTHY),
                    i => anyhow::bail!(RPCError::new(
                        tonic::Status::new(tonic::Code::InvalidArgument, format!("Got unexpected status of {}", i)),
                        "[RCPWorkerAPI:health]".to_string()
                    )),
                }
            },
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:register]".to_string())),
        }
    }

    async fn clean(&mut self, tid: TransactionId) -> Result<CleanResponse> {
        let request = tonic::Request::new(CleanRequest { transaction_id: tid });
        let inv = self.worker.clean(request);
        #[cfg(feature = "full_spans")]
        let inv = inv.instrument(name_span!(self.worker.config.name));
        match inv.await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:clean]".to_string())),
        }
    }
    async fn list_registered_funcs(&mut self, tid: TransactionId) -> Result<ListFunctionResponse> {
        let request = tonic::Request::new(ListFunctionRequest { transaction_id: tid });
        match self.worker.list_registered_funcs(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:list_registered_funcs]".to_string())),
        }
    }
}
