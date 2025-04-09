// use iluvatar_rpc::worker::RPCWorkerAPI;
use super::WorkerAPI;
use anyhow::{bail, Result};
use iluvatar_library::bail_error;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::{Compute, ContainerServer, HealthStatus, Isolation, MemSizeMb, ResourceTimings};
use iluvatar_library::utils::port::Port;
use iluvatar_rpc::rpc::iluvatar_worker_client::IluvatarWorkerClient;
use iluvatar_rpc::rpc::{
    CleanRequest, HealthRequest, InvokeAsyncLookupRequest, InvokeAsyncRequest, InvokeRequest, ListFunctionRequest,
    PingRequest, PrewarmRequest, RegisterRequest, StatusRequest,
};
use iluvatar_rpc::rpc::{CleanResponse, InvokeResponse, LanguageRuntime, ListFunctionResponse, StatusResponse};
use iluvatar_rpc::RPCError;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};
use tracing::{debug, error, warn};

#[allow(unused)]
pub struct RPCWorkerAPI {
    client: IluvatarWorkerClient<Channel>,
}

impl RPCWorkerAPI {
    /// Open a new connection to a Worker API
    /// Returns a [RPCError] with details if the connection fails
    pub async fn new(address: &str, port: Port, tid: &TransactionId) -> Result<RPCWorkerAPI, RPCError> {
        Self::repeat_try_connection(address, port, 5, tid).await
    }

    /// Try opening a new connection, with several retries
    /// This can be flaky, so if there is an error, the connection is retried several times
    async fn repeat_try_connection(
        address: &str,
        port: Port,
        mut retries: u32,
        tid: &TransactionId,
    ) -> Result<RPCWorkerAPI, RPCError> {
        loop {
            match Self::try_new_connection(address, port).await {
                Ok(api) => {
                    return Ok(api);
                },
                Err(e) => {
                    warn!(error=%e, tid=tid, "Error opening RPC connection to Worker API");
                    retries -= 1;
                    if retries == 0 {
                        return Err(e);
                    }
                },
            }
        }
    }

    async fn try_new_connection(address: &str, port: Port) -> Result<RPCWorkerAPI, RPCError> {
        let addr = format!("http://{}:{}", address, port);
        match IluvatarWorkerClient::connect(addr).await {
            Ok(c) => Ok(RPCWorkerAPI { client: c }),
            Err(e) => Err(RPCError {
                message: Status::new(Code::Unknown, format!("Got unexpected error of {:?}", e)),
                source: "[RCPWorkerAPI:new]".to_string(),
            }),
        }
    }
}

impl Clone for RPCWorkerAPI {
    /// A fast method for duplicating the Worker API.
    /// Use this instead of concurrently sharing.
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
        }
    }
}

/// An implementation of the worker API that communicates with workers via RPC
#[tonic::async_trait]
impl WorkerAPI for RPCWorkerAPI {
    async fn ping(&mut self, tid: TransactionId) -> Result<String> {
        let request = Request::new(PingRequest {
            message: "Ping".to_string(),
            transaction_id: tid,
        });
        match self.client.ping(request).await {
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
        let request = Request::new(InvokeRequest {
            function_name,
            function_version: version,
            json_args: args,
            transaction_id: tid,
        });
        match self.client.invoke(request).await {
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
        let request = Request::new(InvokeAsyncRequest {
            function_name,
            function_version: version,
            json_args: args,
            transaction_id: tid.clone(),
        });
        match self.client.invoke_async(request).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.success {
                    debug!(tid = tid, "Async invoke succeeded");
                    Ok(response.lookup_cookie)
                } else {
                    error!(tid = tid, "Async invoke failed");
                    anyhow::bail!("Async invoke failed")
                }
            },
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:invoke_async]".to_string())),
        }
    }

    async fn invoke_async_check(&mut self, cookie: &str, tid: TransactionId) -> Result<InvokeResponse> {
        let request = Request::new(InvokeAsyncLookupRequest {
            lookup_cookie: cookie.to_owned(),
            transaction_id: tid,
        });
        match self.client.invoke_async_check(request).await {
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
        let request = Request::new(PrewarmRequest {
            function_name,
            function_version: version,
            transaction_id: tid.clone(),
            compute: compute.bits(),
        });
        match self.client.prewarm(request).await {
            Ok(response) => {
                let response = response.into_inner();
                let err = format!("Prewarm request failed: {:?}", response.message);
                match response.success {
                    true => Ok(response.message),
                    false => bail_error!(tid=tid, message=%response.message, err),
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
        timings: Option<&ResourceTimings>,
        system_function: bool,
    ) -> Result<String> {
        let request = Request::new(RegisterRequest::new(
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
        match self.client.register(request).await {
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
        let request = Request::new(StatusRequest { transaction_id: tid });
        match self.client.status(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:status]".to_string())),
        }
    }

    async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus> {
        let request = Request::new(HealthRequest { transaction_id: tid });
        match self.client.health(request).await {
            Ok(response) => {
                match response.into_inner().status {
                    // HealthStatus::Healthy
                    0 => Ok(HealthStatus::HEALTHY),
                    // HealthStatus::Unhealthy
                    1 => Ok(HealthStatus::UNHEALTHY),
                    i => anyhow::bail!(RPCError {
                        message: Status::new(Code::InvalidArgument, format!("Got unexpected status of {}", i)),
                        source: "[RCPWorkerAPI:health]".to_string()
                    }),
                }
            },
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:health]".to_string())),
        }
    }

    async fn list_registered_funcs(&mut self, tid: TransactionId) -> Result<ListFunctionResponse> {
        let request = Request::new(ListFunctionRequest { transaction_id: tid });
        match self.client.list_registered_funcs(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:list_registered_funcs]".to_string())),
        }
    }

    async fn clean(&mut self, tid: TransactionId) -> Result<CleanResponse> {
        let request = Request::new(CleanRequest { transaction_id: tid });
        match self.client.clean(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPWorkerAPI:clean]".to_string())),
        }
    }
}
