use crate::services::ControllerAPITrait;
use anyhow::{bail, Result};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::utils::port_utils::Port;
use iluvatar_rpc::rpc::iluvatar_controller_client::IluvatarControllerClient;
use iluvatar_rpc::rpc::{
    InvokeAsyncLookupRequest, InvokeAsyncRequest, InvokeRequest, InvokeResponse, PingRequest, PrewarmRequest,
    PrewarmResponse, RegisterRequest, RegisterResponse, RegisterWorkerRequest,
};
use iluvatar_rpc::RPCError;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};
use tracing::warn;

pub struct RpcControllerAPI {
    client: IluvatarControllerClient<Channel>,
}

impl RpcControllerAPI {
    /// Open a new connection to a controller API
    /// Returns a [RPCError] with details if the connection fails
    pub async fn new(address: &str, port: Port, tid: &TransactionId) -> Result<RpcControllerAPI, RPCError> {
        Self::repeat_try_connection(address, port, 5, tid).await
    }

    /// Try opening a new connection, with several retries
    /// This can be flaky, so if there is an error, the connection is retried several times
    async fn repeat_try_connection(
        address: &str,
        port: Port,
        mut retries: u32,
        tid: &TransactionId,
    ) -> Result<RpcControllerAPI, RPCError> {
        loop {
            match Self::try_new_connection(address, port).await {
                Ok(api) => {
                    return Ok(api);
                },
                Err(e) => {
                    warn!(error=%e, tid=tid, "Error opening RPC connection to controller API");
                    retries -= 1;
                    if retries == 0 {
                        return Err(e);
                    }
                },
            }
        }
    }

    async fn try_new_connection(address: &str, port: Port) -> Result<RpcControllerAPI, RPCError> {
        let addr = format!("http://{}:{}", address, port);
        match IluvatarControllerClient::connect(addr).await {
            Ok(c) => Ok(RpcControllerAPI { client: c }),
            Err(e) => Err(RPCError {
                message: Status::new(Code::Unknown, format!("Got unexpected error of {:?}", e)),
                source: "[RCPcontrollerAPI:new]".to_string(),
            }),
        }
    }
}

impl Clone for RpcControllerAPI {
    /// A fast method for duplicating the controller API
    /// Use this instead of concurently sharing
    fn clone(&self) -> Self {
        RpcControllerAPI {
            client: self.client.clone(),
        }
    }
}

/// An implementation of the controller API that communicates with controllers via RPC
#[tonic::async_trait]
impl ControllerAPITrait for RpcControllerAPI {
    async fn ping(&self, msg: PingRequest) -> Result<String> {
        let request = Request::new(msg);
        match self.client.clone().ping(request).await {
            Ok(response) => Ok(response.into_inner().message),
            Err(e) => bail!(RPCError::new(e, "[RCPcontrollerAPI:ping]".to_string())),
        }
    }

    #[tracing::instrument(skip(self, prewarm), fields(tid=prewarm.transaction_id))]
    async fn prewarm(&self, prewarm: PrewarmRequest) -> Result<PrewarmResponse> {
        let request = Request::new(prewarm);
        match self.client.clone().prewarm(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPcontrollerAPI:prewarm]".to_string())),
        }
    }

    async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResponse> {
        let request = Request::new(request);
        match self.client.clone().invoke(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPcontrollerAPI:invoke]".to_string())),
        }
    }

    async fn invoke_async(&self, invoke: InvokeAsyncRequest) -> Result<String> {
        let request = Request::new(invoke);
        match self.client.clone().invoke_async(request).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.success {
                    Ok(response.lookup_cookie)
                } else {
                    anyhow::bail!("Async invoke failed")
                }
            },
            Err(e) => bail!(RPCError::new(e, "[RCPcontrollerAPI:invoke_async]".to_string())),
        }
    }

    async fn invoke_async_check(&self, check: InvokeAsyncLookupRequest) -> Result<Option<InvokeResponse>> {
        let request = Request::new(check);
        match self.client.clone().invoke_async_check(request).await {
            Ok(response) => Ok(Some(response.into_inner())),
            Err(e) => bail!(RPCError::new(e, "[RCPcontrollerAPI:invoke_async_check]".to_string())),
        }
    }

    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        let request = Request::new(request);
        match self.client.clone().register(request).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => bail!(RPCError::new(e, "[RCPcontrollerAPI:register]".to_string())),
        }
    }

    async fn register_worker(&self, request: RegisterWorkerRequest) -> Result<()> {
        match self.client.clone().register_worker(Request::new(request)).await {
            Ok(_) => Ok(()),
            Err(e) => bail!(RPCError::new(e, "[RCPcontrollerAPI:register_worker]".to_string())),
        }
    }
}
