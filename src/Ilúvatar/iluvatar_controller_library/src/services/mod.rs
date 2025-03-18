pub mod async_invoke;
pub mod controller_health;
pub mod load_balance;
pub mod load_reporting;
pub mod registration;

use anyhow::Result;
use iluvatar_rpc::rpc::{
    InvokeAsyncLookupRequest, InvokeAsyncRequest, InvokeRequest, InvokeResponse, PingRequest, PrewarmRequest,
    PrewarmResponse, RegisterRequest, RegisterResponse, RegisterWorkerRequest,
};

#[tonic::async_trait]
pub trait ControllerAPITrait {
    async fn ping(&self, msg: PingRequest) -> Result<String>;
    async fn prewarm(&self, prewarm: PrewarmRequest) -> Result<PrewarmResponse>;
    async fn invoke(&self, invoke: InvokeRequest) -> Result<InvokeResponse>;
    async fn invoke_async(&self, invoke: InvokeAsyncRequest) -> Result<String>;
    async fn invoke_async_check(&self, check: InvokeAsyncLookupRequest) -> Result<Option<InvokeResponse>>;
    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse>;
    async fn register_worker(&self, request: RegisterWorkerRequest) -> Result<()>;
}

pub type ControllerAPI = std::sync::Arc<dyn ControllerAPITrait + Sync + Send>;
