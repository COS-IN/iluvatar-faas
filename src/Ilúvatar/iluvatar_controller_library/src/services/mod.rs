pub mod async_invoke;
pub mod controller_health;
pub mod load_balance;
pub mod load_reporting;
pub mod registration;

use iluvatar_library::{transaction::TransactionId, types::MemSizeMb};
use anyhow::Result;
use iluvatar_worker_library::worker_api::HealthStatus;
use crate::rpc::{CleanResponse, InvokeResponse, StatusResponse};
use iluvatar_library::types::{Compute, Isolation, ResourceTimings};

#[tonic::async_trait]
pub trait ControllerAPI {
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
