use crate::{ilúvatar_api::{WorkerAPI, HealthStatus}, transaction::TransactionId, types::MemSizeMb};
use crate::rpc::StatusResponse;

pub struct HttpWorkerAPI {

}

#[tonic::async_trait]
#[allow(unused)]

impl WorkerAPI for HttpWorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>> {
    todo!();
  }

  async fn invoke(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    todo!();
  }

  async fn invoke_async(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    todo!();
  }

  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>> {
    todo!();
  }

  async fn prewarm(&mut self, function_name: String, version: String, memory: Option<MemSizeMb>, cpu: Option<u32>, image: Option<String>, tid: TransactionId) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    todo!();
  }

  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: MemSizeMb, cpus: u32, parallels: u32, tid: TransactionId) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    todo!();
  }
  
  async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse, Box<(dyn std::error::Error + 'static)>> {
    todo!();
  }

  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus, Box<(dyn std::error::Error + 'static)>> {
    todo!();
  }
}