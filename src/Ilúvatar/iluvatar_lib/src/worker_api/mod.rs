use tonic::async_trait;

use crate::transaction::TransactionId;

pub enum HealthStatus {
  HEALTHY,
  UNHEALTHY,
}

#[async_trait]
pub trait WorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>>;
  async fn invoke(&mut self, function_name: String, version: String, args: String, memory: Option<u32>, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>>;
  async fn invoke_async(&mut self, function_name: String, version: String, args: String, memory: Option<u32>, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>>;
  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>>;
  async fn prewarm(&mut self, function_name: String, version: String, memory: Option<u32>, cpu: Option<u32>, image: Option<String>, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>>;
  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: u32, cpus: u32, parallels: u32, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>>;
  async fn status(&mut self, tid: TransactionId) -> Result<String, Box<dyn std::error::Error>>;
  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus, Box<dyn std::error::Error>>;
}
