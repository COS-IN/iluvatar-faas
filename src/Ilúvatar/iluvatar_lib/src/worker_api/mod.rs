use tonic::async_trait;

pub enum HealthStatus {
  HEALTHY,
  UNHEALTHY,
}

#[async_trait]
pub trait WorkerAPI {
  async fn ping(&mut self) -> Result<String, Box<dyn std::error::Error>>;
  async fn invoke(&mut self, function_name: &str, version: &str) -> Result<String, Box<dyn std::error::Error>>;
  async fn invoke_async(&mut self, function_name: &str, version: &str) -> Result<String, Box<dyn std::error::Error>>;
  async fn prewarm(&mut self, function_name: &str, version: &str) -> Result<String, Box<dyn std::error::Error>>;
  async fn register(&mut self, function_name: &str, version: &str) -> Result<String, Box<dyn std::error::Error>>;
  async fn status(&mut self) -> Result<String, Box<dyn std::error::Error>>;
  async fn health(&mut self) -> Result<HealthStatus, Box<dyn std::error::Error>>;
}
