use tonic::async_trait;

#[async_trait]
pub trait WorkerAPI {
  async fn ping(&mut self) -> Result<String, Box<dyn std::error::Error>>;
  async fn invoke(&mut self, function_name: &str, version: &str) -> Result<String, Box<dyn std::error::Error>>;
  async fn register(&mut self, function_name: &str, version: &str) -> Result<String, Box<dyn std::error::Error>>;
  async fn status(&mut self) -> Result<String, Box<dyn std::error::Error>>;
}
