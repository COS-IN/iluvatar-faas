use crate::{transaction::TransactionId, types::MemSizeMb};
use crate::rpc::{StatusResponse, InvokeResponse};
use anyhow::Result;

#[derive(Debug, PartialEq, Eq)]
pub enum HealthStatus {
  HEALTHY,
  UNHEALTHY,
}

#[tonic::async_trait]
pub trait WorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String>;
  async fn invoke(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<String>;
  async fn invoke_async(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<String>;
  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<InvokeResponse>;
  async fn prewarm(&mut self, function_name: String, version: String, memory: Option<MemSizeMb>, cpu: Option<u32>, image: Option<String>, tid: TransactionId) -> Result<String>;
  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: MemSizeMb, cpus: u32, parallels: u32, tid: TransactionId) -> Result<String>;
  async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse>;
  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus>;
}
