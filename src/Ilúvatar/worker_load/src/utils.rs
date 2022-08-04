use serde::{Deserialize, Serialize};

pub struct ThreadError {
  pub thread_id: usize,
  pub error: anyhow::Error
}
#[derive(Serialize,Deserialize)]
pub struct ThreadResult {
  pub thread_id: usize,
  pub data: Vec<InvocationResult>,
  pub registration: RegistrationResult,
  pub errors: u64,
}
#[derive(Serialize,Deserialize)]
pub struct InvocationResult {
  pub json: RealInvokeResult,
  pub duration_ms: u64
}
#[derive(Serialize,Deserialize)]
pub struct RegistrationResult {
  pub duration_ms: u64,
  pub result: String
}

// {"body": {"greeting": greeting, "cold":was_cold, "start":start, "end":end, "latency": end-start} }
#[derive(Serialize,Deserialize)]
pub struct RealInvokeResult {
  pub body: Body
}
#[derive(Serialize,Deserialize)]
pub struct Body {
  // pub greeting: String,
  pub cold: bool,
  pub start: f64,
  pub end: f64,
  /// python runtime latency in seconds
  pub latency: f64,
}
