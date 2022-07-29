use std::{time::SystemTime, future::Future};
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Run an async task, and return how long it took to return
pub async fn time<F, Out>(func: F) -> Result<(u64,Out)>
  where
  F: Future<Output = Out>, {

  let start = SystemTime::now();

  let ret = func.await;

  let duration_ms = match start.elapsed() {
    Ok(dur) => dur,
    Err(e) => anyhow::bail!("timer error recording invocation duration: '{}'", e),
  }.as_millis() as u64;

  return Ok( (duration_ms, ret) );
}

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
