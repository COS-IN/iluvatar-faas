use std::{time::SystemTime, future::Future};
use anyhow::Result;
use serde::{Deserialize, Serialize};

///
pub async fn time<F, Out>(func: F) -> Result<(SystemTime,u64,Out)>
  where
  F: Future<Output = Out>, {

  let start = SystemTime::now();

  let ret = func.await;

  let duration_ms = match start.elapsed() {
    Ok(dur) => dur,
    Err(e) => anyhow::bail!("timer error recording invocation duration: '{}'", e),
  }.as_millis() as u64;

  return Ok( (start, duration_ms, ret) );
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
  pub json: HelloResult,
  pub duration_ms: u64
}
#[derive(Serialize,Deserialize)]
pub struct RegistrationResult {
  pub duration_ms: u64,
  pub result: String
}

// {"body": {"greeting": greeting, "cold":was_cold, "start":start, "end":end, "latency": end-start} }
#[derive(Serialize,Deserialize)]
pub struct HelloResult {
  pub body: Body
}
#[derive(Serialize,Deserialize)]
pub struct Body {
  // pub greeting: String,
  pub cold: bool,
  pub start: f64,
  pub end: f64,
  pub latency: f64,
}
