use std::{time::SystemTime, future::Future};
use anyhow::Result;

///
pub async fn time<F, Out>(func: F) -> Result<(SystemTime,u64,Out)>
  where
  F: Future<Output = Out>, {

  let start = SystemTime::now();

  let ret = func.await;

  let duration = match start.elapsed() {
    Ok(dur) => dur,
    Err(e) => anyhow::bail!("timer error recording invocation duration '{}'", e),
  }.as_millis() as u64;

  return Ok( (start, duration, ret) );
}

#[allow(unused)]
pub struct ThreadError {
  thread_id: u64,
  error: anyhow::Error
}