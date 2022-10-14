use std::sync::Arc;
use iluvatar_library::{transaction::TransactionId, bail_error};
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use anyhow::Result;

#[derive(Debug)]
#[allow(unused)]
pub struct InvocationResult {
  pub result_json: String,
  pub duration: u64,
  pub attempts: u32,
  pub completed: bool
}
impl InvocationResult {
  pub fn boxed() -> InvocationResultPtr {
    Arc::new(Mutex::new(InvocationResult {
      completed: false,
      duration: 0,
      result_json: "".to_string(),
      attempts: 0
    }))
  }
}

pub type InvocationResultPtr = Arc<Mutex<InvocationResult>>;

#[derive(Debug)]
#[allow(unused)]
pub struct EnqueuedInvocation {
  pub result_ptr: InvocationResultPtr,
  pub function_name: String, 
  pub function_version: String, 
  pub json_args: String, 
  pub tid: TransactionId,
  signal: Semaphore,
}

impl EnqueuedInvocation {
  pub fn new(function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Self {
    EnqueuedInvocation {
      result_ptr: InvocationResult::boxed(),
      function_name,
      function_version,
      json_args,
      tid,
      signal: Semaphore::new(0)
    }
  }

  pub async fn wait(&self, tid: &TransactionId) -> Result<()> {
    match self.signal.acquire().await {
      Ok(_) => Ok(()),
      Err(e) => bail_error!(tid=%tid, error=%e, "Failed to wait on enqueued invocation signal due to an error"),
    }
  }

  pub fn signal(&self) {
    self.signal.add_permits(1);
  }
}
