use std::{sync::Arc, time::Duration};
use iluvatar_library::{transaction::TransactionId};
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::sync::Notify;
use anyhow::Result;

#[derive(Debug)]
#[allow(unused)]
pub struct InvocationResult {
  /// The output from the invocation
  pub result_json: String,
  /// The invocation time as tracked by the worker
  pub duration: Duration,
  pub attempts: u32,
  pub completed: bool
}
impl InvocationResult {
  pub fn boxed() -> InvocationResultPtr {
    Arc::new(Mutex::new(InvocationResult {
      completed: false,
      duration: Duration::from_micros(0),
      result_json: "".to_string(),
      attempts: 0
    }))
  }
}

pub type InvocationResultPtr = Arc<Mutex<InvocationResult>>;

#[derive(Debug)]
/// A struct to hold a function while it is in the invocation queue
pub struct EnqueuedInvocation {
  pub fqdn: String,
  /// Pointer where results will be stored on invocation completion
  pub result_ptr: InvocationResultPtr,
  pub function_name: String, 
  pub function_version: String, 
  pub json_args: String, 
  pub tid: TransactionId,
  signal: Notify,
  /// The local time at which the item was inserted into the queue
  pub queue_insert_time: OffsetDateTime,
}

impl EnqueuedInvocation {
  pub fn new(fqdn: String, function_name: String, function_version: String, json_args: String, tid: TransactionId, queue_insert_time: OffsetDateTime) -> Self {
    EnqueuedInvocation {
      fqdn,
      result_ptr: InvocationResult::boxed(),
      function_name,
      function_version,
      json_args,
      tid,
      signal: Notify::new(),
      queue_insert_time
    }
  }

  /// Block the current thread until the invocation is complete
  /// Only one waiter on an invocation is currently supported
  /// If the invocation is complete, this will return immediately
  pub async fn wait(&self, _tid: &TransactionId) -> Result<()> {
    self.signal.notified().await;
    Ok(())
  }

  /// Signal the current waiter to wake up because the invocation result is available
  /// If no one is waiting, the next person to check will continue immediately
  pub fn signal(&self) {
    self.signal.notify_one();
  }
}
