use std::collections::BinaryHeap;
use std::sync::Arc;
use crate::worker_api::worker_config::InvocationConfig;
use anyhow::Result;
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::sync::Semaphore;
use tracing::debug;
use super::{InvokerQueuePolicy, create_concurrency_semaphore};
use super::invoker_structs::MinHeapEnqueuedInvocation;
use super::invoker_structs::EnqueuedInvocation;

pub struct FCFSInvoker {
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapEnqueuedInvocation<OffsetDateTime>>>>,
  concurrency_semaphore: Arc<Semaphore>,
}

impl FCFSInvoker {
  pub fn new(invocation_config: Arc<InvocationConfig>) -> Result<Arc<Self>> {
    let svc = Arc::new(FCFSInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?.ok_or(anyhow::anyhow!("Must provide `concurrent_invokes`"))?,
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
    });
    Ok(svc)
  }
}

#[tonic::async_trait]
impl InvokerQueuePolicy for FCFSInvoker {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    Some(r.item.clone())
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.item.clone();
    let mut func_name = "empty"; 
    if let Some(e) = invoke_queue.peek() {
      func_name = e.item.registration.function_name.as_str();
    }
    debug!(tid=%v.tid,  "Popped item from queue fcfs heap - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.registration.function_name, func_name );
    v
  }
  fn queue_len(&self) -> usize { self.invoke_queue.lock().len() }
  fn concurrency_semaphore(&self) -> Option<&Arc<Semaphore>> { Some(&self.concurrency_semaphore) }
  fn bypass_running(&self) -> Option<&std::sync::atomic::AtomicU32> { None }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new(item.clone(), item.queue_insert_time));
  }
}
