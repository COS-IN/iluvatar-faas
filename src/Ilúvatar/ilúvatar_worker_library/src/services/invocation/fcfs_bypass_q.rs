use std::sync::{atomic::AtomicU32, Arc};
use crate::worker_api::worker_config::InvocationConfig;
use anyhow::Result;
use iluvatar_library::transaction::TransactionId;
use parking_lot::Mutex;
use time::OffsetDateTime;
use tracing::{debug, info};
use super::InvokerQueuePolicy;
use super::invoker_structs::{MinHeapEnqueuedInvocation, EnqueuedInvocation};
use std::collections::BinaryHeap;

/// A First-Come-First-Serve queue management policy
/// Items with an execution duration of less than [InvocationConfig::bypass_duration_ms] will skip the queue
/// In the event of a cold start on such an invocation, it will be enqueued
pub struct FCFSBypassQueue {
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapEnqueuedInvocation<OffsetDateTime>>>>,
  bypass_running: AtomicU32
}

impl FCFSBypassQueue {
  pub fn new(invocation_config: Arc<InvocationConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    if let Some(bp) = invocation_config.bypass_duration_ms {
      if bp <= 0 {
        anyhow::bail!("Cannot have a bypass_duration_ms of zero for this invoker queue policy")
      }
    }
    let svc = Arc::new(FCFSBypassQueue {
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      bypass_running: AtomicU32::new(0)
    });
    info!(tid=%tid, "Created FCFSBypassInvoker");
    Ok(svc)
  }
}

#[tonic::async_trait]
impl InvokerQueuePolicy for FCFSBypassQueue {
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

  fn queue_len(&self) -> usize {
    self.invoke_queue.lock().len()
  }
  fn bypass_running(&self) -> Option<&AtomicU32> { Some(&self.bypass_running) }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new(item.clone(), item.queue_insert_time));
  }
}
