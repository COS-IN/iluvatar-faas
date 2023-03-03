use std::sync::Arc;
use anyhow::Result;
use tracing::debug;
use super::{invoker_structs::EnqueuedInvocation, InvokerQueuePolicy};

/// This implementation does not support [crate::worker_api::worker_config::InvocationConfig::concurrent_invokes]
pub struct Queueless {
  async_queue: parking_lot::Mutex<Vec<Arc<EnqueuedInvocation>>>,
}
impl Queueless {
  pub fn new() -> Result<Arc<Self>> {
    let svc = Arc::new(Queueless {
      async_queue: parking_lot::Mutex::new(Vec::new()),
    });
    Ok(svc)
  }
}

#[tonic::async_trait]
#[allow(dyn_drop)]
impl InvokerQueuePolicy for Queueless {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let q = self.async_queue.lock();
    if let Some(r) = q.get(0) {
      return Some(r.clone())
    }
    None
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    self.async_queue.lock().remove(0)
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.async_queue.lock();
    queue.push(item.clone());
    debug!(tid=%item.tid, "Added item to front of queue; waking worker thread");
  }
}
