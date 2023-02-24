use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::worker_api::worker_config::InvocationConfig;
use iluvatar_library::{transaction::TransactionId, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use parking_lot::Mutex;
use tracing::debug;
use super::InvokerQueuePolicy;
use super::invoker_structs::{EnqueuedInvocation, MinHeapEnqueuedInvocation, MinHeapFloat};
use std::collections::BinaryHeap;

fn time_since_epoch() -> f64 {
    let start = SystemTime::now();
    start.duration_since( UNIX_EPOCH )
         .expect("Time went backwards")
         .as_secs_f64()
}

pub struct MinHeapEDBPQueue {
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
  cmap: Arc<CharacteristicsMap>,
  bypass_running: AtomicU32
}

impl MinHeapEDBPQueue {
  pub fn new(invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    if let Some(bp) = invocation_config.bypass_duration_ms {
      if bp <= 0 {
        anyhow::bail!("Cannot have a bypass_duration_ms of zero for this invoker queue policy")
      }
    }
    let svc = Arc::new(MinHeapEDBPQueue {
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
      bypass_running: AtomicU32::new(0)
    });
    debug!(tid=%tid, "Created MinHeapEDBPInvoker");
    Ok(svc)
  }
}

#[tonic::async_trait]
impl InvokerQueuePolicy for MinHeapEDBPQueue {
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
    debug!(tid=%v.tid,  component="minheap", "Popped item from queue minheap - len: {} popped: {} top: {} ",
           invoke_queue.len(),
           v.registration.function_name,
           func_name );
    v
  }

  fn queue_len(&self) -> usize {
    self.invoke_queue.lock().len()
  }
  fn bypass_running(&self) -> Option<&AtomicU32> { Some(&self.bypass_running) }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    let deadline = self.cmap.get_exec_time(&item.registration.fqdn) + time_since_epoch();
    queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), deadline ));
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.function_name,
                        queue.peek().unwrap().item.registration.function_name );
  }
}
