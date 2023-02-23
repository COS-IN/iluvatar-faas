use std::sync::Arc;
use crate::services::containers::structs::ContainerState;
use crate::services::invocation::create_concurrency_semaphore;
use crate::worker_api::worker_config::InvocationConfig;
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::TransactionId, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::debug;
use super::InvokerQueuePolicy;
use super::invoker_structs::{EnqueuedInvocation, MinHeapEnqueuedInvocation, MinHeapFloat};
use std::collections::BinaryHeap;

pub struct ColdPriorityInvoker {
  cont_manager: Arc<ContainerManager>,
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
  cmap: Arc<CharacteristicsMap>,
  concurrency_semaphore: Arc<Semaphore>,
}

impl ColdPriorityInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let svc = Arc::new(ColdPriorityInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?.ok_or(anyhow::anyhow!("Must provide `concurrent_invokes`"))?,
      cont_manager,
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
    });
    debug!(tid=%tid, "Created ColdPriorityInvoker");
    Ok(svc)
  }
}

impl InvokerQueuePolicy for ColdPriorityInvoker {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    let r = r.item.clone();
    return Some(r);
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.item.clone();
    let top = invoke_queue.peek();
    let func_name;
    match top {
        Some(e) => func_name = e.item.registration.function_name.as_str(),
        None => func_name = "empty"
    }
    debug!(tid=%v.tid,  component="minheap", "Popped item from queue - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.registration.function_name, func_name );
    v
  }

  fn queue_len(&self) -> usize {
    self.invoke_queue.lock().len()
  }
  fn concurrency_semaphore(&self) -> Option<&Arc<Semaphore>> {
    Some(&self.concurrency_semaphore)
  }

  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut priority = 0.0;
    if self.cont_manager.outstanding(Some(&item.registration.fqdn)) == 0 {
      priority = self.cmap.get_warm_time(&item.registration.fqdn);
    }
    priority = match self.cont_manager.container_available(&item.registration.fqdn) {
      ContainerState::Warm => priority,
      ContainerState::Prewarm => self.cmap.get_prewarm_time(&item.registration.fqdn),
      _ => self.cmap.get_cold_time(&item.registration.fqdn),
    };
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), priority));
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.fqdn,
                        queue.peek().unwrap().item.registration.fqdn );
  }
}
