use std::sync::Arc;
use crate::services::containers::{structs::ContainerState, containermanager::ContainerManager};
use iluvatar_library::{transaction::TransactionId, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use parking_lot::Mutex;
use tracing::debug;
use crate::services::invocation::{InvokerQueuePolicy, EnqueuedInvocation, MinHeapEnqueuedInvocation, MinHeapFloat};
use std::collections::BinaryHeap;

/// An invoker that scales concurrency based on system load
/// Prioritizes based on container availability
pub struct AvailableScalingQueue {
  cont_manager: Arc<ContainerManager>,
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
  cmap: Arc<CharacteristicsMap>,
  est_time: Mutex<f64>
}

impl AvailableScalingQueue {
  pub fn new(cont_manager: Arc<ContainerManager>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let svc = Arc::new(AvailableScalingQueue {
      cont_manager,
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
      est_time: Mutex::new(0.0),
    });
    debug!(tid=%tid, "Created AvailableScalingInvoker");
    Ok(svc)
  }
}

impl InvokerQueuePolicy for AvailableScalingQueue {
  fn queue_len(&self) -> usize { 
    self.invoke_queue.lock().len()
  }
  fn est_queue_time(&self) -> f64 { 
    *self.est_time.lock() 
  }
  
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

  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
    *self.est_time.lock() += item.est_execution_time;
    let mut priority = 0.0;
    if self.cont_manager.outstanding(&item.registration.fqdn) == 0 {
      priority = self.cmap.get_warm_time(&item.registration.fqdn);
    }
    priority = match self.cont_manager.container_available(&item.registration.fqdn, iluvatar_library::types::Compute::CPU)? {
      ContainerState::Warm => priority,
      ContainerState::Prewarm => self.cmap.get_prewarm_time(&item.registration.fqdn),
      _ => self.cmap.get_cold_time(&item.registration.fqdn),
    };
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), priority).into());
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.fqdn,
                        queue.peek().unwrap().item.registration.fqdn );
    Ok(())
  }
}
