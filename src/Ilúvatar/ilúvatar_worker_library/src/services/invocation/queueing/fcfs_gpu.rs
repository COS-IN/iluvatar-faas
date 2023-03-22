use std::{collections::VecDeque, sync::Arc};
use anyhow::Result;
use parking_lot::Mutex;
use crate::services::{invocation::gpu_q_invoke::{GpuQueuePolicy, GpuBatch}, registration::RegisteredFunction};
use super::EnqueuedInvocation;

/// A queue for GPU invocations
/// Items are returned in a FCFS manner, with no coalescing into batches unless they line up
pub struct FcfsGpuQueue {
  invoke_queue: Mutex<VecDeque<GpuBatch>>,
  est_time: Mutex<f64>
}

impl FcfsGpuQueue {
  pub fn new() -> Result<Arc<Self>> {
    let svc = Arc::new(FcfsGpuQueue {
      invoke_queue: Mutex::new(VecDeque::new()),
      est_time: Mutex::new(0.0),
    });
    Ok(svc)
  }
}

#[tonic::async_trait]
impl GpuQueuePolicy for FcfsGpuQueue {
  fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
    let r = self.invoke_queue.lock();
    if let Some(item) = r.front() {
      return Some(item.item_registration().clone());
    }
    None
  }
  fn pop_queue(&self) -> GpuBatch {
    let mut invoke_queue = self.invoke_queue.lock();
    let batch = invoke_queue.pop_front().unwrap();
    *self.est_time.lock() -= batch.est_queue_time();
    batch
  }
  fn queue_len(&self) -> usize { self.invoke_queue.lock().len() }
  fn est_queue_time(&self) -> f64 { 
    *self.est_time.lock() 
  }
  
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
    let mut queue = self.invoke_queue.lock();
    let mut est_time = self.est_time.lock();
    *est_time += item.est_execution_time;
    if let Some(back_item) = queue.back_mut() {
      if back_item.item_registration().fqdn == item.registration.fqdn {
        back_item.add(item.clone());
        return Ok(());
      }
    }
    queue.push_back(GpuBatch::new(item.clone()));
    Ok(())
  }
}
