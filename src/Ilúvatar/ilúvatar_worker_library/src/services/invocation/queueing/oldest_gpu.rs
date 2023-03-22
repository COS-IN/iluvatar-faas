use std::sync::{Arc, atomic::AtomicUsize};
use anyhow::Result;
use dashmap::DashMap;
use parking_lot::Mutex;
use crate::services::{invocation::gpu_q_invoke::{GpuQueuePolicy, GpuBatch}, registration::RegisteredFunction};
use super::EnqueuedInvocation;

/// Combines invocations into batches, and returned the batch with the oldest item in front
pub struct BatchGpuQueue {
  invoke_batches: DashMap<String, GpuBatch>,
  est_time: Mutex<f64>,
  num_queued: AtomicUsize,
}

impl BatchGpuQueue {
  pub fn new() -> Result<Arc<Self>> {
    let svc = Arc::new(BatchGpuQueue {
      invoke_batches: DashMap::new(),
      est_time: Mutex::new(0.0),
      num_queued: AtomicUsize::new(0),
    });
    Ok(svc)
  }
}

#[tonic::async_trait]
impl GpuQueuePolicy for BatchGpuQueue {
  fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
    if let Some(next) = self.invoke_batches.iter().min_by_key(|x| x.value().peek().queue_insert_time) {
      return Some(next.value().item_registration().clone());
    }
    None
  }
  fn pop_queue(&self) -> GpuBatch {
    let batch_key = self.invoke_batches.iter().min_by_key(|x| x.value().peek().queue_insert_time).unwrap().key().clone();
    let (_fqdn, batch) = self.invoke_batches.remove(&batch_key).unwrap();

    self.num_queued.fetch_sub(batch.len(), std::sync::atomic::Ordering::Relaxed);

    *self.est_time.lock() -= batch.est_queue_time();
    batch
  }
  fn queue_len(&self) -> usize { self.num_queued.load(std::sync::atomic::Ordering::Relaxed) }
  fn est_queue_time(&self) -> f64 { 
    *self.est_time.lock() 
  }
  
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
    *self.est_time.lock() += item.est_execution_time;
    self.num_queued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match self.invoke_batches.entry(item.registration.fqdn.clone()) {
      dashmap::mapref::entry::Entry::Occupied(mut v) => v.get_mut().add(item.clone()),
      dashmap::mapref::entry::Entry::Vacant(e) => {e.insert(GpuBatch::new(item.clone()));},
    }
    Ok(())
  }
}
