use super::EnqueuedInvocation;
use crate::services::containers::{containermanager::ContainerManager, structs::ContainerState};
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};

/// A queue for GPU invocations
/// Items are returned in a FCFS manner, with no coalescing into batches unless they line up
pub struct FcfsGpuQueue {
    invoke_queue: Mutex<VecDeque<GpuBatch>>,
    est_time: Mutex<f64>,
    cmap: WorkerCharMap,
    cont_manager: Arc<ContainerManager>,
}

impl FcfsGpuQueue {
    pub fn new(cont_manager: Arc<ContainerManager>, cmap: WorkerCharMap) -> Result<Arc<Self>> {
        let svc = Arc::new(FcfsGpuQueue {
            invoke_queue: Mutex::new(VecDeque::new()),
            est_time: Mutex::new(0.0),
            cmap,
            cont_manager,
        });
        Ok(svc)
    }
}

impl GpuQueuePolicy for FcfsGpuQueue {
    fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
        let r = self.invoke_queue.lock();
        if let Some(item) = r.front() {
            return Some(item.item_registration().clone());
        }
        None
    }
    fn pop_queue(&self) -> Option<GpuBatch> {
        let mut invoke_queue = self.invoke_queue.lock();
        let batch = invoke_queue.pop_front().unwrap();
        *self.est_time.lock() -= batch.est_queue_time();
        Some(batch)
    }
    fn queue_len(&self) -> usize {
        self.invoke_queue.lock().len()
    }
    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item), fields(tid=item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        let est_time = match self
            .cont_manager
            .container_available(&item.registration.fqdn, iluvatar_library::types::Compute::GPU)
        {
            ContainerState::Warm => self.cmap.get_avg(&item.registration.fqdn, Chars::GpuWarmTime),
            ContainerState::Prewarm => self.cmap.get_avg(&item.registration.fqdn, Chars::GpuPreWarmTime),
            _ => self.cmap.get_avg(&item.registration.fqdn, Chars::GpuColdTime),
        };

        let mut queue = self.invoke_queue.lock();
        if let Some(back_item) = queue.back_mut() {
            if back_item.item_registration().fqdn == item.registration.fqdn {
                let est_time = self.cmap.get_avg(&item.registration.fqdn, Chars::GpuExecTime);
                back_item.add(item.clone(), est_time);
                *self.est_time.lock() += est_time;
                return Ok(());
            }
        }
        *self.est_time.lock() += est_time;
        queue.push_back(GpuBatch::new(item.clone(), est_time));
        Ok(())
    }
}
