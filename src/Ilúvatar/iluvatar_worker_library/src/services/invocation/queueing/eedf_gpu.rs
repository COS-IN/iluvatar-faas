use super::{EnqueuedInvocation, MinHeapFloat};
use crate::services::containers::{containermanager::ContainerManager, structs::ContainerState};
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::clock::now;
use parking_lot::Mutex;
use std::{collections::BinaryHeap, sync::Arc};
use tokio::time::Instant;

/// A queue for GPU invocations
/// Items are returned in Earliest Expected Deadline First
pub struct EedfGpuQueue {
    invoke_queue: Mutex<BinaryHeap<MinHeapFloat>>,
    est_time: Mutex<f64>,
    cmap: Arc<CharacteristicsMap>,
    cont_manager: Arc<ContainerManager>,
    creation: Instant,
}

impl EedfGpuQueue {
    pub fn new(cont_manager: Arc<ContainerManager>, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
        let svc = Arc::new(Self {
            invoke_queue: Mutex::new(BinaryHeap::new()),
            est_time: Mutex::new(0.0),
            cmap,
            cont_manager,
            creation: now(),
        });
        Ok(svc)
    }
    fn time_since_creation(&self) -> f64 {
        now().duration_since(self.creation).as_secs_f64()
    }
}

impl GpuQueuePolicy for EedfGpuQueue {
    fn queue_len(&self) -> usize {
        self.invoke_queue.lock().len()
    }
    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }
    fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
        let r = self.invoke_queue.lock();
        if let Some(item) = r.peek() {
            return Some(item.item.registration.clone());
        }
        None
    }
    fn pop_queue(&self) -> Option<GpuBatch> {
        let mut invoke_queue = self.invoke_queue.lock();
        let batch = invoke_queue.pop().unwrap();
        *self.est_time.lock() -= batch.est_wall_time;
        Some(GpuBatch::new(batch.item, batch.est_wall_time))
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        let est_time = match self
            .cont_manager
            .container_available(&item.registration.fqdn, iluvatar_library::types::Compute::GPU)
        {
            ContainerState::Warm => self.cmap.get_gpu_warm_time(&item.registration.fqdn),
            ContainerState::Prewarm => self.cmap.get_gpu_warm_time(&item.registration.fqdn),
            _ => self.cmap.get_gpu_cold_time(&item.registration.fqdn),
        };

        let mut queue = self.invoke_queue.lock();
        *self.est_time.lock() += est_time;
        let deadline = self.cmap.get_exec_time(&item.registration.fqdn) + self.time_since_creation();
        queue.push(MinHeapFloat::new_f(item.clone(), deadline, est_time));
        Ok(())
    }
}
