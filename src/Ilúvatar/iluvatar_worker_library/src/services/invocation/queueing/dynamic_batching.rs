use super::EnqueuedInvocation;
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::{atomic::AtomicUsize, Arc};

/// Combines invocations into batches, and returned the batch with the oldest item in front
#[allow(unused)]
pub struct DynBatchGpuQueue {
    /// Grouped invocations of same function ready for dispatch.
    invoke_batches: DashMap<String, GpuBatch>,
    est_time: Mutex<f64>,
    num_queued: AtomicUsize,
    cmap: WorkerCharMap,
    /// For preventing starvation: dont want super large batches which dominate execution.
    max_batch_size: i32,
    /// Number of invocations we want to batch together at the head of the dispatch queue. The tail is uncompressed and regular FCFS queue.
    compress_window: i32,
    /// Initially, just add invocations in an FCFS queue which is later compressed into batches.
    incoming_queue: Mutex<VecDeque<Arc<EnqueuedInvocation>>>, // from FcfsGpuQueue
}
#[allow(unused)]
impl DynBatchGpuQueue {
    pub fn new(cmap: WorkerCharMap) -> Result<Arc<Self>> {
        let svc = Arc::new(DynBatchGpuQueue {
            invoke_batches: DashMap::new(),
            est_time: Mutex::new(0.0),
            num_queued: AtomicUsize::new(0),
            max_batch_size: 4,
            compress_window: 4,
            incoming_queue: Mutex::new(VecDeque::new()),
            cmap,
        });
        Ok(svc)
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item), fields(tid=item.tid)))]
    /// Will need to convert from registered function in the incoming queue to an enqueued invocation after compression.
    fn add_item_to_batches(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.num_queued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let est_time;
        match self.invoke_batches.entry(item.registration.fqdn.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut v) => {
                est_time = self.cmap.get_avg(&item.registration.fqdn, Chars::GpuExecTime);
                v.get_mut().add(item.clone(), est_time);
            },
            dashmap::mapref::entry::Entry::Vacant(e) => {
                est_time = self.cmap.get_avg(&item.registration.fqdn, Chars::GpuColdTime);
                e.insert(GpuBatch::new(item.clone(), est_time));
            },
        }
        *self.est_time.lock() += est_time;
        Ok(())
    }
}

impl GpuQueuePolicy for DynBatchGpuQueue {
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item), fields(tid=item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        let mut queue = self.incoming_queue.lock();
        queue.push_back(item.clone()); //cloning an Arc, hmm
        Ok(())
    }

    /// This will be dependent on the current GPU state (which functions are running etc)
    fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
        if let Some(next) = self
            .invoke_batches
            .iter()
            .min_by_key(|x| x.value().peek().queue_insert_time)
        {
            return Some(next.value().item_registration().clone());
        }
        None
    }

    /// The GPU may already be running functions and we may want to run a single invocation for more fine-grained scheduling.
    /// Ideally want to schedule individual functions. Batch as unit of execution seems too coarse-grained.
    /// Need approx snapshot of GPU state and capacity. What functions may be resident, memory, etc.
    fn pop_queue(&self) -> Option<GpuBatch> {
        let batch_key = self
            .invoke_batches
            .iter()
            .min_by_key(|x| x.value().peek().queue_insert_time)
            .unwrap()
            .key()
            .clone();
        let (_fqdn, batch) = self.invoke_batches.remove(&batch_key).unwrap();

        self.num_queued
            .fetch_sub(batch.len(), std::sync::atomic::Ordering::Relaxed);
        *self.est_time.lock() -= batch.est_queue_time();
        Some(batch)
    }

    fn queue_len(&self) -> usize {
        self.num_queued.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }
}
