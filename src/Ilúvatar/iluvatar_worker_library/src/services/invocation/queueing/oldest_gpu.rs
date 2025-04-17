use super::EnqueuedInvocation;
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use parking_lot::Mutex;
use std::sync::{atomic::AtomicUsize, Arc};

/// Combines invocations into batches, and returned the batch with the oldest item in front
pub struct BatchGpuQueue {
    invoke_batches: DashMap<String, GpuBatch>,
    est_time: Mutex<f64>,
    num_queued: AtomicUsize,
    cmap: WorkerCharMap,
}

impl BatchGpuQueue {
    pub fn new(cmap: WorkerCharMap) -> Result<Arc<Self>> {
        let svc = Arc::new(BatchGpuQueue {
            invoke_batches: DashMap::new(),
            est_time: Mutex::new(0.0),
            num_queued: AtomicUsize::new(0),
            cmap,
        });
        Ok(svc)
    }
}

impl GpuQueuePolicy for BatchGpuQueue {
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

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item), fields(tid=item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
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

#[cfg(test)]
mod oldest_batch {
    use super::*;
    use iluvatar_library::char_map::{worker_char_map, Chars};
    use iluvatar_library::clock::get_global_clock;
    use iluvatar_library::transaction::gen_tid;

    fn reg(name: &str) -> Arc<RegisteredFunction> {
        Arc::new(RegisteredFunction {
            function_name: name.to_string(),
            function_version: name.to_string(),
            fqdn: name.to_string(),
            image_name: name.to_string(),
            memory: 1,
            cpus: 1,
            parallel_invokes: 1,
            ..Default::default()
        })
    }

    #[test]
    fn single_item_cold() {
        let m = worker_char_map();
        let name = "t1";
        let rf = reg(name);

        let invoke = Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
        ));
        m.update(&invoke.registration.fqdn, Chars::GpuColdTime, 1.5);

        let b = BatchGpuQueue::new(m).unwrap();
        b.add_item_to_queue(&invoke).unwrap();

        assert_eq!(b.est_queue_time(), 1.5);
    }

    #[test]
    fn two_item_mix() {
        let m = worker_char_map();
        let name = "t1";
        let rf = reg(name);
        let invoke = Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
        ));
        m.update(&invoke.registration.fqdn, Chars::GpuColdTime, 1.5);
        m.update(&invoke.registration.fqdn, Chars::GpuExecTime, 1.0);

        let b = BatchGpuQueue::new(m).unwrap();
        b.add_item_to_queue(&invoke).unwrap();
        b.add_item_to_queue(&invoke).unwrap();

        assert_eq!(b.est_queue_time(), 2.5);
    }

    #[test]
    fn two_func_mix() {
        let m = worker_char_map();
        let name = "t1";
        let rf = reg(name);
        let invoke = Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
        ));
        m.update(&invoke.registration.fqdn, Chars::GpuColdTime, 1.5);
        m.update(&invoke.registration.fqdn, Chars::GpuExecTime, 1.0);

        let name = "t2";
        let rf2 = reg(name);
        let invoke2 = Arc::new(EnqueuedInvocation::new(
            rf2,
            name.to_string(),
            name.to_string(),
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
        ));
        m.update(&invoke2.registration.fqdn, Chars::GpuColdTime, 0.9);
        m.update(&invoke2.registration.fqdn, Chars::GpuExecTime, 0.3);

        let b = BatchGpuQueue::new(m).unwrap();
        b.add_item_to_queue(&invoke).unwrap();
        b.add_item_to_queue(&invoke).unwrap();

        b.add_item_to_queue(&invoke2).unwrap();
        b.add_item_to_queue(&invoke2).unwrap();

        assert_eq!(b.est_queue_time(), 2.5 + 0.9 + 0.3);
    }
}
