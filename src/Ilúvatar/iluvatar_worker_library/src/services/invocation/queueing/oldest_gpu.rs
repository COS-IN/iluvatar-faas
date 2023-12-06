use super::EnqueuedInvocation;
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use parking_lot::Mutex;
use std::sync::{atomic::AtomicUsize, Arc};

/// Combines invocations into batches, and returned the batch with the oldest item in front
pub struct BatchGpuQueue {
    invoke_batches: DashMap<String, GpuBatch>,
    est_time: Mutex<f64>,
    num_queued: AtomicUsize,
    cmap: Arc<CharacteristicsMap>,
}

impl BatchGpuQueue {
    pub fn new(cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
        let svc = Arc::new(BatchGpuQueue {
            invoke_batches: DashMap::new(),
            est_time: Mutex::new(0.0),
            num_queued: AtomicUsize::new(0),
            cmap,
        });
        Ok(svc)
    }
}

#[tonic::async_trait]
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
    fn pop_queue(&self) -> GpuBatch {
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
        batch
    }
    fn queue_len(&self) -> usize {
        self.num_queued.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.num_queued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let est_time;
        match self.invoke_batches.entry(item.registration.fqdn.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut v) => {
                est_time = self.cmap.get_gpu_exec_time(&item.registration.fqdn);
                v.get_mut().add(item.clone(), est_time);
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                est_time = self.cmap.get_gpu_cold_time(&item.registration.fqdn);
                e.insert(GpuBatch::new(item.clone(), est_time));
            }
        }
        *self.est_time.lock() += est_time;
        Ok(())
    }
}

#[cfg(test)]
mod oldest_batch {
    use super::*;
    use iluvatar_library::characteristics_map::{Characteristics, Values};
    use time::OffsetDateTime;

    fn reg(name: &str) -> Arc<RegisteredFunction> {
        Arc::new(RegisteredFunction {
            function_name: name.to_string(),
            function_version: name.to_string(),
            fqdn: name.to_string(),
            image_name: name.to_string(),
            memory: 1,
            cpus: 1,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: iluvatar_library::types::Isolation::CONTAINERD,
            supported_compute: iluvatar_library::types::Compute::CPU,
        })
    }

    #[test]
    fn single_item_cold() {
        let m = CharacteristicsMap::new(iluvatar_library::characteristics_map::AgExponential::new(0.6));
        let name = "t1";
        let rf = reg(name);

        let invoke = Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            OffsetDateTime::now_utc(),
        ));
        m.add(
            &invoke.registration.fqdn,
            Characteristics::GpuColdTime,
            Values::F64(1.5),
            true,
        );

        let b = BatchGpuQueue::new(Arc::new(m)).unwrap();
        b.add_item_to_queue(&invoke).unwrap();

        assert_eq!(b.est_queue_time(), 1.5);
    }

    #[test]
    fn two_item_mix() {
        let m = CharacteristicsMap::new(iluvatar_library::characteristics_map::AgExponential::new(0.6));
        let name = "t1";
        let rf = reg(name);
        let invoke = Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            OffsetDateTime::now_utc(),
        ));
        m.add(
            &invoke.registration.fqdn,
            Characteristics::GpuColdTime,
            Values::F64(1.5),
            true,
        );
        m.add(
            &invoke.registration.fqdn,
            Characteristics::GpuExecTime,
            Values::F64(1.0),
            true,
        );

        let b = BatchGpuQueue::new(Arc::new(m)).unwrap();
        b.add_item_to_queue(&invoke).unwrap();
        b.add_item_to_queue(&invoke).unwrap();

        assert_eq!(b.est_queue_time(), 2.5);
    }

    #[test]
    fn two_func_mix() {
        let m = CharacteristicsMap::new(iluvatar_library::characteristics_map::AgExponential::new(0.6));
        let name = "t1";
        let rf = reg(name);
        let invoke = Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            OffsetDateTime::now_utc(),
        ));
        m.add(
            &invoke.registration.fqdn,
            Characteristics::GpuColdTime,
            Values::F64(1.5),
            true,
        );
        m.add(
            &invoke.registration.fqdn,
            Characteristics::GpuExecTime,
            Values::F64(1.0),
            true,
        );

        let name = "t2";
        let rf2 = reg(name);
        let invoke2 = Arc::new(EnqueuedInvocation::new(
            rf2,
            name.to_string(),
            name.to_string(),
            OffsetDateTime::now_utc(),
        ));
        m.add(
            &invoke2.registration.fqdn,
            Characteristics::GpuColdTime,
            Values::F64(0.9),
            true,
        );
        m.add(
            &invoke2.registration.fqdn,
            Characteristics::GpuExecTime,
            Values::F64(0.3),
            true,
        );

        let b = BatchGpuQueue::new(Arc::new(m)).unwrap();
        b.add_item_to_queue(&invoke).unwrap();
        b.add_item_to_queue(&invoke).unwrap();

        b.add_item_to_queue(&invoke2).unwrap();
        b.add_item_to_queue(&invoke2).unwrap();

        assert_eq!(b.est_queue_time(), 2.5 + 0.9 + 0.3);
    }
}
