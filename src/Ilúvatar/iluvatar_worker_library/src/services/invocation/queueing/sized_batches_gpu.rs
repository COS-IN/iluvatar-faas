use super::EnqueuedInvocation;
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc},
};

/// Combines invocations into batches, and returned the batch with the oldest item in front
pub struct SizedBatchGpuQueue {
    invoke_batches: DashMap<String, VecDeque<GpuBatch>>,
    est_time: Mutex<f64>,
    num_queued: AtomicUsize,
    cmap: Arc<CharacteristicsMap>,
    max_batch_size: usize,
}

impl SizedBatchGpuQueue {
    pub fn new(cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
        let svc = Arc::new(Self {
            invoke_batches: DashMap::new(),
            est_time: Mutex::new(0.0),
            num_queued: AtomicUsize::new(0),
            cmap,
            max_batch_size: 10,
        });
        Ok(svc)
    }
}

impl GpuQueuePolicy for SizedBatchGpuQueue {
    fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
        let mut ret_min = time::PrimitiveDateTime::MAX.assume_utc();
        let mut ret = None;
        for batch in self.invoke_batches.iter() {
            if !batch.value().is_empty() {
                if let Some(b) = batch.value().front() {
                    if b.peek().queue_insert_time < ret_min {
                        ret_min = b.peek().queue_insert_time;
                        ret = Some(b.item_registration().clone());
                    }
                }
            }
        }
        ret
    }

    fn pop_queue(&self) -> Option<GpuBatch> {
        let mut ret_min = time::PrimitiveDateTime::MAX.assume_utc();
        let mut ret = None;
        for batch in self.invoke_batches.iter_mut() {
            if !batch.value().is_empty() {
                if let Some(b) = batch.value().front() {
                    if b.peek().queue_insert_time < ret_min {
                        ret_min = b.peek().queue_insert_time;
                        ret = Some(batch);
                    }
                }
            }
        }
        let batch = ret?.value_mut().pop_front()?;
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

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.num_queued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let est_time;
        match self.invoke_batches.entry(item.registration.fqdn.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut v) => {
                est_time = self.cmap.get_gpu_exec_time(&item.registration.fqdn);
                let q = v.get_mut();
                if let Some(b) = q.back_mut() {
                    if b.len() >= self.max_batch_size {
                        q.push_back(GpuBatch::new(item.clone(), est_time));
                    } else {
                        b.add(item.clone(), est_time);
                    }
                } else {
                    q.push_back(GpuBatch::new(item.clone(), est_time));
                }
            },
            dashmap::mapref::entry::Entry::Vacant(e) => {
                est_time = self.cmap.get_gpu_cold_time(&item.registration.fqdn);
                let mut q = VecDeque::new();
                q.push_back(GpuBatch::new(item.clone(), est_time));
                e.insert(q);
            },
        }
        *self.est_time.lock() += est_time;
        Ok(())
    }
}

#[cfg(test)]
mod oldest_batch {
    use super::*;
    use iluvatar_library::characteristics_map::{Characteristics, Values};
    use iluvatar_library::clock::get_global_clock;
    use iluvatar_library::transaction::gen_tid;
    use std::collections::HashMap;

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
            historical_runtime_data_sec: HashMap::new(),
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
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
        ));
        m.add(
            &invoke.registration.fqdn,
            Characteristics::GpuColdTime,
            Values::F64(1.5),
            true,
        );

        let b = SizedBatchGpuQueue::new(Arc::new(m)).unwrap();
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
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
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

        let b = SizedBatchGpuQueue::new(Arc::new(m)).unwrap();
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
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
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
            get_global_clock(&gen_tid()).unwrap().now(),
            0.0,
            0.0,
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

        let b = SizedBatchGpuQueue::new(Arc::new(m)).unwrap();
        b.add_item_to_queue(&invoke).unwrap();
        b.add_item_to_queue(&invoke).unwrap();

        b.add_item_to_queue(&invoke2).unwrap();
        b.add_item_to_queue(&invoke2).unwrap();

        assert_eq!(b.est_queue_time(), 2.5 + 0.9 + 0.3);
    }
}
