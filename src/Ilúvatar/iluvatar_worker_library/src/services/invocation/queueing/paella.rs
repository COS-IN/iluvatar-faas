use super::{EnqueuedInvocation, MinHeapFloat};
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use crate::utils::characteristics_map::CharacteristicsMap;
use anyhow::Result;
use dashmap::mapref::multiple::RefMutMulti;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc},
};

/// Combines invocations into batches, and returned the batch with the oldest item in front
pub struct PaellaGpuQueue {
    invoke_batches: DashMap<String, FunctionDetail>,
    est_time: Mutex<f64>,
    num_queued: AtomicUsize,
    cmap: Arc<CharacteristicsMap>,
    fairness_thres: f64,
}

pub struct FunctionDetail {
    pub queue: VecDeque<MinHeapFloat>,
    pub registration: Arc<RegisteredFunction>,
    pub deficit: f64,
}
impl FunctionDetail {
    pub fn new(registration: Arc<RegisteredFunction>) -> Self {
        Self {
            queue: VecDeque::new(),
            deficit: 0.0,
            registration,
        }
    }
}

impl PaellaGpuQueue {
    pub fn new(cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
        let svc = Arc::new(Self {
            invoke_batches: DashMap::new(),
            est_time: Mutex::new(0.0),
            num_queued: AtomicUsize::new(0),
            cmap,
            fairness_thres: 25.0,
        });
        Ok(svc)
    }

    fn next(&self) -> Option<RefMutMulti<'_, String, FunctionDetail>> {
        let mut min_t = 1000000000.0;
        let mut min_q = None;
        for que in self.invoke_batches.iter_mut() {
            if let Some(item) = que.queue.front() {
                if que.deficit >= self.fairness_thres {
                    min_q = Some(que);
                    break;
                }
                if min_t > item.est_wall_time {
                    min_t = item.est_wall_time;
                    min_q = Some(que);
                }
            }
        }
        min_q
    }
}

#[tonic::async_trait]
impl GpuQueuePolicy for PaellaGpuQueue {
    fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
        let mut min_t = 1000000000.0;
        let mut min_q = None;
        for que in self.invoke_batches.iter() {
            if let Some(item) = que.queue.front() {
                if que.deficit >= self.fairness_thres {
                    min_q = Some(que);
                    break;
                }
                if min_t > item.est_wall_time {
                    min_t = item.est_wall_time;
                    min_q = Some(que);
                }
            }
        }
        min_q.map(|q| q.registration.clone())
    }

    fn pop_queue(&self) -> Option<GpuBatch> {
        let len = self.invoke_batches.len() as f64;
        let mut q = self.next()?;
        q.deficit -= 1.0 - (1.0 / len);
        let item = q.queue.pop_front().unwrap();
        drop(q);
        for mut que in self.invoke_batches.iter_mut() {
            if que.registration.fqdn != item.item.registration.fqdn {
                que.deficit += 1.0 / len;
            }
        }
        *self.est_time.lock() -= item.est_wall_time;
        self.num_queued.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        Some(GpuBatch::new(item.item, 0.0))
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
        let est_time = self.cmap.get_gpu_exec_time(&item.registration.fqdn);
        let que = MinHeapFloat::new_f(item.clone(), est_time, est_time);
        match self.invoke_batches.entry(item.registration.fqdn.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut v) => {
                let q = v.get_mut();
                q.queue.push_back(que);
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let mut q = FunctionDetail::new(item.registration.clone());
                q.queue.push_back(que);
                e.insert(q);
            }
        }
        *self.est_time.lock() += est_time;
        Ok(())
    }
}
