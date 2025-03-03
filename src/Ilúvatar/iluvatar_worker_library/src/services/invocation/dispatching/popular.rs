use crate::services::invocation::dispatching::{
    queueing_dispatcher::DispatchPolicy, EnqueueingPolicy, QueueMap, NO_ESTIMATE,
};
use crate::services::registration::RegisteredFunction;
use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use ordered_float::OrderedFloat;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

pub fn get_popular(pol: EnqueueingPolicy, cmap: &WorkerCharMap, que_map: QueueMap) -> Result<Arc<dyn DispatchPolicy>> {
    match pol {
        EnqueueingPolicy::TopAvg => TopAvgDispatch::boxed(cmap),
        EnqueueingPolicy::LeastPopular => LeastPopularDispatch::boxed(),
        EnqueueingPolicy::Popular => PopularDispatch::boxed(),
        EnqueueingPolicy::PopularEstTimeDispatch => PopularEstTimeDispatch::boxed(que_map),
        EnqueueingPolicy::PopularQueueLenDispatch => PopularQueueLenDispatch::boxed(que_map),
        // popular policy not being used, give dummy basic policy
        _ => LeastPopularDispatch::boxed(),
    }
}

struct PopularityTracker {
    invokes: HashMap<String, u64>,
    total_invokes: u64,
}
impl PopularityTracker {
    pub fn new() -> Self {
        Self {
            invokes: HashMap::new(),
            total_invokes: 0,
        }
    }
    pub fn track(&mut self, fqdn: &str) -> f64 {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(fqdn) {
            true => {
                let val = self.invokes.get_mut(fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(fqdn.to_string(), 1);
                1
            },
        };
        invokes as f64 / self.total_invokes as f64
    }
}

pub struct TopAvgDispatch {
    /// Map of FQDN -> IAT
    iats: RwLock<HashMap<String, f64>>,
    cmap: WorkerCharMap,
}
impl TopAvgDispatch {
    pub fn boxed(cmap: &WorkerCharMap) -> Result<Arc<dyn DispatchPolicy>> {
        Ok(Arc::new(Self {
            iats: RwLock::new(HashMap::new()),
            cmap: cmap.clone(),
        }))
    }
}
impl DispatchPolicy for TopAvgDispatch {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        let iat = self.cmap.get_avg(&reg.fqdn, Chars::IAT);
        self.iats.write().insert(reg.fqdn.clone(), iat);
        let iats = self.iats.read().values().copied().collect::<Vec<f64>>();
        let avg = iats.iter().sum::<f64>() / iats.len() as f64;
        match iat > avg {
            true => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            false => (Compute::GPU, NO_ESTIMATE, NO_ESTIMATE),
        }
    }
}

/// Schedules the least popular functions to run on GPU
pub struct LeastPopularDispatch {
    track: Mutex<PopularityTracker>,
    popular_cutoff: f64,
}
impl LeastPopularDispatch {
    pub fn boxed() -> Result<Arc<dyn DispatchPolicy>> {
        Ok(Arc::new(Self {
            track: Mutex::new(PopularityTracker::new()),
            popular_cutoff: 0.15,
        }))
    }
}
impl DispatchPolicy for LeastPopularDispatch {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        let freq = self.track.lock().track(&reg.fqdn);
        match freq > self.popular_cutoff {
            true => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            false => (Compute::GPU, NO_ESTIMATE, NO_ESTIMATE),
        }
    }
}

/// Schedules the most popular functions to run on GPU
pub struct PopularDispatch {
    track: Mutex<PopularityTracker>,
    popular_cutoff: f64,
}
impl PopularDispatch {
    pub fn boxed() -> anyhow::Result<Arc<dyn DispatchPolicy>> {
        Ok(Arc::new(Self {
            track: Mutex::new(PopularityTracker::new()),
            popular_cutoff: 0.15,
        }))
    }
}
impl DispatchPolicy for PopularDispatch {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        let freq = self.track.lock().track(&reg.fqdn);
        match freq < self.popular_cutoff {
            true => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            false => (Compute::GPU, NO_ESTIMATE, NO_ESTIMATE),
        }
    }
}

/// Schedules the most popular functions to run on GPU.
/// Don't let them run if Q is long.
pub struct PopularQueueLenDispatch {
    track: Mutex<PopularityTracker>,
    popular_cutoff: f64,
    queue_len: usize,
    que_map: QueueMap,
}
impl PopularQueueLenDispatch {
    pub fn boxed(que_map: QueueMap) -> anyhow::Result<Arc<dyn DispatchPolicy>> {
        Ok(Arc::new(Self {
            track: Mutex::new(PopularityTracker::new()),
            popular_cutoff: 0.15,
            queue_len: 2,
            que_map,
        }))
    }
}
impl DispatchPolicy for PopularQueueLenDispatch {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        let freq = self.track.lock().track(&reg.fqdn);
        match freq < self.popular_cutoff {
            true => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            false => match self.que_map.get(&Compute::GPU).map_or(0, |q| q.queue_len()) > self.queue_len {
                true => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
                false => (Compute::GPU, NO_ESTIMATE, NO_ESTIMATE),
            },
        }
    }
}

/// Schedules the most popular functions to run on GPU.
/// Don't let them run if CPU time faster.
pub struct PopularEstTimeDispatch {
    track: Mutex<PopularityTracker>,
    popular_cutoff: f64,
    que_map: QueueMap,
}
impl PopularEstTimeDispatch {
    pub fn boxed(que_map: QueueMap) -> Result<Arc<dyn DispatchPolicy>> {
        Ok(Arc::new(Self {
            track: Mutex::new(PopularityTracker::new()),
            popular_cutoff: 0.15,
            que_map,
        }))
    }
}
impl DispatchPolicy for PopularEstTimeDispatch {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let freq = self.track.lock().track(&reg.fqdn);
        match freq < self.popular_cutoff {
            true => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            false => {
                let mut opts = vec![];
                for c in reg.supported_compute.into_iter() {
                    if let Some(q) = self.que_map.get(&c) {
                        opts.push((q.est_completion_time(reg, tid), c));
                    }
                }
                match opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
                    Some(((est_time, load), c)) => (*c, *load, *est_time),
                    None => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
                }
            },
        }
    }
}
