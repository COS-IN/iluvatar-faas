use crate::services::invocation::dispatching::{queueing_dispatcher::DispatchPolicy, EnqueueingPolicy};
use crate::services::invocation::queueing::{DeviceQueue, EnqueuedInvocation};
use crate::worker_api::config::InvocationConfig;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub fn get_popular(
    pol: EnqueueingPolicy,
    cmap: &Arc<CharacteristicsMap>,
    _invocation_config: &Arc<InvocationConfig>,
    cpu_queue: &Arc<dyn DeviceQueue>,
    gpu_queue: &Option<Arc<dyn DeviceQueue>>,
) -> anyhow::Result<Box<dyn DispatchPolicy>> {
    match pol {
        EnqueueingPolicy::TopAvg => TopAvgDispatch::boxed(cmap),
        EnqueueingPolicy::LeastPopular => LeastPopularDispatch::boxed(),
        EnqueueingPolicy::Popular => PopularDispatch::boxed(),
        EnqueueingPolicy::PopularEstTimeDispatch => PopularEstTimeDispatch::boxed(cpu_queue, gpu_queue),
        EnqueueingPolicy::PopularQueueLenDispatch => PopularQueueLenDispatch::boxed(cpu_queue, gpu_queue),
        // popular policy not being used, give dummy basic policy
        _ => LeastPopularDispatch::boxed(),
    }
}

pub struct TopAvgDispatch {
    /// Map of FQDN -> IAT
    iats: RwLock<HashMap<String, f64>>,
    cmap: Arc<CharacteristicsMap>,
}
impl TopAvgDispatch {
    pub fn boxed(cmap: &Arc<CharacteristicsMap>) -> anyhow::Result<Box<dyn DispatchPolicy>> {
        Ok(Box::new(Self {
            iats: RwLock::new(HashMap::new()),
            cmap: cmap.clone(),
        }))
    }
}
impl DispatchPolicy for TopAvgDispatch {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, _tid: &TransactionId) -> Compute {
        let iat = self.cmap.get_iat(&item.registration.fqdn);
        self.iats.write().insert(item.registration.fqdn.clone(), iat);
        let iats = self.iats.read().values().copied().collect::<Vec<f64>>();
        let avg = iats.iter().sum::<f64>() / iats.len() as f64;
        match iat > avg {
            true => Compute::CPU,
            false => Compute::GPU,
        }
    }
}

/// Schedules the least popular functions to run on GPU
pub struct LeastPopularDispatch {
    /// Map of FQDN -> invokes
    invokes: HashMap<String, u64>,
    total_invokes: u64,
    popular_cutoff: f64,
}
impl LeastPopularDispatch {
    pub fn boxed() -> anyhow::Result<Box<dyn DispatchPolicy>> {
        Ok(Box::new(Self {
            invokes: HashMap::new(),
            total_invokes: 0,
            popular_cutoff: 0.15,
        }))
    }
}
impl DispatchPolicy for LeastPopularDispatch {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, _tid: &TransactionId) -> Compute {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&item.registration.fqdn) {
            true => {
                let val = self.invokes.get_mut(&item.registration.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(item.registration.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq > self.popular_cutoff {
            true => Compute::CPU,
            false => Compute::GPU,
        }
    }
}

/// Schedules the most popular functions to run on GPU
pub struct PopularDispatch {
    /// Map of FQDN -> invokes
    invokes: HashMap<String, u64>,
    total_invokes: u64,
    popular_cutoff: f64,
}
impl PopularDispatch {
    pub fn boxed() -> anyhow::Result<Box<dyn DispatchPolicy>> {
        Ok(Box::new(Self {
            invokes: HashMap::new(),
            total_invokes: 0,
            popular_cutoff: 0.15,
        }))
    }
}
impl DispatchPolicy for PopularDispatch {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, _tid: &TransactionId) -> Compute {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&item.registration.fqdn) {
            true => {
                let val = self.invokes.get_mut(&item.registration.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(item.registration.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq < self.popular_cutoff {
            true => Compute::CPU,
            false => Compute::GPU,
        }
    }
}

/// Schedules the most popular functions to run on GPU.
/// Don't let them run if Q is long.
pub struct PopularQueueLenDispatch {
    /// Map of FQDN -> invokes
    invokes: HashMap<String, u64>,
    total_invokes: u64,
    popular_cutoff: f64,
    queue_len: usize,
    _cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
}
impl PopularQueueLenDispatch {
    pub fn boxed(
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
    ) -> anyhow::Result<Box<dyn DispatchPolicy>> {
        Ok(Box::new(Self {
            invokes: HashMap::new(),
            total_invokes: 0,
            popular_cutoff: 0.15,
            queue_len: 2,
            _cpu_queue: cpu_queue.clone(),
            gpu_queue: gpu_queue
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create PopularQueueLenDispatch"))?,
        }))
    }
}
impl DispatchPolicy for PopularQueueLenDispatch {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, _tid: &TransactionId) -> Compute {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&item.registration.fqdn) {
            true => {
                let val = self.invokes.get_mut(&item.registration.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(item.registration.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq < self.popular_cutoff {
            true => Compute::CPU,
            false => match self.gpu_queue.queue_len() > self.queue_len {
                true => Compute::CPU,
                false => Compute::GPU,
            },
        }
    }
}

/// Schedules the most popular functions to run on GPU.
/// Don't let them run if CPU time faster.
pub struct PopularEstTimeDispatch {
    /// Map of FQDN -> invokes
    invokes: HashMap<String, u64>,
    total_invokes: u64,
    popular_cutoff: f64,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
}
impl PopularEstTimeDispatch {
    pub fn boxed(
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
    ) -> anyhow::Result<Box<dyn DispatchPolicy>> {
        Ok(Box::new(Self {
            invokes: HashMap::new(),
            total_invokes: 0,
            popular_cutoff: 0.15,
            cpu_queue: cpu_queue.clone(),
            gpu_queue: gpu_queue
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create PopularEstTimeDispatch"))?,
        }))
    }
}
impl DispatchPolicy for PopularEstTimeDispatch {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, _tid: &TransactionId) -> Compute {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&item.registration.fqdn) {
            true => {
                let val = self.invokes.get_mut(&item.registration.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(item.registration.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq < self.popular_cutoff {
            true => Compute::CPU,
            false => {
                let cpu = self.cpu_queue.est_completion_time(&item.registration, &item.tid);
                let gpu = self.gpu_queue.est_completion_time(&item.registration, &item.tid);
                match cpu < gpu {
                    true => Compute::CPU,
                    false => Compute::GPU,
                }
            },
        }
    }
}
