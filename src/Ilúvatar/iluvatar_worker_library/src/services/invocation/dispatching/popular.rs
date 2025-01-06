use crate::services::invocation::dispatching::{queueing_dispatcher::DispatchPolicy, EnqueueingPolicy};
use crate::services::invocation::queueing::DeviceQueue;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::InvocationConfig;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::clock::now;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::info;

pub fn get_popular(
    pol: EnqueueingPolicy,
    cmap: &Arc<CharacteristicsMap>,
    invocation_config: &Arc<InvocationConfig>,
    cpu_queue: &Arc<dyn DeviceQueue>,
    gpu_queue: &Option<Arc<dyn DeviceQueue>>,
) -> Result<Box<dyn DispatchPolicy>> {
    match pol {
        EnqueueingPolicy::TopAvg => TopAvgDispatch::boxed(cmap),
        EnqueueingPolicy::LeastPopular => LeastPopularDispatch::boxed(),
        EnqueueingPolicy::Popular => PopularDispatch::boxed(),
        EnqueueingPolicy::PopularEstTimeDispatch => PopularEstTimeDispatch::boxed(cpu_queue, gpu_queue),
        EnqueueingPolicy::PopularQueueLenDispatch => PopularQueueLenDispatch::boxed(cpu_queue, gpu_queue),
        EnqueueingPolicy::TCPEstSpeedup => TCPEstSpeedup::boxed(cpu_queue, gpu_queue, cmap, invocation_config),
        // popular policy not being used, give dummy basic policy
        _ => LeastPopularDispatch::boxed(),
    }
}

struct TCPEstSpeedup {
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
    cmap: Arc<CharacteristicsMap>,
    invocation_config: Arc<InvocationConfig>,
    running_avg_speedup: f64,
    last_avg_update: Instant,
    max_ratio: f64,
}
impl TCPEstSpeedup {
    fn boxed(
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
        cmap: &Arc<CharacteristicsMap>,
        invocation_config: &Arc<InvocationConfig>,
    ) -> Result<Box<dyn DispatchPolicy>> {
        Ok(Box::new(Self {
            running_avg_speedup: invocation_config.speedup_ratio.unwrap_or(4.0),
            invocation_config: invocation_config.clone(),
            cmap: cmap.clone(),
            cpu_queue: cpu_queue.clone(),
            gpu_queue: gpu_queue
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create PopularQueueLenDispatch"))?,
            last_avg_update: now(),
            max_ratio: 0.0,
        }))
    }
}
impl DispatchPolicy for TCPEstSpeedup {
    fn choose(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64) {
        let cpu = self.cmap.avg_cpu_exec_t(&reg.fqdn);
        let gpu = self.cmap.avg_gpu_exec_t(&reg.fqdn);
        let ratio = cpu / gpu;
        self.max_ratio = f64::max(self.max_ratio, ratio);

        if self.last_avg_update.elapsed().as_secs_f64() > 5.0 {
            let q_len = self.gpu_queue.queue_len();
            if q_len <= 1 {
                self.running_avg_speedup *= 0.99;
            } else if q_len >= 10 {
                self.running_avg_speedup = f64::min(self.max_ratio, self.running_avg_speedup * 1.1);
            }
            info!(tid=%tid, new_avg=self.running_avg_speedup, "running avg");
            self.last_avg_update = now();
        }
        if ratio > self.running_avg_speedup {
            let mut opts = vec![];
            if reg.supported_compute.contains(Compute::CPU) {
                opts.push((self.cpu_queue.est_completion_time(&reg, &tid), Compute::CPU));
            }
            if reg.supported_compute.contains(Compute::GPU) {
                opts.push((self.gpu_queue.est_completion_time(&reg, &tid), Compute::GPU));
            }
            if let Some(((_est_time, load), c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
                (*c, *load)
            } else {
                (Compute::CPU, 0.0)
            }
        } else {
            if self.invocation_config.log_details() {
                info!(tid=%tid, fqdn=%reg.fqdn, pot_creds=0.0, "Cache Miss");
            }
            (Compute::CPU, 0.0)
        }
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
    fn choose(&mut self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64) {
        let iat = self.cmap.get_iat(&reg.fqdn);
        self.iats.write().insert(reg.fqdn.clone(), iat);
        let iats = self.iats.read().values().copied().collect::<Vec<f64>>();
        let avg = iats.iter().sum::<f64>() / iats.len() as f64;
        match iat > avg {
            true => (Compute::CPU, 0.0),
            false => (Compute::GPU, 0.0),
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
    fn choose(&mut self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64) {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&reg.fqdn) {
            true => {
                let val = self.invokes.get_mut(&reg.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(reg.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq > self.popular_cutoff {
            true => (Compute::CPU, 0.0),
            false => (Compute::GPU, 0.0),
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
    fn choose(&mut self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64) {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&reg.fqdn) {
            true => {
                let val = self.invokes.get_mut(&reg.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(reg.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq < self.popular_cutoff {
            true => (Compute::CPU, 0.0),
            false => (Compute::GPU, 0.0),
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
    fn choose(&mut self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64) {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&reg.fqdn) {
            true => {
                let val = self.invokes.get_mut(&reg.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(reg.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq < self.popular_cutoff {
            true => (Compute::CPU, 0.0),
            false => match self.gpu_queue.queue_len() > self.queue_len {
                true => (Compute::CPU, 0.0),
                false => (Compute::GPU, 0.0),
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
    fn choose(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64) {
        self.total_invokes += 1;
        let invokes = match self.invokes.contains_key(&reg.fqdn) {
            true => {
                let val = self.invokes.get_mut(&reg.fqdn).unwrap();
                *val += 1;
                *val
            },
            false => {
                self.invokes.insert(reg.fqdn.clone(), 1);
                1
            },
        };
        let freq = invokes as f64 / self.total_invokes as f64;
        match freq < self.popular_cutoff {
            true => (Compute::CPU, 0.0),
            false => {
                let cpu = self.cpu_queue.est_completion_time(&reg, &tid);
                let gpu = self.gpu_queue.est_completion_time(&reg, &tid);
                match cpu < gpu {
                    true => (Compute::CPU, 0.0),
                    false => (Compute::GPU, 0.0),
                }
            },
        }
    }
}
