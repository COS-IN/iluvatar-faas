use crate::services::invocation::queueing::DeviceQueue;
use crate::services::registration::{RegisteredFunction, RegistrationService};
use crate::services::resources::gpu::GpuResourceTracker;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::threading;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(serde::Deserialize, Debug)]
pub enum AllowPolicy {
    /// Top 25% of funcs are allowed on GPU.
    TopQuarter,
    /// Top 33% of funcs are allowed on GPU.
    TopThird,
    /// Allow funcs in based on load.
    /// Use [GreedyWeightConfig::load].
    LoadLimit,
    /// Allow funcs in based on their IATs and the throughput of the GPU queue.
    QueueTput,
    /// Specific number of functions allowed in.
    /// Use [GreedyWeightConfig::cache_size].
    Fixed,
    Incremental,
}
impl Default for AllowPolicy {
    fn default() -> Self {
        Self::TopQuarter
    }
}
#[derive(serde::Deserialize, Debug)]
pub struct GreedyWeightConfig {
    #[serde(default)]
    allow: AllowPolicy,
    #[serde(default)]
    log: bool,
    #[serde(default)]
    /// Amount of function 'load' allowed on GPU.
    /// Must be used with [AllowPolicy::LoadLimit].
    allow_load: f64,
    #[serde(default)]
    /// Specify the exact cache size to be used.
    /// Must be used with [AllowPolicy::Fixed].
    cache_size: usize,
    #[serde(default)]
    /// Disable dynamic dispatching based on cache position if [true].
    fixed_assignment: bool,
}
#[derive(Debug)]
struct FuncInfo {
    fqdn: String,
    load: f64,
    tput: f64,
    iat: f64,
    opp_cost: OrderedFloat<f64>,
}
#[allow(unused)]
struct CacheData {
    size: usize,
    old_tput: f64,
    old_load_avg: f64,
}

type AllowSet = HashMap<String, usize>;
#[allow(unused)]
pub struct GreedyWeights {
    cmap: Arc<CharacteristicsMap>,
    config: Arc<GreedyWeightConfig>,
    reg: Arc<RegistrationService>,
    allow_set: RwLock<AllowSet>,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
    gpu: Arc<GpuResourceTracker>,
    last_load: RwLock<CacheData>,
    _thread: JoinHandle<()>,
}
impl GreedyWeights {
    pub fn boxed(
        cmap: &Arc<CharacteristicsMap>,
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
        config: &Option<Arc<GreedyWeightConfig>>,
        reg: &Arc<RegistrationService>,
        gpu: &Option<Arc<GpuResourceTracker>>,
    ) -> Result<Arc<Self>> {
        let (trd, tx) = threading::tokio_thread(3000, "greedy_w_bkg".to_string(), Self::update_set);
        let svc = Arc::new(Self {
            last_load: RwLock::new(CacheData {
                size: 5,
                old_tput: 0.0,
                old_load_avg: 0.0,
            }),
            cmap: cmap.clone(),
            cpu_queue: cpu_queue.clone(),
            gpu_queue: gpu_queue
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create GreedyWeights"))?,
            gpu: gpu
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GpuResourceTracker was empty trying to create GreedyWeights"))?,
            config: config
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GreedyWeightConfig was empty trying to create GreedyWeights"))?,
            reg: reg.clone(),
            allow_set: RwLock::new(HashMap::new()),
            _thread: trd,
        });
        tx.send(svc.clone())?;
        Ok(svc)
    }

    /// Limit cache size to the total number of possible GPU containers.
    #[inline(always)]
    fn max_size(&self, poss_size: usize) -> usize {
        usize::min(self.gpu.total_gpus() as usize, poss_size)
    }

    fn allow_size(&self, fun_data: &[FuncInfo], num: usize) -> (AllowSet, f64) {
        let num = self.max_size(num);
        let mut allowed_load = 0.0;
        let mut allow_set = AllowSet::new();
        for info in fun_data.iter() {
            allowed_load += info.load;
            allow_set.insert(info.fqdn.clone(), allow_set.len());
            if allow_set.len() >= num {
                break;
            }
        }
        (allow_set, allowed_load)
    }

    fn allow_load(&self, fun_data: &[FuncInfo], load: f64) -> (AllowSet, f64) {
        let mut allowed_load = 0.0;
        let mut allow_set = AllowSet::new();
        for info in fun_data.iter() {
            allowed_load += info.load;
            allow_set.insert(info.fqdn.clone(), allow_set.len());
            if allowed_load >= load || allow_set.len() >= self.max_size(fun_data.len()) {
                break;
            }
        }
        (allow_set, allowed_load)
    }

    fn queue_tput(&self, fun_data: &[FuncInfo]) -> (AllowSet, f64) {
        let mut allowed_load = 0.0;
        let mut allowed_tput = 0.0;
        let mut allow_set = AllowSet::new();
        let gpu_tput = self.cmap.get_gpu_tput();
        if self.gpu_queue.queue_len() > 10 {
            for info in fun_data.iter() {
                allowed_load += info.load;
                allowed_tput += info.tput;
                allow_set.insert(info.fqdn.clone(), allow_set.len());
                if allowed_tput >= gpu_tput || allow_set.len() >= self.max_size(fun_data.len()) {
                    break;
                }
            }
        } else {
            for info in fun_data.iter().take(self.max_size(fun_data.len())) {
                allow_set.insert(info.fqdn.clone(), allow_set.len());
            }
        }

        (allow_set, allowed_load)
    }

    fn fixed_size(&self, fun_data: &[FuncInfo]) -> (AllowSet, f64) {
        let mut allowed_load = 0.0;
        let mut allow_set = AllowSet::new();
        for info in fun_data.iter() {
            if allow_set.len() >= self.config.cache_size {
                break;
            }
            allowed_load += info.load;
            allow_set.insert(info.fqdn.clone(), allow_set.len());
        }
        (allow_set, allowed_load)
    }

    fn incremental(&self, fun_data: &[FuncInfo]) -> (AllowSet, f64) {
        let q_load = self.gpu_queue.queue_load();
        let mut old_load = self.last_load.write();
        let mut new_cache_size = old_load.size;
        if q_load.load_avg == 0.0 && !fun_data.is_empty() && fun_data.iter().fold(0.0, |acc, f| acc + f.iat) != 0.0 {
            new_cache_size += 1;
        } else if q_load.tput >= 0.75 && q_load.load_avg >= 15.0 {
            new_cache_size = usize::max(1, new_cache_size - 1);
        } else if q_load.tput >= 0.6 || q_load.load_avg >= 5.0 {
            new_cache_size += 1;
        }
        *old_load = CacheData {
            size: new_cache_size,
            old_load_avg: q_load.load_avg,
            old_tput: q_load.tput,
        };
        self.allow_size(fun_data, new_cache_size)
    }

    async fn update_set(self: Arc<Self>, _tid: String) {
        let mut data = vec![];
        for fqdn in self.reg.registered_funcs() {
            let gpu = self.cmap.avg_gpu_exec_t(&fqdn);
            if gpu == 0.0 {
                continue;
            }
            let cpu = self.cmap.avg_cpu_exec_t(&fqdn);
            let mut iat = self.cmap.get_iat(&fqdn);
            let real_iat = iat;
            if iat == 0.0 {
                iat = 100.0; // unknown IAT, make large
            }
            let opp = (cpu - gpu) / iat;
            let load = gpu * (1.0 / iat);
            data.push(FuncInfo {
                opp_cost: OrderedFloat(opp),
                tput: 1.0 / iat,
                fqdn,
                iat: real_iat,
                load,
            });
        }
        data.sort_by(|i1, i2| i2.opp_cost.cmp(&i1.opp_cost));
        // TODO: a principled but working way to allow functions in
        // Based on per-function applied load applied to GPU?
        // Allow fractions of a function's invocations to go through?
        let (allow_set, allowed_load) = match self.config.allow {
            AllowPolicy::TopThird => self.allow_size(&data, data.len() / 3),
            AllowPolicy::TopQuarter => self.allow_size(&data, data.len() / 4),
            AllowPolicy::LoadLimit => self.allow_load(&data, self.config.allow_load),
            AllowPolicy::QueueTput => self.queue_tput(&data),
            AllowPolicy::Fixed => self.fixed_size(&data),
            AllowPolicy::Incremental => self.incremental(&data),
        };
        if self.config.log {
            tracing::info!(tid=%_tid, allowed_load=allowed_load, allow_set=?allow_set, data=?data, "Sorted function allowed GPU");
        }
        *self.allow_set.write() = allow_set;
    }

    pub fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64) {
        let lck = self.allow_set.read();
        let cache_size = lck.len();
        let entry = lck.get(&reg.fqdn).cloned();
        drop(lck);
        match entry {
            Some(pos) => {
                if self.config.fixed_assignment || pos <= (cache_size / 2) {
                    return (Compute::GPU, 0.0);
                }
                let mut opts = vec![];
                let mut cpu_load = 0.0;
                if reg.supported_compute.contains(Compute::CPU) {
                    let (cpu_est, load) = self.cpu_queue.est_completion_time(reg, tid);
                    cpu_load = load;
                    opts.push(((cpu_est, cpu_load), Compute::CPU));
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    opts.push((self.gpu_queue.est_completion_time(reg, tid), Compute::GPU));
                }
                match opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
                    Some(((_est, load), c)) => (*c, *load),
                    None => (Compute::CPU, cpu_load),
                }
            },
            None => (Compute::CPU, 0.0),
        }
    }
}
