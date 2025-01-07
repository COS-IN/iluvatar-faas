use std::collections::HashSet;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::task::JoinHandle;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::threading;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use anyhow::Result;
use ordered_float::OrderedFloat;
use crate::services::invocation::queueing::DeviceQueue;
use crate::services::registration::{RegisteredFunction, RegistrationService};

#[derive(serde::Deserialize, Debug)]
pub enum AllowPolicy {
    TopThird,
    TopQuarter,
    LoadLimit,
    QueueTput,
}
impl Default for AllowPolicy {
    fn default() -> Self {
        Self::TopThird
    }
}
#[derive(serde::Deserialize, Debug)]
pub struct GreedyWeightConfig {
    #[serde(default)]
    allow: AllowPolicy,
    #[serde(default)]
    allow_load: f64,
    #[serde(default)]
    log: bool,
}
#[derive(Debug)]
struct FuncInfo {
    fqdn: String,
    load: f64,
    tput: f64,
    opp_cost: OrderedFloat<f64>,
}

#[allow(unused)]
pub struct GreedyWeights {
    cmap: Arc<CharacteristicsMap>,
    config: Arc<GreedyWeightConfig>,
    reg: Arc<RegistrationService>,
    allow_set: RwLock<HashSet<String>>,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
    _thread: JoinHandle<()>
}
impl GreedyWeights {
    pub fn boxed(cmap: &Arc<CharacteristicsMap>,
             cpu_queue: &Arc<dyn DeviceQueue>,
             gpu_queue: &Option<Arc<dyn DeviceQueue>>,
             config: &Option<Arc<GreedyWeightConfig>>,
             reg: &Arc<RegistrationService>,) -> Result<Arc<Self>> {
        let (trd, tx) = threading::tokio_thread(10000, "greedy_w_bkg".to_string(), Self::update_set);
        let svc = Arc::new(Self {
            cmap: cmap.clone(),
            cpu_queue: cpu_queue.clone(),
            gpu_queue: gpu_queue
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create GreedyWeights"))?,
            config: config.clone()
                .ok_or_else(|| anyhow::anyhow!("GreedyWeightConfig was empty trying to create GreedyWeights"))?,
            reg: reg.clone(),
            allow_set: RwLock::new(HashSet::new()),
            _thread: trd
        });
        tx.send(svc.clone())?;
        Ok(svc)
    }

    fn allow_size(&self, fun_data: &Vec<FuncInfo>, num: usize) -> (HashSet<String>, f64) {
        let mut allowed_load = 0.0;
        let mut allow_set = HashSet::new();
        for info in fun_data.iter() {
            allowed_load += info.load;
            allow_set.insert(info.fqdn.clone());
            if allow_set.len() >= num {
                break;
            }
        }
        (allow_set, allowed_load)
    }

    fn allow_load(&self, fun_data: &Vec<FuncInfo>, load: f64) -> (HashSet<String>, f64) {
        let mut allowed_load = 0.0;
        let mut allow_set = HashSet::new();
        for info in fun_data.iter() {
            allowed_load += info.load;
            allow_set.insert(info.fqdn.clone());
            if allowed_load >= load {
                break;
            }
        }
        (allow_set, allowed_load)
    }

    fn queue_tput(&self, fun_data: &Vec<FuncInfo>) -> (HashSet<String>, f64) {
        let mut allowed_load = 0.0;
        let mut allowed_tput = 0.0;
        let mut allow_set = HashSet::new();
        let gpu_tput = self.cmap.get_gpu_tput();
        if self.gpu_queue.queue_len() > 10 {
            for info in fun_data.iter() {
                allowed_load += info.load;
                allowed_tput += info.tput;
                allow_set.insert(info.fqdn.clone());
                if allowed_tput >= gpu_tput {
                    break;
                }
            }
        } else {
            // mul by num GPUs
            allow_set = HashSet::from_iter(fun_data.iter().take(16).map(|f| f.fqdn.clone()));
        }

        (allow_set, allowed_load)
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
            if iat == 0.0 {
                iat = 100.0; // unknown IAT, make large
            }
            let opp = (cpu - gpu) / iat;
            let load = gpu * (1.0/iat);
            data.push(FuncInfo {
                opp_cost: OrderedFloat(opp),
                tput: 1.0/iat,
                fqdn,
                load,
            });
        }
        data.sort_by(|i1, i2| i2.opp_cost.cmp(&i1.opp_cost));
        // TODO: a better way to allow functions in
        // Based on per-func applied load applied to GPU?
        // Allow fractions of a funcs invocations to go through?
        let (allow_set, allowed_load) = match self.config.allow {
            AllowPolicy::TopThird => self.allow_size(&data, data.len() / 3),
            AllowPolicy::TopQuarter => self.allow_size(&data, data.len() / 4),
            AllowPolicy::LoadLimit => self.allow_load(&data, self.config.allow_load),
            AllowPolicy::QueueTput => self.queue_tput(&data),
        };
        if self.config.log {
            tracing::info!(tid=%_tid, allowed_load=allowed_load, allow_set=?allow_set, data=?data, "Sorted function allowed GPU");
        }
        *self.allow_set.write() = allow_set;
    }

    pub fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64) {
        match self.allow_set.read().contains(&reg.fqdn) {
            true => {
                (Compute::GPU, 0.0)
                // let mut opts = vec![];
                // if reg.supported_compute.contains(Compute::CPU) {
                //     opts.push((self.cpu_queue.est_completion_time(reg, &_tid), Compute::CPU));
                // }
                // if reg.supported_compute.contains(Compute::GPU) {
                //     opts.push((self.gpu_queue.est_completion_time(reg, &_tid), Compute::GPU));
                // }
                // if let Some(((_est, load), c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
                //     (*c, *load)
                // } else {
                //     (Compute::GPU, 0.0)
                // }
            },
            false => (Compute::CPU, 0.0)
        }
    }
}