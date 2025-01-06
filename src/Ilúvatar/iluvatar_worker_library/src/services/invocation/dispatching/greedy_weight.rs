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
use crate::worker_api::config::InvocationConfig;

#[allow(unused)]
pub struct GreedyWeights {
    cmap: Arc<CharacteristicsMap>,
    _invocation_config: Arc<InvocationConfig>,
    reg: Arc<RegistrationService>,
    allow_set: RwLock<HashSet<String>>,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
    _thread: JoinHandle<()>
}
impl GreedyWeights {
    pub fn boxed(cmap: &Arc<CharacteristicsMap>,
             invocation_config: &Arc<InvocationConfig>,
             cpu_queue: &Arc<dyn DeviceQueue>,
             gpu_queue: &Option<Arc<dyn DeviceQueue>>,
             reg: &Arc<RegistrationService>,) -> Result<Arc<Self>> {
        let (trd, tx) = threading::tokio_thread(10000, "greedy_w_bkg".to_string(), Self::update_set);
        let svc = Arc::new(Self {
            cmap: cmap.clone(),
            cpu_queue: cpu_queue.clone(),
            gpu_queue: gpu_queue
                .clone()
                .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create GreedyWeights"))?,
                _invocation_config: invocation_config.clone(),
            reg: reg.clone(),
            allow_set: RwLock::new(HashSet::new()),
            _thread: trd
        });
        tx.send(svc.clone())?;
        Ok(svc)
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
            data.push( (OrderedFloat(opp), OrderedFloat(load), fqdn) );
        }
        data.sort_by(|i1, i2| i2.0.cmp(&i1.0));
        // tracing::info!(tid=%_tid, data=?data, "Sorted function loads");
        let mut allow_set = HashSet::new();
        for i in 0..(data.len() / 3) {
            allow_set.insert(data[i].2.clone());
        }
        let allowed_load = 0.0;
        // TODO: a better way to allow functions in
        // Based on per-func applied load applied to GPU? 
        // Allow fractions of a funcs invocations to go through?
        
        // let mut allowed_load = 0.0;
        // for (_opp, load, fqdn) in data.into_iter() {
        //     allowed_load += load.0;
        //     allow_set.insert(fqdn);
        //     if allowed_load >= 3.0 {
        //         break;
        //     }
        // }
        tracing::info!(tid=%_tid, allowed_load=allowed_load, allow_set=?allow_set, "GPU allowed functions");
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