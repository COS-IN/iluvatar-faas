use crate::services::invocation::dispatching::queueing_dispatcher::DispatchPolicy;
use crate::services::invocation::dispatching::EnqueueingPolicy;
use crate::services::invocation::queueing::{DeviceQueue, EnqueuedInvocation};
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::InvocationConfig;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

const CACHE_LOG: &str = "Landlord cache";
const INSERT_LOG: &str = "Adding function to cache";

#[derive(Debug, Deserialize)]
pub struct LandlordConfig {
    pub cache_size: u32,
    pub log_cache_info: bool,
}

pub fn get_landlord(
    pol: EnqueueingPolicy,
    cmap: &Arc<CharacteristicsMap>,
    invocation_config: &Arc<InvocationConfig>,
    cpu_queue: &Arc<dyn DeviceQueue>,
    gpu_queue: &Option<Arc<dyn DeviceQueue>>,
) -> Result<Box<dyn DispatchPolicy>> {
    match pol {
        EnqueueingPolicy::Landlord => LandlordDispatch::boxed(cmap, &invocation_config.landlord_config),
        EnqueueingPolicy::LandlordEstTime => {
            LandlordEstTimeDispatch::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue)
        },
        EnqueueingPolicy::LandlordPerFuncRent => {
            LandlordPerFuncRent::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue)
        },
        EnqueueingPolicy::LandlordPerFuncRentHistorical => {
            LandlordPerFuncRentHistorical::boxed(cmap, &invocation_config.landlord_config)
        },
        // landlord policy not being used, give dummy basic policy
        _ => LandlordDispatch::boxed(cmap, &invocation_config.landlord_config),
    }
}

pub struct LandlordDispatch {
    /// Map of FQDN -> credit
    credits: HashMap<String, f64>,
    cmap: Arc<CharacteristicsMap>,
    cfg: Arc<LandlordConfig>,
}
impl LandlordDispatch {
    pub fn boxed(cmap: &Arc<CharacteristicsMap>, cfg: &Option<Arc<LandlordConfig>>) -> Result<Box<dyn DispatchPolicy>> {
        match cfg {
            None => anyhow::bail!("LandlordConfig was empty"),
            Some(c) => Ok(Box::new(Self {
                credits: HashMap::new(),
                cmap: cmap.clone(),
                cfg: c.clone(),
            })),
        }
    }
    fn cache_size(&self) -> usize {
        self.credits.len()
    }
    fn insert(&mut self, fqdn: &str, tid: &TransactionId) {
        match self.credits.get_mut(fqdn) {
            None => {
                let cost = self.cmap.avg_gpu_e2e_t(fqdn) - self.cmap.avg_cpu_e2e_t(fqdn);
                self.credits.insert(fqdn.to_string(), cost);
                if self.cfg.log_cache_info {
                    info!(tid=%tid, fqdn=%fqdn, "{}", INSERT_LOG);
                }
            },
            Some(c) => {
                *c += self.cmap.get_gpu_exec_time(fqdn);
            },
        }
    }
    fn charge_rent(&mut self, fqdn: &str) {
        let new_credit = self.calc_credit(fqdn);
        self.credits.retain(|_, c| {
            *c -= new_credit;
            *c > 0.0
        });
    }
    fn present(&self, fqdn: &str) -> bool {
        self.credits.contains_key(fqdn)
    }
    fn calc_credit(&self, fqdn: &str) -> f64 {
        self.cmap.avg_cpu_e2e_t(fqdn) - self.cmap.avg_gpu_e2e_t(fqdn)
    }
    fn credit(&mut self, fqdn: &str) -> Compute {
        let new_credit = self.calc_credit(fqdn);
        if let Some(credit) = self.credits.get_mut(fqdn) {
            if new_credit < 0.0 {
                // no benefit! run on CPU
                return Compute::CPU;
            }
            *credit += new_credit;
        }
        Compute::GPU
    }
    #[inline(always)]
    fn log(&self, tid: &TransactionId) {
        if self.cfg.log_cache_info {
            info!(tid=%tid, cache=?self.credits, "{}", CACHE_LOG);
        }
    }
}
impl DispatchPolicy for LandlordDispatch {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> Compute {
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn, tid);
            self.log(tid);
            return Compute::GPU;
        }
        if self.present(&item.registration.fqdn) {
            self.log(tid);
            return self.credit(&item.registration.fqdn);
        }
        self.charge_rent(&item.registration.fqdn);
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn, tid);
            self.log(tid);
            return Compute::GPU;
        }
        self.log(tid);
        Compute::CPU
    }
}

pub struct LandlordEstTimeDispatch {
    /// Map of FQDN -> credit
    credits: HashMap<String, f64>,
    cmap: Arc<CharacteristicsMap>,
    cfg: Arc<LandlordConfig>,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
}
impl LandlordEstTimeDispatch {
    pub fn boxed(
        cmap: &Arc<CharacteristicsMap>,
        cfg: &Option<Arc<LandlordConfig>>,
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
    ) -> Result<Box<dyn DispatchPolicy>> {
        match cfg {
            None => anyhow::bail!("LandlordConfig was empty"),
            Some(c) => Ok(Box::new(Self {
                credits: HashMap::new(),
                cmap: cmap.clone(),
                cfg: c.clone(),
                cpu_queue: cpu_queue.clone(),
                gpu_queue: gpu_queue
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create LandlordEstTimeDispatch"))?,
            })),
        }
    }
    fn cache_size(&self) -> usize {
        self.credits.len()
    }
    fn insert(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) {
        match self.credits.get_mut(&reg.fqdn) {
            None => {
                // let cost = self.cmap.avg_gpu_e2e_t(fqdn) - self.cmap.avg_cpu_e2e_t(fqdn);
                let cost = self.calc_credit(reg, tid);
                self.credits.insert(reg.fqdn.clone(), cost);
                if self.cfg.log_cache_info {
                    info!(tid=%tid, fqdn=%&reg.fqdn, "{}", INSERT_LOG);
                }
            },
            Some(c) => {
                *c += self.cmap.get_gpu_exec_time(&reg.fqdn);
            },
        }
    }
    fn charge_rent(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) {
        let new_credit = self.calc_credit(reg, tid);
        self.credits.retain(|_, c| {
            *c -= new_credit;
            *c > 0.0
        });
    }
    fn present(&self, fqdn: &str) -> bool {
        self.credits.contains_key(fqdn)
    }
    fn calc_credit(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
        self.cpu_queue.est_completion_time(reg, tid) - self.gpu_queue.est_completion_time(reg, tid)
    }
    fn credit(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Compute {
        let new_credit = self.calc_credit(reg, tid);
        if let Some(credit) = self.credits.get_mut(&reg.fqdn) {
            if new_credit < 0.0 {
                // no benefit! run on CPU
                return Compute::CPU;
            }
            *credit += new_credit;
        }
        Compute::GPU
    }
    #[inline(always)]
    fn log(&self, tid: &TransactionId) {
        if self.cfg.log_cache_info {
            info!(tid=%tid, cache=?self.credits, "{}", CACHE_LOG);
        }
    }
}
impl DispatchPolicy for LandlordEstTimeDispatch {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> Compute {
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration, tid);
            self.log(tid);
            return Compute::GPU;
        }
        if self.present(&item.registration.fqdn) {
            self.log(tid);
            return self.credit(&item.registration, tid);
        }
        self.charge_rent(&item.registration, tid);
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration, tid);
            self.log(tid);
            return Compute::GPU;
        }
        self.log(tid);
        Compute::CPU
    }
}

////////////////////
/// This is the main landlord policy, ignore the rest 

/// Only expected to run with MQFQ for GPU queuing.
pub struct LandlordPerFuncRent {
    /// Map of FQDN -> credit
    credits: HashMap<String, f64>,
    gpu_invokes: HashMap<String, u64>,
    cmap: Arc<CharacteristicsMap>,
    cfg: Arc<LandlordConfig>,
    _cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
}
impl LandlordPerFuncRent {
    pub fn boxed(
        cmap: &Arc<CharacteristicsMap>,
        cfg: &Option<Arc<LandlordConfig>>,
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
    ) -> Result<Box<dyn DispatchPolicy>> {
        match cfg {
            None => anyhow::bail!("LandlordConfig was empty"),
            Some(c) => Ok(Box::new(Self {
                credits: HashMap::new(),
                gpu_invokes: HashMap::new(),
                cmap: cmap.clone(),
                cfg: c.clone(),
                _cpu_queue: cpu_queue.clone(),
                gpu_queue: gpu_queue
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create LandlordPerFuncRent"))?,
            })),
        }
    }
    fn cache_size(&self) -> usize {
        self.credits.len()
    }
    fn insert(&mut self, fqdn: &str, tid: &TransactionId) {
        match self.credits.get_mut(fqdn) {
            None => {
                let cost = self.cmap.avg_gpu_e2e_t(fqdn) - self.cmap.avg_cpu_e2e_t(fqdn);
                self.credits.insert(fqdn.to_string(), cost);
                if self.cfg.log_cache_info {
                    info!(tid=%tid, fqdn=%fqdn, "{}", INSERT_LOG);
                }
            },
            Some(c) => {
                *c += self.cmap.get_gpu_exec_time(fqdn);
            },
        }
    }
    
    fn charge_rent(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) {
	let total_rent_due = self.calc_credit(reg, tid);

	// rent is charged proportional to n*exec from each fqdn 
        if let Some(mqfq) = self.gpu_queue.expose_mqfq() {
            let vals = self
                .credits
                .keys()
                .map(|fqdn| {
                    (
                        fqdn.clone(),
                        mqfq.get(fqdn).map_or(0.0, |q| q.queue.len() as f64),
                        self.cmap.get_gpu_exec_time(fqdn),
                    )
                })
                .collect::<Vec<(String, f64, f64)>>();
            let tot = vals.iter().fold(0.0, |acc, (_, x, y)| acc + (x * y));

	    let frac_rent = total_rent_due / tot ; 
	    
            let _ = vals
                .into_iter()
                .map(|(fqdn, len, exec)| self.credits.get_mut(&fqdn).map(|x| *x -= len * exec * frac_rent));
            self.credits.retain(|_fqdn, c| *c > 0.0);
        }
    }
    fn present(&self, fqdn: &str) -> bool {
        self.credits.contains_key(fqdn)
    }

    fn calc_credit(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {

	let mqfq_est = self.gpu_queue.est_completion_time(reg, tid) ;
	let gpu_est = self.cmap.get_gpu_est(&reg.fqdn, mqfq_est) ;
	
        self._cpu_queue.est_completion_time(reg, tid) - gpu_est 
    }
    
    fn credit(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Compute {
        let new_credit = self.calc_credit(reg, tid); 
	    // self.cmap.avg_cpu_e2e_t(&reg.fqdn) - self.cmap.avg_gpu_e2e_t(&reg.fqdn);
        if let Some(credit) = self.credits.get_mut(&reg.fqdn) {
            if new_credit < 0.0 {
                // no benefit! run on CPU
                return Compute::CPU;
            }
            *credit += new_credit;
        }
        match self.gpu_invokes.get_mut(&reg.fqdn) {
            None => {
                self.gpu_invokes.insert(reg.fqdn.clone(), 1);
            },
            Some(i) => *i += 1,
        }
        Compute::GPU
    }
    #[inline(always)]
    fn log(&self, tid: &TransactionId) {
        if self.cfg.log_cache_info {
            info!(tid=%tid, cache=?self.credits, "{}", CACHE_LOG);
        }
    }
}
impl DispatchPolicy for LandlordPerFuncRent {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> Compute {
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn, tid);
            self.log(tid);
            return Compute::GPU;
        }
        if self.present(&item.registration.fqdn) {
            self.log(tid);
            return self.credit(&item.registration, tid);
        }
        self.charge_rent(&item.registration, tid);
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn, tid);
            self.log(tid);
            return Compute::GPU;
        }
        self.log(tid);
        Compute::CPU
    }
}

//////////////////////////////////

pub struct LandlordPerFuncRentHistorical {
    /// Map of FQDN -> credit
    credits: HashMap<String, f64>,
    gpu_invokes: HashMap<String, u64>,
    cmap: Arc<CharacteristicsMap>,
    cfg: Arc<LandlordConfig>,
}
impl LandlordPerFuncRentHistorical {
    pub fn boxed(cmap: &Arc<CharacteristicsMap>, cfg: &Option<Arc<LandlordConfig>>) -> Result<Box<dyn DispatchPolicy>> {
        match cfg {
            None => anyhow::bail!("LandlordConfig was empty"),
            Some(c) => Ok(Box::new(Self {
                credits: HashMap::new(),
                gpu_invokes: HashMap::new(),
                cmap: cmap.clone(),
                cfg: c.clone(),
            })),
        }
    }
    fn cache_size(&self) -> usize {
        self.credits.len()
    }
    fn insert(&mut self, fqdn: &str, tid: &TransactionId) {
        match self.credits.get_mut(fqdn) {
            None => {
                let cost = self.cmap.avg_gpu_e2e_t(fqdn) - self.cmap.avg_cpu_e2e_t(fqdn);
                self.credits.insert(fqdn.to_string(), cost);
                if self.cfg.log_cache_info {
                    info!(tid=%tid, fqdn=%fqdn, "{}", INSERT_LOG);
                }
            },
            Some(c) => {
                *c += self.cmap.get_gpu_exec_time(fqdn);
            },
        }
    }
    fn charge_rent(&mut self) {
        let tot = self
            .credits
            .keys()
            .map(|fqdn| (*self.gpu_invokes.get(fqdn).unwrap_or(&0) as f64) * self.cmap.get_gpu_exec_time(fqdn))
            .sum::<f64>();
        self.credits.retain(|fqdn, c| {
            *c -= (*self.gpu_invokes.get(fqdn).unwrap_or(&0) as f64) * self.cmap.get_gpu_exec_time(fqdn) / tot;
            *c > 0.0
        });
    }
    fn present(&self, fqdn: &str) -> bool {
        self.credits.contains_key(fqdn)
    }
    #[inline(always)]
    fn log(&self, tid: &TransactionId) {
        if self.cfg.log_cache_info {
            info!(tid=%tid, cache=?self.credits, "{}", CACHE_LOG);
        }
    }
    
    fn credit(&mut self, reg: &Arc<RegisteredFunction>) -> Compute {
        let new_credit = self.cmap.avg_cpu_e2e_t(&reg.fqdn) - self.cmap.avg_gpu_e2e_t(&reg.fqdn);
        if let Some(credit) = self.credits.get_mut(&reg.fqdn) {
            if new_credit < 0.0 {
                // no benefit! run on CPU
                return Compute::CPU;
            }
            *credit += new_credit;
        }
        match self.gpu_invokes.get_mut(&reg.fqdn) {
            None => {
                self.gpu_invokes.insert(reg.fqdn.clone(), 1);
            },
            Some(i) => *i += 1,
        }
        Compute::GPU
    }
}
impl DispatchPolicy for LandlordPerFuncRentHistorical {
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> Compute {
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn, tid);
            self.log(tid);
            return Compute::GPU;
        }
        if self.present(&item.registration.fqdn) {
            self.log(tid);
            return self.credit(&item.registration);
        }
        self.charge_rent();
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn, tid);
            self.log(tid);
            return Compute::GPU;
        }
        self.log(tid);
        Compute::CPU
    }
}
