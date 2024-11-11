use crate::services::invocation::queueing::EnqueuedInvocation;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::types::Compute;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct LandlordConfig {
    pub cache_size: u32,
}

pub struct LandlordDispatch {
    /// Map of FQDN -> credit
    credits: HashMap<String, f64>,
    cmap: Arc<CharacteristicsMap>,
    cfg: Arc<LandlordConfig>,
}
impl LandlordDispatch {
    pub fn new(cmap: &Arc<CharacteristicsMap>, cfg: &Option<Arc<LandlordConfig>>) -> anyhow::Result<Self> {
        match cfg {
            None => anyhow::bail!("LandlordConfig was empty"),
            Some(c) => Ok(Self {
                credits: HashMap::new(),
                cmap: cmap.clone(),
                cfg: c.clone(),
            }),
        }
    }
    fn cache_size(&self) -> usize {
        self.credits.len()
    }
    fn insert(&mut self, fqdn: &str) {
        match self.credits.get_mut(fqdn) {
            None => {
                let size = self.cmap.avg_gpu_e2e_t(fqdn);
                self.credits.insert(fqdn.to_string(), size);
            }
            Some(c) => {
                *c += self.cmap.get_gpu_exec_time(fqdn);
            }
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
        self.cmap.avg_gpu_e2e_t(fqdn) - self.cmap.avg_cpu_e2e_t(fqdn)
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
    pub fn choose(&mut self, item: &Arc<EnqueuedInvocation>) -> Compute {
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn);
            return Compute::GPU;
        }
        if self.present(&item.registration.fqdn) {
            return self.credit(&item.registration.fqdn);
        }
        self.charge_rent(&item.registration.fqdn);
        if self.cache_size() < self.cfg.cache_size as usize {
            // easy insert
            self.insert(&item.registration.fqdn);
            return Compute::GPU;
        }
        Compute::CPU
    }
}
