use crate::services::invocation::queueing::EnqueuedInvocation;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::types::Compute;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TopAvgDispatch {
    /// Map of FQDN -> IAT
    iats: RwLock<HashMap<String, f64>>,
    cmap: Arc<CharacteristicsMap>,
}
impl TopAvgDispatch {
    pub fn new(cmap: &Arc<CharacteristicsMap>) -> anyhow::Result<Self> {
        Ok(Self {
            iats: RwLock::new(HashMap::new()),
            cmap: cmap.clone(),
        })
    }

    pub fn choose(&self, item: &Arc<EnqueuedInvocation>) -> Compute {
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

pub struct PopularDispatch {
    /// Map of FQDN -> invokes
    invokes: HashMap<String, u64>,
    total_invokes: u64,
    popular_cutoff: f64,
}
impl PopularDispatch {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            invokes: HashMap::new(),
            total_invokes: 0,
            popular_cutoff: 0.15,
        })
    }

    pub fn choose(&mut self, item: &Arc<EnqueuedInvocation>) -> Compute {
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
