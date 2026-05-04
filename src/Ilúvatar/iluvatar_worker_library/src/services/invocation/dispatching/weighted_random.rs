use crate::services::invocation::dispatching::queueing_dispatcher::{DispatchPolicy, PolymDispatchCtx};
use crate::services::invocation::dispatching::NO_ESTIMATE;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::InvocationConfig;
use iluvatar_library::char_map::WorkerCharMap;
use iluvatar_library::clock::{get_global_clock, Clock};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct WeightedRandomConfig {
//     pub gpu_probability: f64,
// }
//
// impl Default for WeightedRandomConfig {
//     fn default() -> Self {
//         Self {
//             gpu_probability: 0.7,
//         }
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightedRandomConfig {
    pub gpu_probability: f64,
}
pub struct WeightedRandom {
    config: Arc<InvocationConfig>,
    dispatch_state: RwLock<PolymDispatchCtx>,
    clock: Clock,
}

impl WeightedRandom {
    pub fn new(config: Arc<InvocationConfig>, cmap: &WorkerCharMap, tid: &TransactionId) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            dispatch_state: RwLock::new(PolymDispatchCtx::boxed(cmap)),
            clock: get_global_clock(tid)?,
        })
    }
}

impl DispatchPolicy for WeightedRandom {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        // let gpu_probability = self.config.weighted_random_config.clone().unwrap().gpu_probability;
        // let gpu_probability = self.config.weighted_random_config.gpu_probability;
        let gpu_probability = self
            .config
            .weighted_random_config
            .as_ref()
            .map(|cfg| cfg.gpu_probability)
            .unwrap_or(0.85);

        let mut rng = rand::rng();
        let roll: f64 = rng.random_range(0.0..1.0);

        let selected = if roll < gpu_probability {
            Compute::GPU
        } else {
            Compute::CPU
        };
        tracing::info!(tid=?tid, %gpu_probability, "GPU Probability");
        tracing::info!(tid=?tid, selected=?selected, "WeightedRandom chose device");

        let mut lck = self.dispatch_state.write();
        lck.select_device_for_fn(&reg.fqdn, &selected, self.clock.now());

        (selected, NO_ESTIMATE, NO_ESTIMATE)
    }
}
