use crate::services::invocation::dispatching::{queueing_dispatcher::DispatchPolicy, QueueMap, NO_ESTIMATE};
use crate::services::registration::RegisteredFunction;
use iluvatar_library::char_map::{Chars, Value, WorkerCharMap};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use rand::seq::IndexedRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
pub struct EpsilonGreedyConfig {
    #[serde(default = "default_epsilon")]
    pub epsilon: f64,
}

fn default_epsilon() -> f64 {
    0.05
}

impl Default for EpsilonGreedyConfig {
    fn default() -> Self {
        Self { epsilon: 0.05 }
    }
}

pub struct EpsilonGreedy {
    cmap: WorkerCharMap,
    config: Arc<EpsilonGreedyConfig>,
    que_map: QueueMap,
}

impl EpsilonGreedy {
    pub fn new(cmap: WorkerCharMap, config: &Option<Arc<EpsilonGreedyConfig>>, que_map: QueueMap) -> Self {
        let config = match config {
            Some(c) => c.clone(),
            None => Arc::new(EpsilonGreedyConfig::default()),
        };
        Self { cmap, config, que_map }
    }

    /// Updates the Kalman-filtered GPU estimate stored in cmap and returns
    /// (filtered_estimate, residual_error).
    fn get_gpu_est(&self, fqdn: &str, mqfq_est: f64) -> (f64, f64) {
        let (est, e2e) = self
            .cmap
            .get_2(fqdn, Chars::EstGpu, Value::Avg, Chars::E2EGpu, Value::Avg);
        let prev_est = if est == 0.0 { mqfq_est } else { est };
        let prev_e2e = if e2e == 0.0 { mqfq_est } else { e2e };
        // Kalman Filter (see faasmeter paper)
        let z = prev_e2e - prev_est; // residual error
        let alpha = 0.1;
        let beta = 0.7;
        let k = 1.0 - (beta + alpha);
        let xhat = (alpha * prev_est) + (beta * mqfq_est) + k * z;
        self.cmap.update(fqdn, Chars::EstGpu, xhat);
        (xhat, z)
    }
}

impl DispatchPolicy for EpsilonGreedy {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let r = rand::rng().random_range(0.0..1.0);

        // Get queue-aware E2E estimates for both devices
        let (cpu_est, cpu_load) = match self.que_map.get(&Compute::CPU) {
            Some(q) => q.est_completion_time(reg, tid),
            None => (self.cmap.get_avg(&reg.fqdn, Chars::CpuExecTime), 0.0),
        };
        let (gpu_est_raw, gpu_load) = match self.que_map.get(&Compute::GPU) {
            Some(q) => q.est_completion_time(reg, tid),
            None => (self.cmap.get_avg(&reg.fqdn, Chars::GpuExecTime), 0.0),
        };

        // Apply Kalman filter to GPU estimate (always update so cmap stays current)
        let (final_gpu_est, gpu_est_err) = self.get_gpu_est(&reg.fqdn, gpu_est_raw);

        if r < self.config.epsilon {
            // Explore: pick randomly, but still return the real estimate for the chosen device
            let v: Vec<Compute> = reg.supported_compute.iter().collect();
            let chosen = *v.choose(&mut rand::rng()).unwrap_or(&Compute::CPU);
            let (chosen_load, chosen_est) = match chosen {
                Compute::CPU => (cpu_load, cpu_est),
                Compute::GPU => (gpu_load, final_gpu_est),
                _ => (NO_ESTIMATE, NO_ESTIMATE),
            };
            info!(
                tid = %tid,
                fqdn = %reg.fqdn,
                cpu_est = cpu_est,
                gpu_est = final_gpu_est,
                gpu_est_err = gpu_est_err,
                epsilon = self.config.epsilon,
                chosen = %chosen,
                "EpsilonGreedy Dispatch (Explore)"
            );
            (chosen, chosen_load, chosen_est)
        } else {
            // Exploit: greedy on queue-aware E2E estimate with Kalman-filtered GPU
            let chosen = if cpu_est <= final_gpu_est {
                if reg.supported_compute.contains(Compute::CPU) {
                    Compute::CPU
                } else {
                    Compute::GPU
                }
            } else if reg.supported_compute.contains(Compute::GPU) {
                Compute::GPU
            } else {
                Compute::CPU
            };

            let (chosen_load, chosen_est) = match chosen {
                Compute::CPU => (cpu_load, cpu_est),
                Compute::GPU => (gpu_load, final_gpu_est),
                _ => (NO_ESTIMATE, NO_ESTIMATE),
            };

            info!(
                tid = %tid,
                fqdn = %reg.fqdn,
                mqfq_est = gpu_est_raw,
                gpu_est = final_gpu_est,
                gpu_est_err = gpu_est_err,
                cpu_est = cpu_est,
                cpu_load = cpu_load,
                gpu_load = gpu_load,
                epsilon = self.config.epsilon,
                chosen = %chosen,
                "EpsilonGreedy Dispatch (Exploit)"
            );
            (chosen, chosen_load, chosen_est)
        }
    }
}
