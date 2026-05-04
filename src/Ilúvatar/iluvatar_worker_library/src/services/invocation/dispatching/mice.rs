use crate::services::invocation::dispatching::queueing_dispatcher::DispatchPolicy;
use crate::services::invocation::dispatching::{QueueMap, NO_ESTIMATE};
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::InvocationConfig;
use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::clock::{get_global_clock, Clock};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use parking_lot::Mutex;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::info;

/// Internal state for the MICE (Machine-Learning ICE) policy.
#[derive(Debug)]
struct MiceState {
    tau: f64,
    gpu_work: f64,
    cpu_work: f64,
    count: u64,
    last_update: OffsetDateTime,
}

/// MICE dispatching policy
pub struct Mice {
    que_map: QueueMap,
    cmap: WorkerCharMap,
    state: Mutex<MiceState>,
    m: u64,
    epsilon: f64,
    alpha: f64,
    clock: Clock,
}

impl Mice {
    fn get_gpu_est(&self, fqdn: &str, obs: f64) -> f64 {
        use iluvatar_library::char_map::Value;

        let (prev_est_raw, prev_e2e_raw) = self
            .cmap
            .get_2(fqdn, Chars::EstGpu, Value::Avg, Chars::E2EGpu, Value::Avg);

        let prev_est = prev_est_raw.max(1e-6);
        let prev_e2e = prev_e2e_raw.max(prev_est);
        let obs = obs.max(prev_est);

        let z = prev_e2e - prev_est;
        let alpha = 0.1;
        let beta = 0.7;
        let k = 1.0 - (alpha + beta);

        let xhat = (alpha * prev_est) + (beta * obs) + (k * z);

        self.cmap.update(fqdn, Chars::EstGpu, xhat);

        info!(
            fqdn = fqdn,
            prev_est,
            obs,
            prev_e2e,
            residual = z,
            xhat,
            "MICE_EST: gpu_exec_filter"
        );

        xhat
    }

    pub fn new(
        _invocation_config: Arc<InvocationConfig>,
        cmap: WorkerCharMap,
        que_map: QueueMap,
        tid: &TransactionId,
    ) -> Result<Self> {
        let clock = get_global_clock(tid)?;
        Ok(Self {
            que_map,
            cmap,
            state: Mutex::new(MiceState {
                tau: 10.0,
                gpu_work: 0.0,
                cpu_work: 0.0,
                count: 0,
                last_update: clock.now(),
            }),
            m: 100,
            epsilon: 0.1,
            alpha: 0.8,
            clock,
        })
    }

    /// Use execution-time estimate only (not queueing delay)
    fn job_size_est(&self, fid: &str, gpu_exec_est: f64) -> f64 {
        if gpu_exec_est.is_finite() && gpu_exec_est > 0.0 {
            gpu_exec_est
        } else {
            self.cmap.get_avg(fid, Chars::GpuExecTime).max(1.0)
        }
    }
}

impl DispatchPolicy for Mice {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let (gpu_est_ct, gpu_load) = self
            .que_map
            .get(&Compute::GPU)
            .map(|q| q.est_completion_time(reg, tid))
            .unwrap_or((NO_ESTIMATE, NO_ESTIMATE));

        let (cpu_est_ct, cpu_load) = self
            .que_map
            .get(&Compute::CPU)
            .map(|q| q.est_completion_time(reg, tid))
            .unwrap_or((NO_ESTIMATE, NO_ESTIMATE));

        // Filter execution time estimate (not queueing time)
        let gpu_exec_est = self.get_gpu_est(&reg.fqdn, gpu_est_ct);
        let size = self.job_size_est(&reg.fqdn, gpu_exec_est);

        let now = self.clock.now();
        let mut st = self.state.lock();

        let gpu_available = self.que_map.get(&Compute::GPU).is_some();
        let use_gpu = gpu_available && size < st.tau;

        info!(
            tid = %tid,
            fqdn = %reg.fqdn,
            tau = st.tau,
            job_size = size,
            gpu_available,
            "MICE_DECIDE: input"
        );

        if use_gpu {
            st.gpu_work += size;
        } else {
            st.cpu_work += size;
        }
        st.count += 1;

        // ---- Epoch update ----
        if st.count >= self.m {
            let dt = (now - st.last_update).as_seconds_f64().max(1e-6);

            // Normalize load by capacity (single GPU/CPU)
            let rho_gpu = (st.gpu_work / dt).clamp(0.0, 1.0);
            let rho_cpu = (st.cpu_work / dt).clamp(0.0, 1.0);
            let rho = (rho_gpu + rho_cpu).clamp(0.0, 1.0);

            let target_gpu = (rho + self.alpha * rho.powi(4) * (1.0 - rho)).clamp(0.0, 1.0);

            let tau_old = st.tau;

            // Hysteresis to prevent oscillation
            let tol = 0.02;
            let delta = rho_gpu - target_gpu;

            if delta < -tol {
                st.tau += self.epsilon;
            } else if delta > tol {
                st.tau = (st.tau - self.epsilon).max(0.0);
            }

            info!(
                tid = %tid,
                dt,
                rho,
                rho_gpu,
                rho_cpu,
                target_gpu,
                tau_old,
                tau_new = st.tau,
                "MICE_EPOCH: update"
            );

            st.gpu_work = 0.0;
            st.cpu_work = 0.0;
            st.count = 0;
            st.last_update = now;
        }

        let (dev, load, est) = if use_gpu {
            (Compute::GPU, gpu_load, gpu_est_ct)
        } else {
            (Compute::CPU, cpu_load, cpu_est_ct)
        };

        info!(
            tid = %tid,
            fqdn = %reg.fqdn,
            device = ?dev,
            tau = st.tau,
            "MICE_DECIDE: output"
        );

        (dev, load, est)
    }
}
