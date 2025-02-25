use crate::services::registration::RegisteredFunction;
use crate::services::status::status_service::LoadAvg;
use crate::worker_api::worker_config::CPUResourceConfig;
use anyhow::Result;
use iluvatar_library::threading::tokio_thread;
use iluvatar_library::transaction::TransactionId;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error};

lazy_static::lazy_static! {
  pub static ref CPU_CONCUR_WORKER_TID: TransactionId = "CPUConcurrencyMonitor".to_string();
}

/// An invoker that scales concurrency based on system load.
/// Prioritizes based on container availability.
/// Increases concurrency by 1 every [crate::worker_api::worker_config::ComputeResourceConfig::concurrency_update_check_ms].
/// If system load is above [crate::worker_api::worker_config::ComputeResourceConfig::max_load], then the concurrency is reduced by half the distance to [crate::worker_api::worker_config::ComputeResourceConfig::max_oversubscribe] rounded up.
pub struct CpuResourceTracker {
    _load_thread: Option<tokio::task::JoinHandle<()>>,
    concurrency_semaphore: Option<Arc<Semaphore>>,
    max_concur: u32,
    min_concur: u32,
    current_concur: Mutex<u32>,
    max_load: Option<f64>,
    pub cores: f64,
    load_avg: LoadAvg,
}

impl CpuResourceTracker {
    pub fn new(config: &Arc<CPUResourceConfig>, load_avg: LoadAvg, tid: &TransactionId) -> Result<Arc<Self>> {
        let mut max_concur = 0;
        let available_cores = match config.count {
            0 => num_cpus::get(),
            n => n as usize,
        };
        let sem = match config.count {
            0 => None,
            p => Some(Arc::new(Semaphore::new(p as usize))),
        };

        let (load_handle, load_tx) = match config.concurrency_update_check_ms {
            Some(0) => (None, None),
            Some(check_dur) => {
                let max_load = config
                    .max_load
                    .ok_or_else(|| anyhow::anyhow!("max_load was not present in InvocationConfig"))?;
                if max_load == 0.0 {
                    anyhow::bail!("Cannot have a 'max_load' of 0");
                }
                max_concur = config
                    .max_oversubscribe
                    .ok_or_else(|| anyhow::anyhow!("max_oversubscribe was not present in InvocationConfig"))?;
                if max_concur == 0 {
                    anyhow::bail!("Cannot have a 'max_concurrency' of 0");
                }

                let (h, tx) = tokio_thread(check_dur, CPU_CONCUR_WORKER_TID.clone(), Self::monitor_load);
                (Some(h), Some(tx))
            },
            None => (None, None),
        };
        let svc = Arc::new(CpuResourceTracker {
            concurrency_semaphore: sem,
            min_concur: config.count,
            current_concur: Mutex::new(config.count),
            max_load: config.max_load,
            max_concur,
            _load_thread: load_handle,
            cores: available_cores as f64,
            load_avg,
        });
        if let Some(load_tx) = load_tx {
            load_tx.send(svc.clone())?;
        }
        debug!(tid=%tid, "Created CPUResourceMananger");
        Ok(svc)
    }

    /// Return a permit for the function to run on its registered number of cores.
    /// If the semaphore is [None], then no permits are being tracked.
    pub fn try_acquire_cores(
        &self,
        reg: &Arc<RegisteredFunction>,
        _tid: &TransactionId,
    ) -> Result<Option<OwnedSemaphorePermit>, tokio::sync::TryAcquireError> {
        if let Some(sem) = &self.concurrency_semaphore {
            return match sem.clone().try_acquire_many_owned(reg.cpus) {
                Ok(p) => Ok(Some(p)),
                Err(e) => Err(e),
            };
        }
        Ok(None)
    }

    /// Blocks until an owned permit can be acquired for the function to run on its registered number of cores.
    /// If the semaphore is [None], then no permits are being tracked.
    pub async fn acquire_cores(
        &self,
        reg: &Arc<RegisteredFunction>,
        _tid: &TransactionId,
    ) -> Result<Option<OwnedSemaphorePermit>, tokio::sync::AcquireError> {
        if let Some(sem) = &self.concurrency_semaphore {
            return match sem.clone().acquire_many_owned(reg.cpus).await {
                Ok(p) => Ok(Some(p)),
                Err(e) => Err(e),
            };
        }
        Ok(None)
    }

    pub fn available_cores(&self) -> usize {
        if let Some(sem) = &self.concurrency_semaphore {
            return sem.available_permits();
        }
        self.cores as usize
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(svc), fields(tid=%tid)))]
    async fn monitor_load(svc: Arc<CpuResourceTracker>, tid: TransactionId) {
        let norm_load = *svc.load_avg.read() / svc.cores;
        let current = *svc.current_concur.lock();
        if norm_load > svc.max_load.unwrap() {
            let change = current - svc.min_concur;
            let change = f64::ceil(change as f64 / 2.0) as u32;
            if change > 0 {
                if let Some(sem) = &svc.concurrency_semaphore {
                    match sem.acquire_many(change).await {
                        Ok(s) => {
                            s.forget();
                            *svc.current_concur.lock() = u32::max(svc.min_concur, current - change);
                        },
                        Err(e) => {
                            error!(tid=%tid, error=%e, "Failed to acquire concurrency semaphore")
                        },
                    };
                }
            }
        } else if current < svc.max_concur {
            if let Some(sem) = &svc.concurrency_semaphore {
                sem.add_permits(1);
                *svc.current_concur.lock() += 1;
            }
        }
        debug!(tid=%tid, concurrency=*svc.current_concur.lock(), load=norm_load, "Current concurrency");
    }
}
