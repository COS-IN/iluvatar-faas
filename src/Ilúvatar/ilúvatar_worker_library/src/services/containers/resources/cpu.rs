use std::sync::Arc;
use crate::worker_api::worker_config::ContainerResourceConfig;
use iluvatar_library::types::ComputeEnum;
use iluvatar_library::{nproc, load_avg};
use iluvatar_library::threading::tokio_thread;
use iluvatar_library::transaction::TransactionId;
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::{debug, error, info};

lazy_static::lazy_static! {
  pub static ref CPU_CONCUR_WORKER_TID: TransactionId = "CPUConcurrencyMonitor".to_string();
}

/// An invoker that scales concurrency based on system load
/// Prioritizes based on container availability
/// Increases concurrency by 1 every [InvocationConfig::concurrency_udpate_check_ms]
/// If system load is above [InvocationConfig::max_load], then the concurrency is reduced by half the distance to [InvocationConfig::concurrent_invokes] rounded up
pub struct CPUResourceMananger {
  _load_thread: Option<tokio::task::JoinHandle<()>>,
  concurrency_semaphore: Arc<Semaphore>,
  max_concur: u32,
  min_concur: u32,
  current_concur: Mutex<u32>,
  max_load: Option<f64>,
  cores: f64,
}

impl CPUResourceMananger {
  pub fn new(config: Arc<ContainerResourceConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let config = match config.resource_map.get(&ComputeEnum::CPU) {
      Some(c) => c.clone(),
      None => anyhow::bail!("Did not have a CPU entry in the `resource_map`"),
    };
    let max_concur = config.max_oversubscribe.ok_or_else(|| anyhow::anyhow!("max_concurrency was not present in InvocationConfig"))?;
    if max_concur == 0 {
      anyhow::bail!("Cannot have a 'max_concurrency' of 0");
    }
    let cores = nproc(tid, false)?;
    let sem = match config.count {
      0 => anyhow::bail!("Invoker concurrency semaphore cannot have 0 permits"),
      p => Arc::new(Semaphore::new(p as usize)),
    };
    let start_concurr = match config.concurrency_udpate_check_ms {
      Some(_) => config.count,
      None => max_concur,
    };

    let (load_handle, laod_tx) = match config.concurrency_udpate_check_ms {
      Some(check_dur) =>  {
        let max_load = config.max_load.ok_or_else(|| anyhow::anyhow!("max_load was not present in InvocationConfig"))?;
        if max_load == 0.0 {
          anyhow::bail!("Cannot have a 'max_load' of 0");
        }
        let (h, tx) = tokio_thread(check_dur, CPU_CONCUR_WORKER_TID.clone(), Self::monitor_load);
        (Some(h), Some(tx))
      },
      None => (None, None)
    };
    let svc = Arc::new(CPUResourceMananger {
      concurrency_semaphore: sem,
      min_concur: config.count,
      current_concur: Mutex::new(start_concurr),
      max_load: config.max_load,
      max_concur,
      _load_thread: load_handle,
      cores: cores as f64 
    });
    if let Some(laod_tx) = laod_tx {
      laod_tx.send(svc.clone())?;
    }
    debug!(tid=%tid, "Created CPUResourceMananger");
    Ok(svc)
  }

  async fn monitor_load(svc: Arc<CPUResourceMananger>, tid: TransactionId) {
    let load_avg = load_avg(&tid);
    if load_avg < 0.0 {
      return;
    }
    let norm_load = load_avg / svc.cores;
    let current = *svc.current_concur.lock();
    if norm_load > svc.max_load.unwrap() {
      let change = current - svc.min_concur;
      let change = f64::ceil(change as f64 / 2.0) as u32;
      if change > 0 {
        match svc.concurrency_semaphore.acquire_many(change as u32).await {
          Ok(s) => {
            s.forget();
            *svc.current_concur.lock() = u32::max(svc.min_concur, current - change);
          },
          Err(e) => error!(tid=%tid, error=%e, "Failed to acquire concurrency semaphore"),
        };
      }
    } else {
      if current < svc.max_concur {
        svc.concurrency_semaphore.add_permits(1);
        *svc.current_concur.lock() += 1;
      }
    }
    info!(tid=%tid, concurrency=*svc.current_concur.lock(), load=norm_load, "Current concurrency");
  }
}