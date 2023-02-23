use std::sync::Arc;
use crate::services::containers::structs::ContainerState;
use crate::services::invocation::create_concurrency_semaphore;
use crate::services::invocation::invoker_structs::MinHeapEnqueuedInvocation;
use crate::worker_api::worker_config::InvocationConfig;
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{nproc, load_avg};
use iluvatar_library::threading::tokio_thread;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::{debug, error, info};
use super::InvokerQueuePolicy;
use super::invoker_structs::MinHeapFloat;
use super::{invoker_structs::EnqueuedInvocation};
use std::collections::BinaryHeap;

/// An invoker that scales concurrency based on system load
/// Prioritizes based on container availability
/// Increases concurrency by 1 every [InvocationConfig::concurrency_udpate_check_ms]
/// If system load is above [InvocationConfig::max_load], then the concurrency is reduced by half the distance to [InvocationConfig::concurrent_invokes] rounded up
pub struct AvailableScalingInvoker {
  cont_manager: Arc<ContainerManager>,
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
  cmap: Arc<CharacteristicsMap>,
  _load_thread: tokio::task::JoinHandle<()>,
  concurrency_semaphore: Arc<Semaphore>,
  max_concur: u64,
  min_concur: u64,
  current_concur: Mutex<u64>,
  max_load: f64,
  cores: f64,
}

impl AvailableScalingInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let check_dur = invocation_config.concurrency_udpate_check_ms.ok_or_else(|| anyhow::anyhow!("concurrency_udpate_check_ms was not present in InvocationConfig"))?;
    if check_dur == 0 {
      anyhow::bail!("Cannot have a 'concurrency_udpate_check_ms' of 0");
    }
    let max_concur = invocation_config.max_concurrency.ok_or_else(|| anyhow::anyhow!("max_concurrency was not present in InvocationConfig"))?;
    if max_concur == 0 {
      anyhow::bail!("Cannot have a 'max_concurrency' of 0");
    }
    let max_load = invocation_config.max_load.ok_or_else(|| anyhow::anyhow!("max_load was not present in InvocationConfig"))?;
    if max_load == 0.0 {
      anyhow::bail!("Cannot have a 'max_load' of 0");
    }
    let cores = nproc(tid, false)?;

    let (load_handle, laod_tx) = tokio_thread(check_dur, INVOKER_QUEUE_WORKER_TID.clone(), Self::monitor_load);
    let svc = Arc::new(AvailableScalingInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?.ok_or(anyhow::anyhow!("Must provide `concurrent_invokes`"))?,
      min_concur: invocation_config.concurrent_invokes.ok_or(anyhow::anyhow!("Must provide `concurrent_invokes`"))? as u64,
      current_concur: Mutex::new(invocation_config.concurrent_invokes.ok_or(anyhow::anyhow!("Must provide `concurrent_invokes`"))? as u64),
      max_load,
      max_concur,
      cont_manager,
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
      _load_thread: load_handle,
      cores: cores as f64 
    });
    laod_tx.send(svc.clone())?;
    debug!(tid=%tid, "Created AvailableScalingInvoker");
    Ok(svc)
  }

  async fn monitor_load(svc: Arc<AvailableScalingInvoker>, tid: TransactionId) {
    let load_avg = load_avg(&tid);
    if load_avg < 0.0 {
      return;
    }
    let norm_load = load_avg / svc.cores;
    let current = *svc.current_concur.lock();
    if norm_load > svc.max_load {
      let change = current - svc.min_concur;
      let change = f64::ceil(change as f64 / 2.0) as u64;
      if change > 0 {
        match svc.concurrency_semaphore.acquire_many(change as u32).await {
          Ok(s) => {
            s.forget();
            *svc.current_concur.lock() = u64::max(svc.min_concur, current - change);
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

impl InvokerQueuePolicy for AvailableScalingInvoker {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    let r = r.item.clone();
    return Some(r);
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.item.clone();
    let top = invoke_queue.peek();
    let func_name; 
    match top {
        Some(e) => func_name = e.item.registration.function_name.as_str(),
        None => func_name = "empty"
    }
    debug!(tid=%v.tid,  component="minheap", "Popped item from queue - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.registration.function_name, func_name );
    v
  }

  fn concurrency_semaphore(&self) -> Option<&Arc<Semaphore>> {
    Some(&self.concurrency_semaphore)
  }

  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut priority = 0.0;
    if self.cont_manager.outstanding(Some(&item.registration.fqdn)) == 0 {
      priority = self.cmap.get_warm_time(&item.registration.fqdn);
    }
    priority = match self.cont_manager.container_available(&item.registration.fqdn) {
      ContainerState::Warm => priority,
      ContainerState::Prewarm => self.cmap.get_prewarm_time(&item.registration.fqdn),
      _ => self.cmap.get_cold_time(&item.registration.fqdn),
    };
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), priority).into());
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.fqdn,
                        queue.peek().unwrap().item.registration.fqdn );
  }
}
