use std::sync::Arc;
use crate::services::containers::structs::ContainerState;
use crate::services::invocation::invoker_trait::create_concurrency_semaphore;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::characteristics_map::compare_f64;
use iluvatar_library::{nproc, load_avg};
use iluvatar_library::threading::tokio_thread;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime, characteristics_map::CharacteristicsMap};
use iluvatar_library::logging::LocalTime;
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, error, info};
use super::{invoker_trait::{Invoker, monitor_queue}, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct AvailScaleEnqueuedInvocation {
  x: Arc<EnqueuedInvocation>,
  priority: f64
}

impl AvailScaleEnqueuedInvocation {
  pub fn new(x: Arc<EnqueuedInvocation>, priority: f64) -> Self {
    AvailScaleEnqueuedInvocation { x, priority }
  }
}
impl Eq for AvailScaleEnqueuedInvocation {
}
impl Ord for AvailScaleEnqueuedInvocation {
  fn cmp(&self, other: &Self) -> Ordering {
      compare_f64( &self.priority, &other.priority )
  }
}

impl PartialOrd for AvailScaleEnqueuedInvocation {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(compare_f64( &self.priority, &other.priority ))
  }
}

impl PartialEq for AvailScaleEnqueuedInvocation {
  fn eq(&self, other: &Self) -> bool {
      self.priority == other.priority
  }
}

/// An invoker that scales concurrency based on system load
/// Prioritizes based on container availability
/// Increases concurrency by 1 every [InvocationConfig::concurrency_udpate_check_ms]
/// If system load is above [InvocationConfig::max_load], then the concurrency is reduced by half the distance to [InvocationConfig::concurrent_invokes] rounded up
pub struct AvailableScalingInvoker {
  cont_manager: Arc<ContainerManager>,
  async_functions: AsyncHelper,
  function_config: Arc<FunctionLimits>,
  invocation_config: Arc<InvocationConfig>,
  invoke_queue: Arc<Mutex<BinaryHeap<Arc<AvailScaleEnqueuedInvocation>>>>,
  cmap: Arc<CharacteristicsMap>,
  _worker_thread: std::thread::JoinHandle<()>,
  _load_thread: tokio::task::JoinHandle<()>,
  queue_signal: Notify,
  clock: LocalTime,
  concurrency_semaphore: Arc<Semaphore>,
  max_concur: u64,
  min_concur: u64,
  current_concur: Mutex<u64>,
  max_load: f64,
  cores: f64,
}

impl AvailableScalingInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(Self::wait_on_queue), Some(function_config.cpu_max as usize))?;
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
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?,
      min_concur: invocation_config.concurrent_invokes as u64,
      current_concur: Mutex::new(invocation_config.concurrent_invokes as u64),
      max_load,
      max_concur,
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      queue_signal: Notify::new(),
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
      _worker_thread: handle,
      _load_thread: load_handle,
      clock: LocalTime::new(tid)?,
      cores: cores as f64 
    });
    tx.send(svc.clone())?;
    laod_tx.send(svc.clone())?;
    debug!(tid=%tid, "Created AvailableScalingInvoker");
    Ok(svc)
  }

  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<AvailableScalingInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
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
        svc.queue_signal.notify_waiters();
        *svc.current_concur.lock() += 1;
      }
    }
    info!(tid=%tid, concurrency=*svc.current_concur.lock(), load=norm_load, "Current concurrency");
  }
}

impl Invoker for AvailableScalingInvoker {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    let r = r.x.clone();
    return Some(r);
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.x.clone();
    let top = invoke_queue.peek();
    let func_name; 
    match top {
        Some(e) => func_name = e.x.function_name.as_str(),
        None => func_name = "empty"
    }
    debug!(tid=%v.tid,  component="minheap", "Popped item from queue - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.function_name, func_name );
    v
  }

  fn cont_manager(&self) -> Arc<ContainerManager>  {
    self.cont_manager.clone()
  }
  fn function_config(&self) -> Arc<FunctionLimits>  {
    self.function_config.clone()
  }
  fn invocation_config(&self) -> Arc<InvocationConfig>  {
    self.invocation_config.clone()
  }
  fn queue_len(&self) -> usize {
    self.invoke_queue.lock().len()
  }
  fn timer(&self) -> &LocalTime {
    &self.clock
  }
  fn async_functions<'a>(&'a self) -> &'a AsyncHelper {
    &self.async_functions
  }
  fn concurrency_semaphore(&self) -> Option<Arc<Semaphore>> {
    Some(self.concurrency_semaphore.clone())
  }
  fn running_funcs(&self) -> u32 {
    self.invocation_config.concurrent_invokes - self.concurrency_semaphore.available_permits() as u32
  }
  fn char_map(&self) -> &Arc<CharacteristicsMap> {
    &self.cmap
  }

  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut priority = 0.0;
    if self.cont_manager.outstanding(Some(&item.fqdn)) == 0 {
      priority = self.cmap.get_warm_time(&item.fqdn);
    }
    priority = match self.cont_manager.container_available(&item.fqdn) {
      ContainerState::Warm => priority,
      ContainerState::Prewarm => self.cmap.get_prewarm_time(&item.fqdn),
      _ => self.cmap.get_cold_time(&item.fqdn),
    };
    let mut queue = self.invoke_queue.lock();
    queue.push(AvailScaleEnqueuedInvocation::new(item.clone(), priority).into());
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.fqdn,
                        queue.peek().unwrap().x.fqdn );
    self.queue_signal.notify_waiters();
  }
}
