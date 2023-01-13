use std::sync::Arc;
use crate::services::containers::structs::ContainerState;
use crate::services::invocation::invoker_trait::create_concurrency_semaphore;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::characteristics_map::compare_f64;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime, characteristics_map::CharacteristicsMap};
use iluvatar_library::logging::LocalTime;
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tracing::debug;
use super::{invoker_trait::{Invoker, monitor_queue}, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct ColdPriEnqueuedInvocation {
  x: Arc<EnqueuedInvocation>,
  priority: f64
}

impl ColdPriEnqueuedInvocation {
  pub fn new(x: Arc<EnqueuedInvocation>, priority: f64) -> Self {
    ColdPriEnqueuedInvocation { x, priority }
  }
}
impl Eq for ColdPriEnqueuedInvocation {
}
impl Ord for ColdPriEnqueuedInvocation {
  fn cmp(&self, other: &Self) -> Ordering {
      compare_f64( &self.priority, &other.priority )
  }
}

impl PartialOrd for ColdPriEnqueuedInvocation {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(compare_f64( &self.priority, &other.priority ))
  }
}

impl PartialEq for ColdPriEnqueuedInvocation {
  fn eq(&self, other: &Self) -> bool {
      self.priority == other.priority
  }
}

pub struct ColdPriorityInvoker {
  cont_manager: Arc<ContainerManager>,
  async_functions: AsyncHelper,
  function_config: Arc<FunctionLimits>,
  invocation_config: Arc<InvocationConfig>,
  invoke_queue: Arc<Mutex<BinaryHeap<Arc<ColdPriEnqueuedInvocation>>>>,
  cmap: Arc<CharacteristicsMap>,
  _worker_thread: std::thread::JoinHandle<()>,
  queue_signal: Notify,
  clock: LocalTime,
  concurrency_semaphore: Arc<Semaphore>,
}

impl ColdPriorityInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(ColdPriorityInvoker::wait_on_queue), Some(function_config.cpu_max as usize))?;
    let svc = Arc::new(ColdPriorityInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?,
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      queue_signal: Notify::new(),
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
      _worker_thread: handle,
      clock: LocalTime::new(tid)?,
    });
    tx.send(svc.clone())?;
    debug!(tid=%tid, "Created ColdPriorityInvoker");
    Ok(svc)
  }

  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<ColdPriorityInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
}

impl Invoker for ColdPriorityInvoker {
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
    if self.cont_manager.outstanding(&item.fqdn) == 0 {
      priority = self.cmap.get_warm_time(&item.fqdn);
    }
    priority = match self.cont_manager.container_available(&item.fqdn) {
      ContainerState::Warm => priority,
      ContainerState::Prewarm => self.cmap.get_prewarm_time(&item.fqdn),
      _ => self.cmap.get_cold_time(&item.fqdn),
    };
    let mut queue = self.invoke_queue.lock();
    queue.push(ColdPriEnqueuedInvocation::new(item.clone(), priority).into());
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.fqdn,
                        queue.peek().unwrap().x.fqdn );
    self.queue_signal.notify_waiters();
  }
}
