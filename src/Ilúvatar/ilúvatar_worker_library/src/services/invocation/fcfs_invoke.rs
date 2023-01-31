use std::collections::BinaryHeap;
use std::sync::Arc;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::logging::LocalTime;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime};
use anyhow::Result;
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::sync::{Notify, Semaphore};
use tracing::debug;
use super::invoker_structs::MinHeapEnqueuedInvocation;
use super::invoker_trait::{monitor_queue, create_concurrency_semaphore};
use super::{invoker_trait::Invoker, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};

pub struct FCFSInvoker {
  cont_manager: Arc<ContainerManager>,
  async_functions: AsyncHelper,
  function_config: Arc<FunctionLimits>,
  invocation_config: Arc<InvocationConfig>,
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapEnqueuedInvocation<OffsetDateTime>>>>,
  _worker_thread: std::thread::JoinHandle<()>,
  queue_signal: Notify,
  clock: LocalTime,
  concurrency_semaphore: Arc<Semaphore>,
  cmap: Arc<CharacteristicsMap>,
}

impl FCFSInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(FCFSInvoker::wait_on_queue), Some(function_config.cpu_max as usize))?;
    let svc = Arc::new(FCFSInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?,
      cont_manager, function_config, invocation_config, cmap,
      async_functions: AsyncHelper::new(),
      queue_signal: Notify::new(),
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      _worker_thread: handle,
      clock: LocalTime::new(tid)?,
    });
    tx.send(svc.clone())?;
    Ok(svc)
  }

  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<FCFSInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
}

#[tonic::async_trait]
impl Invoker for FCFSInvoker {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    Some(r.item.clone())
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.item.clone();
    let mut func_name = "empty"; 
    if let Some(e) = invoke_queue.peek() {
      func_name = e.item.function_name.as_str();
    }
    debug!(tid=%v.tid,  "Popped item from queue fcfs heap - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.function_name, func_name );
    v
  }
  fn cont_manager(&self) -> &Arc<ContainerManager>  { &self.cont_manager }
  fn function_config(&self) -> &Arc<FunctionLimits>  { &self.function_config }
  fn invocation_config(&self) -> &Arc<InvocationConfig>  { &self.invocation_config }
  fn queue_len(&self) -> usize { self.invoke_queue.lock().len() }
  fn timer(&self) -> &LocalTime { &self.clock }
  fn async_functions<'a>(&'a self) -> &'a AsyncHelper { &self.async_functions }
  fn concurrency_semaphore(&self) -> Option<Arc<Semaphore>> { Some(self.concurrency_semaphore.clone()) }
  fn running_funcs(&self) -> u32 {
    self.invocation_config.concurrent_invokes - self.concurrency_semaphore.available_permits() as u32
  }
  fn char_map(&self) -> &Arc<CharacteristicsMap> { &self.cmap }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new(item.clone(), item.queue_insert_time));
    self.queue_signal.notify_waiters();
  }
}
