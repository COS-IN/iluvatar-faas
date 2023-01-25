use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use crate::services::invocation::invoker_trait::create_concurrency_semaphore;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime, characteristics_map::CharacteristicsMap};
use iluvatar_library::logging::LocalTime;
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, info};
use super::{invoker_trait::{Invoker, monitor_queue}, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};
use std::collections::BinaryHeap;
use std::cmp::Ordering; 

#[derive(Debug)]
pub struct FCFSBPEnqueuedInvocation {
  x: Arc<EnqueuedInvocation>,
}

impl FCFSBPEnqueuedInvocation {
  fn new( x: Arc<EnqueuedInvocation> ) -> Self {
    FCFSBPEnqueuedInvocation {
      x,
    }
  }
}

impl Eq for FCFSBPEnqueuedInvocation {
}
impl Ord for FCFSBPEnqueuedInvocation {
  fn cmp(&self, other: &Self) -> Ordering {
    self.x.queue_insert_time.cmp(&other.x.queue_insert_time)
  }
}
impl PartialOrd for FCFSBPEnqueuedInvocation {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.x.queue_insert_time.cmp(&other.x.queue_insert_time))
  }
}
impl PartialEq for FCFSBPEnqueuedInvocation {
  fn eq(&self, other: &Self) -> bool {
   self.x.queue_insert_time == other.x.queue_insert_time
  }
}

/// A First-Come-First-Serve queue management policy
/// Items with an execution duration of less than [InvocationConfig::bypass_duration_ms] will skip the queue
/// In the event of a cold start on such an invocation, it will be enqueued
pub struct FCFSBypassInvoker {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: AsyncHelper,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  pub invoke_queue: Arc<Mutex<BinaryHeap<Arc<FCFSBPEnqueuedInvocation>>>>,
  pub cmap: Arc<CharacteristicsMap>,
  _worker_thread: std::thread::JoinHandle<()>,
  queue_signal: Notify,
  clock: LocalTime,
  concurrency_semaphore: Arc<Semaphore>,
  bypass_running: AtomicU32
}

impl FCFSBypassInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(FCFSBypassInvoker::wait_on_queue), Some(function_config.cpu_max as usize))?;
    if invocation_config.bypass_duration_ms <= 0 {
      anyhow::bail!("Cannot have a bypass_duration_ms of zero for this invoker queue policy")
    }
    let svc = Arc::new(FCFSBypassInvoker {
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
      bypass_running: AtomicU32::new(0)
    });
    tx.send(svc.clone())?;
    info!(tid=%tid, "Created FCFSBypassInvoker");
    Ok(svc)
  }

  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<FCFSBypassInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
}

#[tonic::async_trait]
impl Invoker for FCFSBypassInvoker {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    Some(r.x.clone())
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.x.clone();
    let mut func_name = "empty"; 
    if let Some(e) = invoke_queue.peek() {
      func_name = e.x.function_name.as_str();
    }
    debug!(tid=%v.tid,  "Popped item from queue fcfs heap - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.function_name, func_name );
    v
  }

  fn cont_manager(&self) -> &Arc<ContainerManager>  { &self.cont_manager }
  fn function_config(&self) -> &Arc<FunctionLimits>  { &self.function_config }
  fn invocation_config(&self) -> &Arc<InvocationConfig>  { &self.invocation_config }
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
  fn char_map(&self) -> &Arc<CharacteristicsMap> {
    &self.cmap
  }
  fn bypass_running(&self) -> Option<&AtomicU32> { Some(&self.bypass_running) }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    queue.push(FCFSBPEnqueuedInvocation::new(item.clone()).into());
    self.queue_signal.notify_waiters();
  }
}
