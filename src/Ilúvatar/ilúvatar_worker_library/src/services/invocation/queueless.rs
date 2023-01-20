use std::{sync::{Arc, atomic::{AtomicU32, Ordering}}, time::Duration};
use crate::{worker_api::worker_config::{FunctionLimits, InvocationConfig}};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, logging::LocalTime, utils::calculate_fqdn, threading::tokio_runtime, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use tokio::sync::Notify;
use tracing::{error, debug, info};
use super::{invoker_trait::{Invoker, monitor_queue}, async_tracker::AsyncHelper, invoker_structs::{EnqueuedInvocation, InvocationResultPtr, InvocationResult}};

/// This implementation does not support [crate::worker_api::worker_config::InvocationConfig::concurrent_invokes]
pub struct QueuelessInvoker {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: AsyncHelper,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  async_queue: parking_lot::Mutex<Vec<Arc<EnqueuedInvocation>>>,
  _worker_thread: std::thread::JoinHandle<()>,
  queue_signal: Notify,
  clock: LocalTime,
  cmap: Arc<CharacteristicsMap>,
  running_funcs: AtomicU32,
}

impl QueuelessInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(Self::wait_on_queue), Some(function_config.cpu_max as usize))?;
    let svc = Arc::new(QueuelessInvoker {
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      clock: LocalTime::new(tid)?,
      running_funcs: AtomicU32::new(0),
      queue_signal: Notify::new(),
      async_queue: parking_lot::Mutex::new(Vec::new()),
      _worker_thread: handle,
      cmap,
    });
    tx.send(svc.clone())?;
    Ok(svc)
  }
  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<Self>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
}

#[tonic::async_trait]
impl Invoker for QueuelessInvoker {
  fn cont_manager(&self) -> &Arc<ContainerManager>  { &self.cont_manager }
  fn function_config(&self) -> &Arc<FunctionLimits>  { &self.function_config }
  fn invocation_config(&self) -> &Arc<InvocationConfig>  { &self.invocation_config }
  fn timer(&self) -> &LocalTime { &self.clock }
  fn async_functions<'a>(&'a self) -> &'a AsyncHelper { &self.async_functions }
  fn char_map(&self) -> &Arc<CharacteristicsMap> {
    &self.cmap
  }
  fn concurrency_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> { None }
  fn running_funcs(&self) -> u32 {
    self.running_funcs.load(Ordering::Relaxed)
  }
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let q = self.async_queue.lock();
    if let Some(r) = q.get(0) {
      return Some(r.clone())
    }
    None
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    self.async_queue.lock().remove(0)
  }
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.async_queue.lock();
    queue.push(item.clone());
    debug!(tid=%item.tid, "Added item to front of queue; waking worker thread");
    self.queue_signal.notify_waiters();
  }
  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<InvocationResultPtr> {
    let fqdn = calculate_fqdn(&function_name, &function_version);
    self.running_funcs.fetch_add(1, Ordering::Relaxed);
    let r = self.invoke_internal(&fqdn, &function_name, &function_version, &json_args, &tid, self.timer().now(), None).await;
    self.running_funcs.fetch_sub(1, Ordering::Relaxed);
    let (result, dur) = r?;
    let r: InvocationResultPtr = InvocationResult::boxed();
    let mut temp = r.lock();
    temp.exec_time = result.duration_sec;
    temp.result_json = result.result_string()?;
    temp.worker_result = Some(result);
    temp.duration = dur;
    info!(tid=%tid, "Invocation complete");
    Ok( r.clone() )
  }

  fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
    let mut result_ptr = item.result_ptr.lock();
    error!(tid=%item.tid, attempts=result_ptr.attempts, "Abandoning attempt to run invocation after attempts");
    result_ptr.duration = Duration::from_micros(0);
    result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", cause);
    result_ptr.completed = true;
    item.signal();
  }
}
