use std::sync::Arc;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::logging::LocalTime;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime};
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, info};
use super::invoker_trait::{monitor_queue, create_concurrency_semaphore};
use super::{invoker_trait::Invoker, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};

pub struct FCFSInvoker {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: AsyncHelper,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  pub invoke_queue: Arc<Mutex<Vec<Arc<EnqueuedInvocation>>>>,
  _worker_thread: std::thread::JoinHandle<()>,
  queue_signal: Notify,
  clock: LocalTime,
  concurrency_semaphore: Arc<Semaphore>,
}

impl FCFSInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(FCFSInvoker::wait_on_queue), Some(function_config.cpu_max as usize));
    let svc = Arc::new(FCFSInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?,
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      queue_signal: Notify::new(),
      invoke_queue: Arc::new(Mutex::new(Vec::new())),
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
    let q = self.invoke_queue.lock();
    if let Some(r) = q.get(0) {
      return Some(r.clone())
    }
    None
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    self.invoke_queue.lock().remove(0)
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

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<super::invoker_structs::InvocationResultPtr> {
    let queued = self.enqueue_new_invocation(function_name, function_version, json_args, tid.clone());
    queued.wait(&tid).await?;
    let result_ptr = queued.result_ptr.lock();
    match result_ptr.completed {
      true => {
        info!(tid=%tid, "Invocation complete");
        Ok( queued.result_ptr.clone() )
      },
      false => {
        anyhow::bail!("Invocation was signaled completion but completion value was not set")
      }
    }
  }
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    match index {
      Some(i) => queue.insert(i, item.clone()),
      None => queue.push(item.clone()),
    };
    debug!(tid=%item.tid, "Added item to front of queue; waking worker thread");
    self.queue_signal.notify_waiters();
  }
}
