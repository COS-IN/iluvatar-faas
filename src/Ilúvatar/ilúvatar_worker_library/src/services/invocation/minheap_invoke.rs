use std::{sync::Arc, time::Duration};
use crate::services::containers::structs::{InsufficientCoresError, InsufficientMemoryError};
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime, characteristics_map::{Characteristics,CharacteristicsMap,AgExponential,Values}};
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::{debug, warn, error, info};
use super::{invoker_trait::Invoker, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};
use crate::rpc::InvokeResponse;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct MHQEnqueuedInvocation {
    x: Arc<EnqueuedInvocation>
}

impl MHQEnqueuedInvocation {
    fn new( x: Arc<EnqueuedInvocation> ) -> Self {
        MHQEnqueuedInvocation {
            x
        }
    }
}

impl Eq for MHQEnqueuedInvocation {
}

impl Ord for MHQEnqueuedInvocation {
 fn cmp(&self, other: &Self) -> Ordering {
     other.x.function_name.len().cmp(&self.x.function_name.len())
 }
}

impl PartialOrd for MHQEnqueuedInvocation {
 fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
     Some(self.cmp(other))
 }
}


impl PartialEq for MHQEnqueuedInvocation {
 fn eq(&self, other: &Self) -> bool {
     self.x.function_name.len() == other.x.function_name.len()
 }
}

pub struct MinHeapInvoker {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: AsyncHelper,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  pub invoke_queue: Arc<Mutex<BinaryHeap<Arc<MHQEnqueuedInvocation>>>>,
  pub cmap: Arc<CharacteristicsMap>,
  _worker_thread: std::thread::JoinHandle<()>,
  queue_signal: Notify
}

impl MinHeapInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), MinHeapInvoker::monitor_queue, Some(MinHeapInvoker::wait_on_queue), Some(function_config.cpu_max as usize));
    let svc = Arc::new(MinHeapInvoker {
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      queue_signal: Notify::new(),
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap: Arc::new(CharacteristicsMap::new(AgExponential::new(0.6))),
      _worker_thread: handle,
    });
    tx.send(svc.clone())?;
    Ok(svc)
  }

  /// Check the invocation queue, running things when there are sufficient resources
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(invoker_svc), fields(tid=%_tid)))]
  async fn monitor_queue(invoker_svc: Arc<MinHeapInvoker>, _tid: TransactionId) {
    while invoker_svc.queue_has_runnable_items() {
      let mut invoke_queue = invoker_svc.invoke_queue.lock();
      let item = invoke_queue.pop().unwrap();
      let item = Arc::try_unwrap(item).expect("item has multiple owners");
      let item = item.x;
      debug!(tid=%item.tid, "Popped item from queue minheap - len: {} popped: {} top: {} ", 
                        invoke_queue.len(),
                        item.function_name,
                        invoke_queue.peek().unwrap().x.function_name );

      // TODO: continuity of spans here
      invoker_svc.spawn_tokio_worker(invoker_svc.clone(), item.into());
    }
  }
  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<MinHeapInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
  /// True if there are things in the queue, and resources to run them on
  fn queue_has_runnable_items(&self) -> bool {
    let invoke_queue = self.invoke_queue.lock();
    invoke_queue.len() > 0 && self.has_resources_to_run()
  }
}

#[tonic::async_trait]
impl Invoker for MinHeapInvoker {
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

  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<(String, Duration)> {
    // self.invoke_internal(&function_name, &function_version, &json_args, &tid).await
    let queued = self.enqueue_new_invocation(function_name.clone(), function_version, json_args, tid.clone());
    queued.wait(&tid).await?;
    let result_ptr = queued.result_ptr.lock();
    match result_ptr.completed {
      true => {
        info!(tid=%tid, "Invocation complete");
        self.cmap.add( function_name, Characteristics::ExecTime, Values::Duration(result_ptr.duration.clone()), Some(true) );
        Ok( (result_ptr.result_json.clone(), result_ptr.duration) )  
      },
      false => {
        anyhow::bail!("Invocation was signaled completion but completion value was not set")
      }
    }
  }
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    queue.push(MHQEnqueuedInvocation::new(item.clone()).into());
    debug!(tid=%item.tid, "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.function_name,
                        queue.peek().unwrap().x.function_name );
    // self.cmap.dump();
    self.queue_signal.notify_waiters();
  }

  fn async_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<String> {
    let invoke = self.enqueue_new_invocation(function_name, function_version, json_args, tid);
    self.async_functions.insert_async_invoke(invoke)
  }
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<InvokeResponse> {
    self.async_functions.invoke_async_check(cookie, tid)
  }

  fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
    if let Some(_core_err) = cause.downcast_ref::<InsufficientCoresError>() {
      debug!(tid=%item.tid, "Insufficient cores to run item right now");
      self.add_item_to_queue(&item, Some(0));
    } else if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
      warn!(tid=%item.tid, "Insufficient memory to run item right now");
      self.add_item_to_queue(&item, Some(0));
    } else {
      error!(tid=%item.tid, error=%cause, "Encountered unknown error while trying to run queued invocation");
      let mut result_ptr = item.result_ptr.lock();
      if result_ptr.attempts >= self.invocation_config().retries {
        error!(tid=%item.tid, attempts=result_ptr.attempts, "Abandoning attempt to run invocation after attempts");
        result_ptr.duration = Duration::from_micros(0);
        result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", cause);
        result_ptr.completed = true;
        item.signal();
      } else {
        result_ptr.attempts += 1;
        debug!(tid=%item.tid, attempts=result_ptr.attempts, "re-queueing invocation attempt after attempting");
        self.add_item_to_queue(&item, Some(0));
      }
    }
  }
}
