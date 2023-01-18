use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
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

fn time_since_epoch() -> f64 {
    let start = SystemTime::now();
    start.duration_since( UNIX_EPOCH )
         .expect("Time went backwards")
         .as_secs_f64()
}

#[derive(Debug)]
pub struct MHQEDEnqueuedInvocation {
    x: Arc<EnqueuedInvocation>,
    deadline: f64
}

impl MHQEDEnqueuedInvocation {
    fn new( x: Arc<EnqueuedInvocation>, deadline: f64 ) -> Self {
        MHQEDEnqueuedInvocation {
            x,
            deadline
        }
    }
}

impl Eq for MHQEDEnqueuedInvocation {
}

impl Ord for MHQEDEnqueuedInvocation {
 fn cmp(&self, other: &Self) -> Ordering {
    compare_f64( &self.deadline, &other.deadline ).reverse()
 }
}

impl PartialOrd for MHQEDEnqueuedInvocation {
 fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(compare_f64( &self.deadline, &other.deadline ).reverse())
  }
}

impl PartialEq for MHQEDEnqueuedInvocation {
 fn eq(&self, other: &Self) -> bool {
     self.deadline == other.deadline
 }
}

pub struct MinHeapEDInvoker {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: AsyncHelper,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  pub invoke_queue: Arc<Mutex<BinaryHeap<Arc<MHQEDEnqueuedInvocation>>>>,
  pub cmap: Arc<CharacteristicsMap>,
  _worker_thread: std::thread::JoinHandle<()>,
  queue_signal: Notify,
  clock: LocalTime,
  concurrency_semaphore: Arc<Semaphore>,
}

impl MinHeapEDInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(MinHeapEDInvoker::wait_on_queue), Some(function_config.cpu_max as usize))?;
    let svc = Arc::new(MinHeapEDInvoker {
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
    debug!(tid=%tid, "Created MinHeapEDInvoker");
    Ok(svc)
  }

  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<MinHeapEDInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
}

#[tonic::async_trait]
impl Invoker for MinHeapEDInvoker {
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
    debug!(tid=%v.tid,  component="minheap", "Popped item from queue minheap - len: {} popped: {} top: {} ",
           invoke_queue.len(),
           v.function_name,
           func_name );
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
    let mut queue = self.invoke_queue.lock();
    let deadline = self.cmap.get_exec_time(&item.fqdn) + time_since_epoch();
    queue.push(MHQEDEnqueuedInvocation::new(item.clone(), deadline ).into());
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.function_name,
                        queue.peek().unwrap().x.function_name );
    self.queue_signal.notify_waiters();
  }
}
