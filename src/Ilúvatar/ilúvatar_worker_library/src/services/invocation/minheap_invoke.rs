use std::{sync::Arc, time::Duration};
use crate::services::containers::structs::{InsufficientCoresError, InsufficientMemoryError};
use crate::services::invocation::invoker_trait::create_concurrency_semaphore;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime, characteristics_map::{Characteristics,CharacteristicsMap,AgExponential,Values,unwrap_val_dur}};
use iluvatar_library::logging::LocalTime;
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, warn, error, info};
use super::{invoker_trait::{Invoker, monitor_queue}, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};
use crate::rpc::InvokeResponse;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use ordered_float::OrderedFloat;     

#[derive(Debug)]
pub struct MHQEnqueuedInvocation {
    x: Arc<EnqueuedInvocation>,
    exectime: f64
}

impl MHQEnqueuedInvocation {
    fn new( x: Arc<EnqueuedInvocation>, exectime: f64 ) -> Self {
        MHQEnqueuedInvocation {
            x,
            exectime
        }
    }
}

fn get_exec_time( cmap: &Arc<CharacteristicsMap>, fname: &String ) -> f64 {
    let exectime = cmap.lookup(fname.clone(), Characteristics::ExecTime); 
    match exectime {
        Some(x) => {
            let exectime = unwrap_val_dur( &x );
            exectime.as_secs_f64()
        }
        None => {
            0.0
        }
    }
}

impl Eq for MHQEnqueuedInvocation {
}

fn compare_f64( lhs: &f64, rhs: &f64 ) -> Ordering {
    let lhs: OrderedFloat<f64> = OrderedFloat( *lhs );
    let rhs: OrderedFloat<f64> = OrderedFloat( *rhs );

    rhs.cmp(&lhs)
}

impl Ord for MHQEnqueuedInvocation {
 fn cmp(&self, other: &Self) -> Ordering {
     compare_f64( &self.exectime, &other.exectime )
 }
}

impl PartialOrd for MHQEnqueuedInvocation {
 fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
     Some(compare_f64( &self.exectime, &other.exectime ))
 }
}

impl PartialEq for MHQEnqueuedInvocation {
 fn eq(&self, other: &Self) -> bool {
     self.exectime == other.exectime
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
  queue_signal: Notify,
  clock: LocalTime,
  concurrency_semaphore: Arc<Semaphore>,
}

impl MinHeapInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(MinHeapInvoker::wait_on_queue), Some(function_config.cpu_max as usize));
    let svc = Arc::new(MinHeapInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes),
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      queue_signal: Notify::new(),
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap: Arc::new(CharacteristicsMap::new(AgExponential::new(0.6))),
      _worker_thread: handle,
      clock: LocalTime::new(tid)?,
    });
    tx.send(svc.clone())?;
    debug!("Created MinHeapInvoker");
    Ok(svc)
  }

  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<MinHeapInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
}

#[tonic::async_trait]
impl Invoker for MinHeapInvoker {
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
        Some(e) => func_name = e.x.function_name.clone(),
        None => func_name = "empty".to_string()
    }
    debug!( component="minheap", "Popped item from queue minheap - len: {} popped: {} top: {} ",
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
  fn concurrency_semaphore(&self) -> Option<Arc<Semaphore>> {
    Some(self.concurrency_semaphore.clone())
  }
  fn running_funcs(&self) -> u32 {
    self.invocation_config.concurrent_invokes - self.concurrency_semaphore.available_permits() as u32
  }

  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<(String, Duration)> {
    // self.invoke_internal(&function_name, &function_version, &json_args, &tid).await
    let queued = self.enqueue_new_invocation(function_name.clone(), function_version, json_args, tid.clone());
    queued.wait(&tid).await?;
    let result_ptr = queued.result_ptr.lock();
    match result_ptr.completed {
      true => {
        info!(tid=%tid, "Invocation complete");
        self.cmap.add( function_name, Characteristics::ExecTime, Values::F64(result_ptr.duration.clone().as_secs_f64()));
        Ok( (result_ptr.result_json.clone(), result_ptr.duration) )  
      },
      false => {
        anyhow::bail!("Invocation was signaled completion but completion value was not set")
      }
    }
  }
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    queue.push(MHQEnqueuedInvocation::new(item.clone(), get_exec_time( &self.cmap, &item.function_name )).into());
    debug!( component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.function_name,
                        queue.peek().unwrap().x.function_name );
    self.cmap.dump();
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
