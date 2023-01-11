use std::sync::atomic::AtomicU32;
use std::{sync::Arc, time::Duration};
use crate::services::invocation::invoker_trait::create_concurrency_semaphore;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, threading::tokio_runtime, characteristics_map::{Characteristics,CharacteristicsMap,Values}};
use iluvatar_library::logging::LocalTime;
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, info, warn};
use super::invoker_structs::InvocationResultPtr;
use super::{invoker_trait::{Invoker, monitor_queue}, async_tracker::AsyncHelper, invoker_structs::EnqueuedInvocation};
use std::collections::BinaryHeap;
use std::cmp::Ordering; 
use iluvatar_library::threading::EventualItem;

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
  bypass_dur: f64,
  bypass_running: AtomicU32
}

impl FCFSBypassInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let (handle, tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_QUEUE_WORKER_TID.clone(), monitor_queue, Some(FCFSBypassInvoker::wait_on_queue), Some(function_config.cpu_max as usize))?;
    let bypass_dur = Duration::from_millis(invocation_config.bypass_duration_ms.ok_or_else(|| anyhow::anyhow!("bypass_duration_ms was not present in InvocationConfig"))?).as_secs_f64();
    if bypass_dur == 0.0 {
      anyhow::bail!("Cannot have a 'bypass_duration_ms' of 0");
    }
    let svc = Arc::new(FCFSBypassInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?,
      bypass_dur: bypass_dur,
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
    info!(tid=%tid, bypass_dur=bypass_dur, "Created FCFSBypassInvoker");
    Ok(svc)
  }

  /// Wait on the Notify object for the queue to be available again
  async fn wait_on_queue(invoker_svc: Arc<FCFSBypassInvoker>, tid: TransactionId) {
    invoker_svc.queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, enqueued), fields(tid=%enqueued.tid)))]
  async fn bypassing_invoke_internal(&self, enqueued: Arc<EnqueuedInvocation>) -> Result<InvocationResultPtr> {
    info!(tid=%enqueued.tid, "Bypassing internal invocation starting");
    let timer = self.timer();
    // take run time now because we may have to wait to get a container
    let remove_time = timer.now_str();

    let ctr_mgr = self.cont_manager();
    let ctr_lock = match ctr_mgr.acquire_container(&enqueued.fqdn, &enqueued.tid) {
      EventualItem::Future(_) => {
        // this would be a cold start, throw it in the queue
        self.add_item_to_queue(&enqueued, Some(usize::MAX));
        enqueued.wait(&enqueued.tid).await?;
        return Ok(enqueued.result_ptr.clone());
      },
      EventualItem::Now(n) => n?,
    };
    info!(tid=%enqueued.tid, insert_time=%timer.format_time(enqueued.queue_insert_time)?, remove_time=%remove_time?, "Item starting to execute");
    self.bypass_running.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (result, duration) = ctr_lock.invoke(&enqueued.json_args).await?;
    self.bypass_running.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    self.update_metadata(&enqueued.fqdn, result.duration_sec);
    let mut temp = enqueued.result_ptr.lock();
    temp.exec_time = result.duration_sec;
    temp.result_json = result.result_string()?;
    temp.worker_result = Some(result);
    temp.duration = duration;
    Ok( enqueued.result_ptr.clone() )
  }

  fn update_metadata(&self, fqdn: &String, result: f64) {
    self.cmap.add( fqdn, Characteristics::WarmTime, Values::F64(result), true);          
    self.cmap.add( fqdn, Characteristics::ExecTime, Values::F64(result), true);          
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
    let bypass = self.bypass_running.load(std::sync::atomic::Ordering::Relaxed);
    let concur = self.invocation_config.concurrent_invokes - self.concurrency_semaphore.available_permits() as u32;
    bypass + concur
  }
  fn char_map(&self) -> &Arc<CharacteristicsMap> {
    &self.cmap
  }
  
  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<InvocationResultPtr> {
    let queued = self.enqueue_new_invocation(function_name, function_version, json_args, tid);
    let exec_time = self.cmap.get_exec_time(&queued.function_name);
    if exec_time != 0.0 && exec_time < self.bypass_dur {
      return self.bypassing_invoke_internal(queued).await;
    } else {
      queued.wait(&queued.tid).await?;
      let result_ptr = queued.result_ptr.lock();
      if result_ptr.completed {
        info!(tid=%queued.tid, exec_time=exec_time, "Invocation complete");
        return Ok( queued.result_ptr.clone() );
      } else {
        anyhow::bail!("Invocation was signaled completion but completion value was not set")
      }
    }
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, index: Option<usize>) {
    let exec_time = self.cmap.get_exec_time(&item.function_name);
    match index {
      Some(0) => {
        if exec_time != 0.0 && exec_time < self.bypass_dur {
          // do not add item to queue, we will invoke it immediately
          debug!(tid=%item.tid, exec_time=exec_time, "Errored function invocation bypassing queue");
          return;
        }
      },
      Some(usize::MAX) => info!(tid=%item.tid, exec_time=exec_time, "Adding item to queue because it would be a cold start"),
      Some(i) => warn!(tid=%item.tid, value=i, "Unknown index in add_item_to_queue"),
      None => {
        if exec_time != 0.0 && exec_time < self.bypass_dur {
          // do not add item to queue, we will invoke it immediately
          debug!(tid=%item.tid, exec_time=exec_time, "Function invocation bypassing queue");
          return;
        }
      }
    };
    let mut queue = self.invoke_queue.lock();
    queue.push(FCFSBPEnqueuedInvocation::new(item.clone()).into());
    self.queue_signal.notify_waiters();
  }
}
