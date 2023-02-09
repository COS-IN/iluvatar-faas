use std::sync::atomic::AtomicU32;
use std::{sync::Arc, time::Duration};
use crate::services::containers::structs::{ParsedResult, InsufficientCoresError, InsufficientMemoryError, ContainerState};
use crate::services::invocation::invoker_structs::InvocationResult;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::characteristics_map::{CharacteristicsMap, Characteristics, Values};
use iluvatar_library::logging::LocalTime;
use iluvatar_library::{transaction::{TransactionId}, threading::EventualItem};
use parking_lot::Mutex;
use time::{OffsetDateTime, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use anyhow::Result;
use super::invoker_structs::{EnqueuedInvocation, InvocationResultPtr};
use crate::rpc::InvokeResponse;

/// Check the invocation queue, running things when there are sufficient resources
#[cfg_attr(feature = "full_spans", tracing::instrument(skip(invoker_svc), fields(tid=%_tid)))]
pub async fn monitor_queue<T: Invoker + 'static>(invoker_svc: Arc<T>, _tid: TransactionId) {
  loop {
    if let Some(peek_item) = invoker_svc.peek_queue() {
      if let Some(permit) = invoker_svc.acquire_resources_to_run(&peek_item) {
        let item = invoker_svc.pop_queue();
        // TODO: continuity of spans here
        invoker_svc.spawn_tokio_worker(invoker_svc.clone(), item, permit);  
      }else { 
        break; 
      }
      // nothing can be run, or nothing to run
    } else { 
      break; 
    }
  }
}
pub fn create_concurrency_semaphore(permits: u32) -> Result<Arc<Semaphore>> {
  match permits {
    0 => anyhow::bail!("Invoker concurrency semaphore cannot have 0 permits"),
    p => Ok(Arc::new(Semaphore::new(p as usize)))
  }
}

#[allow(dyn_drop)]
#[tonic::async_trait]
/// A trait representing the functionality a queue policy must implement
/// Overriding functions _must_ re-implement [info] level log statements for consistency
pub trait Invoker: Send + Sync {
  fn cont_manager(&self) -> &Arc<ContainerManager>;
  fn function_config(&self) -> &Arc<FunctionLimits>;
  fn invocation_config(&self) -> &Arc<InvocationConfig>;
  fn async_functions<'a>(&'a self) -> &'a super::async_tracker::AsyncHelper;
  fn char_map(&self) -> &Arc<CharacteristicsMap>;
  fn timer(&self) -> &LocalTime;
  /// pointer to the semaphore to manage the invoker concurrency
  /// A [None] value means the invoker doesn't limit concurrency
  fn concurrency_semaphore(&self) -> Option<Arc<Semaphore>>;
  /// The number of functions currently running
  fn running_funcs(&self) -> u32 {
    let bypass = match self.bypass_running() {
      Some(x) => x.load(std::sync::atomic::Ordering::Relaxed),
      None => 0,
    };
    let concur = match self.concurrency_semaphore() {
      Some(x) => self.invocation_config().concurrent_invokes - x.available_permits() as u32,
      None => 0,
    };
    bypass + concur
  }
  /// An atomic counter tracking the number of items that bypass the concurrency limit
  /// If this returns [None], then there will be no bypassing. 
  /// So not implementing this in a queue disables the bypass
  fn bypass_running(&self) -> Option<&AtomicU32> { None }

  /// The length of a queue, if the implementation has one
  /// Default is 0 if not overridden
  /// An implementing struct only needs to implement this if it uses [monitor_queue]
  fn queue_len(&self) -> usize { 0 }
  /// A peek at the first item in the queue.
  /// Returns [Some(Arc<EnqueuedInvocation>)] if there is anything in the queue, [None] otherwise.
  /// An implementing struct only needs to implement this if it uses [monitor_queue].
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {todo!()}
  /// Destructively return the first item in the queue.
  /// This function will only be called if something is known to be un the queue, so using `unwrap` to remove an [Option] is safe
  /// An implementing struct only needs to implement this if it uses [monitor_queue]
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {todo!()}

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
  /// A synchronous invocation against this invoker
  /// Re-implementers **must** duplicate [tracing::info] logs for consistency
  async fn sync_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<super::invoker_structs::InvocationResultPtr> {
    // check if the bypass counter exists, and if this fqdn can by bypassed
    if let Some(bypass_running) = self.bypass_running() {
      if self.should_bypass(&reg.fqdn) {
        // if both true, then go for the bypass
        match self.bypassing_invoke_internal(reg.clone(), &json_args, &tid, bypass_running).await? {
          Some(x) => return Ok(x),
          None => (), // None means the function would have run cold, so instead we put it into the queue
        };
      }
    }

    let queued = self.enqueue_new_invocation(reg, json_args, tid.clone());
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
  fn async_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<String> {
    let invoke = self.enqueue_new_invocation(reg, json_args, tid);
    self.async_functions().insert_async_invoke(invoke)
  }
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<InvokeResponse> {
    self.async_functions().invoke_async_check(cookie, tid)
  }

  /// Return [true] if the item should bypass concurrency restrictions 
  fn should_bypass(&self, fqdn: &String) -> bool {
    let exec_time = self.char_map().get_exec_time(fqdn);
    exec_time != 0.0 && exec_time < Duration::from_millis(self.invocation_config().bypass_duration_ms).as_secs_f64()
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn, json_args, bypass_running), fields(tid=%tid)))]
  /// Run an invocation, bypassing any concurrency restrictions
  /// A return value of [Ok(None)] means that the function would have run cold, and the caller should enqueue it instead
  async fn bypassing_invoke_internal(&self, reg: Arc<RegisteredFunction>, json_args: &String, tid: &TransactionId, bypass_running: &AtomicU32) -> Result<Option<InvocationResultPtr>> {
    info!(tid=%tid, "Bypassing internal invocation starting");
    let timer = self.timer();
    // take run time now because we may have to wait to get a container
    let remove_time = timer.now_str()?;

    let ctr_mgr = self.cont_manager();
    let ctr_lock = match ctr_mgr.acquire_container(&reg, &tid) {
      EventualItem::Future(_) => return Ok(None), // return None on a cold start, signifying the bypass didn't happen
      EventualItem::Now(n) => n?,
    };
    info!(tid=%tid, insert_time=%remove_time, remove_time=%remove_time, "Item starting to execute");
    bypass_running.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (result, duration) = ctr_lock.invoke(&json_args).await?;
    bypass_running.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    match ctr_lock.container.state() 
    {
      ContainerState::Warm => {self.char_map().add(&reg.fqdn, Characteristics::WarmTime, Values::F64(result.duration_sec), true);},
      ContainerState::Prewarm => {self.char_map().add(&reg.fqdn, Characteristics::PreWarmTime, Values::F64(result.duration_sec), true);},
      _ => error!(tid=%tid, container_id=ctr_lock.container.container_id(), "Got a cold container when doing a bypass invoke"), // can't have a cold container
    };
    self.char_map().add(&reg.fqdn, Characteristics::ExecTime, Values::F64(result.duration_sec), true);
    let r = Arc::new(Mutex::new(InvocationResult {
      completed: true,
      duration: duration,
      result_json: result.result_string()?,
      attempts: 0,
      exec_time: result.duration_sec,
      worker_result: Some(result),
    }));
    Ok(Some(r))
  }

  /// Returns an owned permit if there are sufficient resources to run a function
  /// A return value of [None] means the resources failed to be acquired
  fn acquire_resources_to_run(&self, item: &Arc<EnqueuedInvocation>) -> Option<Box<dyn Drop+Send>> {
    let invoke_perm = match self.concurrency_semaphore() {
      Some(s) => match s.try_acquire_many_owned(1) {
        Ok(c) => Some(c),
        Err(e) => { 
          match e {
            tokio::sync::TryAcquireError::Closed => error!(tid=%item.tid, "invoker concurrency semaphore `try_acquire_many_owned` returned a closed error!"),
            tokio::sync::TryAcquireError::NoPermits => (),
          };
          return None;
        },
      },
      None => None,
    };
    let core_perm = match self.cont_manager().try_acquire_cores(&item.registration, &item.tid) {
      Ok(c) => c,
      Err(e) => { 
        match e {
          tokio::sync::TryAcquireError::Closed => error!(tid=%item.tid, "Container manager `try_acquire_cores` returned a closed error!"),
          tokio::sync::TryAcquireError::NoPermits => (),
        };
        return None;
      },
    };
    Some(Box::new(vec![core_perm, invoke_perm]))
  }

  /// Insert an item into the queue, optionally at a specific index
  /// If not specified, added to the end
  /// Wakes up the queue monitor thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, _item, _index), fields(tid=%_item.tid)))]
  fn add_item_to_queue(&self, _item: &Arc<EnqueuedInvocation>, _index: Option<usize>) { }

  /// Runs the specific invocation inside a new tokio worker thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, invoker_svc, item, permit), fields(tid=%item.tid)))]
  fn spawn_tokio_worker(&self, invoker_svc: Arc<dyn Invoker>, item: Arc<EnqueuedInvocation>, permit: Box<dyn Drop + Send>) {
    let _handle = tokio::spawn(async move {
      debug!(tid=%item.tid, "Launching invocation thread for queued item");
      invoker_svc.invocation_worker_thread(item, permit).await;
    });
  }

  /// Handle executing an invocation, plus account for its success or failure
  /// On success, the results are moved to the pointer and it is signaled
  /// On failure, [Invoker::handle_invocation_error] is called
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, permit), fields(tid=%item.tid)))]
  async fn invocation_worker_thread(&self, item: Arc<EnqueuedInvocation>, permit: Box<dyn Drop + Send>) {
    match self.invoke_internal(&item.registration, &item.json_args, &item.tid, item.queue_insert_time, Some(permit)).await {
      Ok( (json, duration) ) =>  {
        let mut result_ptr = item.result_ptr.lock();
        result_ptr.duration = duration;
        result_ptr.exec_time = json.duration_sec;
        result_ptr.result_json = json.result_string().unwrap_or_else(|cause| format!("{{ \"Error\": \"{}\" }}", cause));
        result_ptr.completed = true;
        result_ptr.worker_result = Some(json);
        item.signal();
        debug!(tid=%item.tid, "queued invocation completed successfully");
      },
      Err(cause) =>
      {
        self.handle_invocation_error(item, cause);
      },
    };
  }

  /// Handle an error with the given enqueued invocation
  /// By default re-enters item if a resource exhaustion error occurs ([InsufficientCoresError] or[InsufficientMemoryError])
  ///   Calls [Self::add_item_to_queue] to do this
  /// Other errors result in exit of invocation if [InvocationConfig.attempts] are made
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, cause), fields(tid=%item.tid)))]
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

  /// Forms invocation data into a [EnqueuedInvocation] that is returned
  /// The default implementation also calls [Invoker::add_item_to_queue] to optionally insert that item into the implementation's queue
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
  fn enqueue_new_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Arc<EnqueuedInvocation> {
    debug!(tid=%tid, "Enqueueing invocation");
    let enqueue = Arc::new(EnqueuedInvocation::new(reg, json_args, tid, self.timer().now()));
    self.add_item_to_queue(&enqueue, None);
    enqueue
  }

  /// acquires a container and invokes the function inside it
  /// returns the json result and duration as a tuple
  /// The optional [permit] is dropped to return held resources
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, _function_name, _function_version, json_args, queue_insert_time, permit), fields(tid=%tid)))]
  async fn invoke_internal(&self, reg: &Arc<RegisteredFunction>, json_args: &String, tid: &TransactionId, queue_insert_time: OffsetDateTime, permit: Option<Box<dyn Drop+Send>>) -> Result<(ParsedResult, Duration)> {
    debug!(tid=%tid, "Internal invocation starting");
    let timer = self.timer();
    // take run time now because we may have to wait to get a container
    let remove_time = timer.now_str();

    let ctr_mgr = self.cont_manager();
    let start = Instant::now();
    let ctr_lock = match ctr_mgr.acquire_container(reg, tid) {
      EventualItem::Future(f) => f.await?,
      EventualItem::Now(n) => n?,
    };
    info!(tid=%tid, insert_time=%timer.format_time(queue_insert_time)?, remove_time=%remove_time?, "Item starting to execute");
    let (data, duration) = ctr_lock.invoke(json_args).await?;
    match ctr_lock.container.state() {
      ContainerState::Warm => self.char_map().add(&reg.fqdn, Characteristics::WarmTime, Values::F64(data.duration_sec), true),
      ContainerState::Prewarm => self.char_map().add(&reg.fqdn, Characteristics::PreWarmTime, Values::F64(data.duration_sec), true),
      _ => self.char_map().add(&reg.fqdn, Characteristics::ColdTime, Values::F64(start.elapsed().as_seconds_f64()), true),
    };
    self.char_map().add(&reg.fqdn, Characteristics::ExecTime, Values::F64(data.duration_sec), true);
    drop(permit);
    Ok((data, duration))
  }
}
