use std::{sync::{Arc, atomic::AtomicU32}, time::Duration};
use crate::services::{containers::{structs::{ParsedResult, InsufficientMemoryError, ContainerState, ContainerLock, InsufficientGPUError}, containermanager::ContainerManager}, invocation::EnqueueingPolicy};
use crate::services::{invocation::invoker_structs::InvocationResult, registration::RegisteredFunction};
use crate::services::resources::{cpu::CPUResourceMananger, gpu::GpuResourceTracker};
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use iluvatar_library::characteristics_map::{CharacteristicsMap, Characteristics, Values};
use iluvatar_library::{transaction::TransactionId, threading::EventualItem, logging::LocalTime, types::Compute, threading::tokio_runtime};
use parking_lot::Mutex;
use time::{OffsetDateTime, Instant};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};
use anyhow::Result;
use super::{async_tracker::AsyncHelper, InvokerQueuePolicy, Invoker, invoker_structs::{EnqueuedInvocation, InvocationResultPtr}};
use super::{avail_scale_q::AvailableScalingQueue, queueless::Queueless, fcfs_q::FCFSQueue, minheap_q::MinHeapQueue, minheap_ed_q::MinHeapEDQueue, minheap_iat_q::MinHeapIATQueue, cold_priority_q::ColdPriorityQueue};

lazy_static::lazy_static! {
  pub static ref INVOKER_CPU_QUEUE_WORKER_TID: TransactionId = "InvokerCPUQueue".to_string();
  pub static ref INVOKER_GPU_QUEUE_WORKER_TID: TransactionId = "InvokerGPUQueue".to_string();
}

pub struct QueueingInvoker {
  cont_manager: Arc<ContainerManager>,
  async_functions: AsyncHelper,
  invocation_config: Arc<InvocationConfig>,
  cmap: Arc<CharacteristicsMap>,
  clock: LocalTime,
  running: AtomicU32,
  last_memory_warning: Mutex<Instant>,
  _cpu_thread: std::thread::JoinHandle<()>,
  cpu_queue_signal: Notify,
  cpu: Arc<CPUResourceMananger>,
  cpu_queue: Arc<dyn InvokerQueuePolicy>,
  _gpu_thread: std::thread::JoinHandle<()>,
  gpu: Arc<GpuResourceTracker>,
  gpu_queue_signal: Notify,
  gpu_queue: Arc<dyn InvokerQueuePolicy>,
}

#[allow(dyn_drop)]
/// An invoker implementation that enqueues invocations and orders them based on a variety of characteristics
/// Queueing method is configurable
impl QueueingInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, 
      tid: &TransactionId, cmap: Arc<CharacteristicsMap>, cpu: Arc<CPUResourceMananger>, gpu: Arc<GpuResourceTracker>) -> Result<Arc<Self>> {
    let (cpu_handle, cpu_tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_CPU_QUEUE_WORKER_TID.clone(), Self::monitor_cpu_queue, Some(Self::cpu_wait_on_queue), Some(function_config.cpu_max as usize))?;
    let (gpu_handle, gpu_tx) = tokio_runtime(invocation_config.queue_sleep_ms, INVOKER_GPU_QUEUE_WORKER_TID.clone(), Self::monitor_gpu_queue, Some(Self::gpu_wait_on_queue), Some(function_config.cpu_max as usize))?;
    let svc = Arc::new(QueueingInvoker {
      cpu_queue: Self::get_invoker_queue(&invocation_config, &cmap, &cont_manager, tid)?,
      gpu_queue: Self::get_invoker_queue(&invocation_config, &cmap, &cont_manager, tid)?,
      cont_manager, invocation_config,
      async_functions: AsyncHelper::new(),
      cpu_queue_signal: Notify::new(),
      _cpu_thread: cpu_handle,
      gpu_queue_signal: Notify::new(),
      _gpu_thread: gpu_handle,
      clock: LocalTime::new(tid)?,
      cpu, gpu, cmap,
      running: AtomicU32::new(0),
      last_memory_warning: Mutex::new(Instant::now()),
    });
    cpu_tx.send(svc.clone())?;
    gpu_tx.send(svc.clone())?;
    debug!(tid=%tid, "Created MinHeapInvoker");
    Ok(svc)
  }

  fn get_invoker_queue(invocation_config: &Arc<InvocationConfig>, cmap: &Arc<CharacteristicsMap>, cont_manager: &Arc<ContainerManager>, tid: &TransactionId)  -> Result<Arc<dyn InvokerQueuePolicy>> {
    let r: Arc<dyn InvokerQueuePolicy> = match invocation_config.queue_policy.to_lowercase().as_str() {
      "none" => Queueless::new()?,
      "fcfs" => FCFSQueue::new()?,
      "minheap" => MinHeapQueue::new(tid, cmap.clone())?,
      "minheap_ed" => MinHeapEDQueue::new(tid, cmap.clone())?,
      "minheap_iat" => MinHeapIATQueue::new(tid, cmap.clone())?,
      "cold_pri" => ColdPriorityQueue::new(cont_manager.clone(), tid, cmap.clone())?,
      "scaling" => AvailableScalingQueue::new(cont_manager.clone(), tid, cmap.clone())?,
      unknown => panic!("Unknown queueing policy '{}'", unknown),
    };
    Ok(r)
  }
  
  /// Wait on the Notify object for the queue to be available again
  async fn cpu_wait_on_queue(invoker_svc: Arc<QueueingInvoker>, tid: TransactionId) {
    invoker_svc.cpu_queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
  /// Check the invocation queue, running things when there are sufficient resources
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(invoker_svc), fields(tid=%tid)))]
  async fn monitor_cpu_queue(invoker_svc: Arc<Self>, tid: TransactionId) {
    invoker_svc.clone().monitor_queue(invoker_svc.cpu_queue.clone(), tid, Compute::CPU).await;
  }
  async fn gpu_wait_on_queue(invoker_svc: Arc<QueueingInvoker>, tid: TransactionId) {
    invoker_svc.gpu_queue_signal.notified().await;
    debug!(tid=%tid, "Invoker waken up by signal");
  }
  /// Check the invocation queue, running things when there are sufficient resources
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(invoker_svc), fields(tid=%tid)))]
  async fn monitor_gpu_queue(invoker_svc: Arc<Self>, tid: TransactionId) {
    invoker_svc.clone().monitor_queue(invoker_svc.gpu_queue.clone(), tid, Compute::GPU).await;
  }
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, queue, compute), fields(tid=%_tid)))]
  async fn monitor_queue(self: Arc<Self>, queue: Arc<dyn InvokerQueuePolicy>, _tid: TransactionId, compute: Compute) {
    loop {
      if let Some(peek_item) = queue.peek_queue() {
        let acquire_gpu = compute == Compute::GPU;
        if let Some(permit) = self.acquire_resources_to_run(&peek_item, acquire_gpu) {
          let item = queue.pop_queue();
          let mut started = item.started.lock();
          match *started {
            true => continue, // item was started on another resource
            false => {
              *started = true;
              drop(started);
            },
          }
          // TODO: continuity of spans here
          self.spawn_tokio_worker(self.clone(), item, permit, compute);  
        }else { 
          debug!(tid=%peek_item.tid, "Insufficient resources to run item");
          break; 
        }
      } else { 
        // nothing can be run, or nothing to run
        break; 
      }
    }
  }

  /// Return [true] if the item should bypass concurrency restrictions
  /// Only CPU supported functions can possibly be bypassed
  fn should_bypass(&self, reg: &Arc<RegisteredFunction>) -> bool {
    if reg.supported_compute.contains(Compute::CPU) {
      return match self.invocation_config.bypass_duration_ms {
        Some(bypass_duration_ms) => {
          let exec_time = self.cmap.get_exec_time(&reg.fqdn);
          exec_time != 0.0 && exec_time < Duration::from_millis(bypass_duration_ms).as_secs_f64()
        },
        None => false,
      };
    } else {
      return false;
    }
  }

  /// Returns an owned permit if there are sufficient resources to run a function
  /// A return value of [None] means the resources failed to be acquired
  fn acquire_resources_to_run(&self, item: &Arc<EnqueuedInvocation>, gpu: bool) -> Option<Box<dyn Drop+Send>> {
    let mut ret = vec![];
    match self.cpu.try_acquire_cores(&item.registration, &item.tid) {
      Ok(c) => ret.push(c),
      Err(e) => { 
        match e {
          tokio::sync::TryAcquireError::Closed => error!(tid=%item.tid, "CPU Resource Monitor `try_acquire_cores` returned a closed error!"),
          tokio::sync::TryAcquireError::NoPermits => (),
        };
        return None;
      },
    };
    if gpu {
      match self.gpu.try_acquire_resource() {
        Ok(c) => ret.push(Some(c)),
        Err(e) => { 
          match e {
            tokio::sync::TryAcquireError::Closed => error!(tid=%item.tid, "GPU Resource Monitor `try_acquire_cores` returned a closed error!"),
            tokio::sync::TryAcquireError::NoPermits => (),
          };
          return None;
        },
      };
    }
    Some(Box::new(ret))
  }

  /// Runs the specific invocation inside a new tokio worker thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, invoker_svc, item, permit), fields(tid=%item.tid)))]
  fn spawn_tokio_worker(&self, invoker_svc: Arc<Self>, item: Arc<EnqueuedInvocation>, permit: Box<dyn Drop + Send>, compute: Compute) {
    let _handle = tokio::spawn(async move {
      debug!(tid=%item.tid, "Launching invocation thread for queued item");
      invoker_svc.invocation_worker_thread(item, permit, compute).await;
    });
  }

  /// Handle executing an invocation, plus account for its success or failure
  /// On success, the results are moved to the pointer and it is signaled
  /// On failure, [Invoker::handle_invocation_error] is called
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, permit), fields(tid=%item.tid)))]
  async fn invocation_worker_thread(&self, item: Arc<EnqueuedInvocation>, permit: Box<dyn Drop + Send>, compute: Compute) {
    match self.invoke(&item.registration, &item.json_args, &item.tid, item.queue_insert_time, Some(permit), compute).await {
      Ok( (json, duration, compute, container_state) ) =>  {
        let mut result_ptr = item.result_ptr.lock();
        result_ptr.duration = duration;
        result_ptr.exec_time = json.duration_sec;
        result_ptr.result_json = json.result_string().unwrap_or_else(|cause| format!("{{ \"Error\": \"{}\" }}", cause));
        result_ptr.completed = true;
        result_ptr.worker_result = Some(json);
        result_ptr.compute = compute;
        result_ptr.container_state = container_state;
        item.signal();
        debug!(tid=%item.tid, "queued invocation completed successfully");
      },
      Err(cause) => self.handle_invocation_error(item, cause, compute),
    };
  }

  /// Handle an error with the given enqueued invocation
  /// By default re-enters item if a resource exhaustion error occurs [InsufficientMemoryError]
  ///   Calls [Self::add_item_to_queue] to do this
  /// Other errors result in exit of invocation if [InvocationConfig.attempts] are made
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, cause), fields(tid=%item.tid)))]
  fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error, compute: Compute) {
    if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
      let mut warn_time = self.last_memory_warning.lock();
      if warn_time.elapsed() > Duration::from_millis(500) {
        warn!(tid=%item.tid, "Insufficient memory to run item right now");
        *warn_time = Instant::now();
      }
      *item.started.lock() = false;
      if compute == Compute::CPU {
        self.cpu_queue.add_item_to_queue(&item, Some(0));
        self.cpu_queue_signal.notify_waiters();
      }
      if compute == Compute::GPU {
        self.gpu_queue.add_item_to_queue(&item, Some(0));
        self.gpu_queue_signal.notify_waiters();
      }
    } else if let Some(_mem_err) = cause.downcast_ref::<InsufficientGPUError>() {
      warn!(tid=%item.tid, "No GPU available to run item right now");
      *item.started.lock() = false;
      self.gpu_queue.add_item_to_queue(&item, Some(0));
      self.gpu_queue_signal.notify_waiters();
    } else {
      error!(tid=%item.tid, error=%cause, "Encountered unknown error while trying to run queued invocation");
      let mut result_ptr = item.result_ptr.lock();
      if result_ptr.attempts >= self.invocation_config.retries {
        error!(tid=%item.tid, attempts=result_ptr.attempts, "Abandoning attempt to run invocation after attempts");
        result_ptr.duration = Duration::from_micros(0);
        result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", cause);
        result_ptr.completed = true;
        item.signal();
      } else {
        result_ptr.attempts += 1;
        debug!(tid=%item.tid, attempts=result_ptr.attempts, "re-queueing invocation attempt after attempting");
        self.cpu_queue.add_item_to_queue(&item, Some(0));
      }
    }
  }

  /// Forms invocation data into a [EnqueuedInvocation] that is returned
  /// The default implementation also calls [Invoker::add_item_to_queue] to optionally insert that item into the implementation's queue
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args), fields(tid=%tid)))]
  fn enqueue_new_invocation(&self, reg: &Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<Arc<EnqueuedInvocation>> {
    debug!(tid=%tid, "Enqueueing invocation");
    let mut exec = 0.0;
    if reg.supported_compute.contains(Compute::CPU) {
      exec = self.cmap.get_exec_time(&reg.fqdn);
    }
    if reg.supported_compute.contains(Compute::GPU) {
      exec = self.cmap.get_gpu_exec_time(&reg.fqdn);
    }
    let enqueue = Arc::new(EnqueuedInvocation::new(reg.clone(), json_args, tid, self.clock.now(), exec));
    let mut enqueues = 0;

    if reg.supported_compute == Compute::CPU {
      self.cpu_queue.add_item_to_queue(&enqueue, None);
      self.cpu_queue_signal.notify_waiters();
      return Ok(enqueue);
    }
    if reg.supported_compute == Compute::GPU {
      self.gpu_queue.add_item_to_queue(&enqueue, None);
      self.gpu_queue_signal.notify_waiters();
      return Ok(enqueue);
    }

    let policy = self.invocation_config.enqueueing_policy.as_ref().unwrap_or(&EnqueueingPolicy::All);
    match policy {
      EnqueueingPolicy::All => {
        if reg.supported_compute.contains(Compute::CPU) {
          self.cpu_queue.add_item_to_queue(&enqueue, None);
          self.cpu_queue_signal.notify_waiters();
          enqueues += 1;
        }
        if reg.supported_compute.contains(Compute::GPU) {
          self.gpu_queue.add_item_to_queue(&enqueue, None);
          self.gpu_queue_signal.notify_waiters();
          enqueues += 1;
        }
      },
      EnqueueingPolicy::AlwaysCPU => {
        if reg.supported_compute.contains(Compute::CPU) {
          self.cpu_queue.add_item_to_queue(&enqueue, None);
          self.cpu_queue_signal.notify_waiters();
          enqueues += 1;
        } else {
          anyhow::bail!("Cannot enqueue invocation using {:?} strategy because it does not support CPU", EnqueueingPolicy::AlwaysCPU);
        }
      },
      EnqueueingPolicy::ShortestExecTime => {
        let mut opts = vec![];
        if reg.supported_compute.contains(Compute::CPU) {
          opts.push((self.cmap.get_exec_time(&reg.fqdn), &self.cpu_queue, &self.cpu_queue_signal));
        }
        if reg.supported_compute.contains(Compute::GPU) {
          opts.push((self.cmap.get_gpu_exec_time(&reg.fqdn), &self.gpu_queue, &self.gpu_queue_signal));
        }
        let best = opts.iter().min_by_key(|i| ordered_float::OrderedFloat(i.0));
        if let Some((_, q, signal)) = best {
          q.add_item_to_queue(&enqueue, None);
          signal.notify_waiters();
          enqueues += 1;
        }
      },
      EnqueueingPolicy::EstCompTime => {
        let mut opts = vec![];
        if reg.supported_compute.contains(Compute::CPU) {
          opts.push((self.cpu_queue.est_queue_time() + self.cmap.get_exec_time(&reg.fqdn), &self.cpu_queue, &self.cpu_queue_signal));
        }
        if reg.supported_compute.contains(Compute::GPU) {
          opts.push((self.gpu_queue.est_queue_time() + self.cmap.get_gpu_exec_time(&reg.fqdn), &self.gpu_queue, &self.gpu_queue_signal));
        }
        let best = opts.iter().min_by_key(|i| ordered_float::OrderedFloat(i.0));
        if let Some((_, q, signal)) = best {
          q.add_item_to_queue(&enqueue, None);
          signal.notify_waiters();
          enqueues += 1;
        }
      },
    }

    if enqueues == 0 {
      anyhow::bail!("Unable to enqueue function invocation, not matching compute");
    }
    Ok(enqueue)
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args), fields(tid=%tid)))]
  /// Run an invocation, bypassing any concurrency restrictions
  /// A return value of [Ok(None)] means that the function would have run cold, and the caller should enqueue it instead
  async fn bypassing_invoke(&self, reg: Arc<RegisteredFunction>, json_args: &String, tid: &TransactionId) -> Result<Option<InvocationResultPtr>> {
    info!(tid=%tid, "Bypassing internal invocation starting");
    // take run time now because we may have to wait to get a container
    let remove_time = self.clock.now();
    let ctr_lock = match self.cont_manager.acquire_container(&reg, &tid, Compute::CPU) {
      EventualItem::Future(_) => return Ok(None), // return None on a cold start, signifying the bypass didn't happen
      EventualItem::Now(n) => n?,
    };
    let (result, duration, compute, container_state) = self.invoke_on_container(&reg, json_args, tid, remove_time, None, ctr_lock, self.clock.format_time(remove_time)?, Instant::now()).await?;
    let r = Arc::new(Mutex::new(InvocationResult {
      completed: true, duration, compute, container_state,
      result_json: result.result_string()?,
      attempts: 0,
      exec_time: result.duration_sec,
      worker_result: Some(result),
    }));
    Ok(Some(r))
  }

  /// acquires a container and invokes the function inside it
  /// returns the json result and duration as a tuple
  /// The optional [permit] is dropped to return held resources
  /// Returns
  /// [ParsedResult] A result representing the function output, the user result plus some platform tracking
  /// [Duration]: The E2E latency between the worker and the container
  /// [Compute]: Compute the invocation was run on
  /// [ContainerState]: State the container was in for the invocation
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args, queue_insert_time, permit, compute), fields(tid=%tid)))]
  async fn invoke<'a>(&'a self, reg: &'a Arc<RegisteredFunction>, json_args: &'a String, tid: &'a TransactionId, 
    queue_insert_time: OffsetDateTime, permit: Option<Box<dyn Drop+Send>>, compute: Compute) -> Result<(ParsedResult, Duration, Compute, ContainerState)> {
    debug!(tid=%tid, "Internal invocation starting");
    // take run time now because we may have to wait to get a container
    let remove_time = self.clock.now_str()?;

    let start = Instant::now();
    let ctr_lock = match self.cont_manager.acquire_container(reg, tid, compute) {
      EventualItem::Future(f) => f.await?,
      EventualItem::Now(n) => n?,
    };
    self.invoke_on_container(reg, json_args, tid, queue_insert_time, permit, ctr_lock, remove_time, start).await
  }

  /// Returns
  /// [ParsedResult] A result representing the function output, the user result plus some platform tracking
  /// [Duration]: The E2E latency between the worker and the container
  /// [Compute]: Compute the invocation was run on
  /// [ContainerState]: State the container was in for the invocation
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args, queue_insert_time, permit, ctr_lock, remove_time,cold_time_start) fields(tid=%tid)))]
  async fn invoke_on_container<'a>(&'a self, reg: &'a Arc<RegisteredFunction>, json_args: &'a String, tid: &'a TransactionId, queue_insert_time: OffsetDateTime, 
      permit: Option<Box<dyn Drop+Send>>, ctr_lock: ContainerLock<'a>, remove_time: String, cold_time_start: Instant) -> Result<(ParsedResult, Duration, Compute, ContainerState)> {
    
    info!(tid=%tid, insert_time=%self.clock.format_time(queue_insert_time)?, remove_time=%remove_time, "Item starting to execute");
    self.running.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (data, duration) = ctr_lock.invoke(json_args).await?;
    self.running.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    match ctr_lock.container.state() {
      ContainerState::Warm => self.cmap.add(&reg.fqdn, Characteristics::WarmTime, Values::F64(data.duration_sec), true),
      ContainerState::Prewarm => self.cmap.add(&reg.fqdn, Characteristics::PreWarmTime, Values::F64(data.duration_sec), true),
      _ => self.cmap.add(&reg.fqdn, Characteristics::ColdTime, Values::F64(cold_time_start.elapsed().as_seconds_f64()), true),
    };
    self.cmap.add(&reg.fqdn, Characteristics::ExecTime, Values::F64(data.duration_sec), true);
    drop(permit);
    self.cpu_queue_signal.notify_waiters();
    Ok((data, duration, ctr_lock.container.compute_type(), ctr_lock.container.state()))
  }
}

#[tonic::async_trait]
impl Invoker for QueueingInvoker {
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args), fields(tid=%tid)))]
  async fn sync_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<super::invoker_structs::InvocationResultPtr> {
    // check if the bypass counter exists, and if this fqdn can by bypassed
    if self.should_bypass(&reg) {
      // if both true, then go for the bypass
      match self.bypassing_invoke(reg.clone(), &json_args, &tid).await? {
        Some(x) => return Ok(x),
        None => (), // None means the function would have run cold, so instead we put it into the queue
      };
    }

    let queued = self.enqueue_new_invocation(&reg, json_args, tid.clone())?;
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
    let invoke = self.enqueue_new_invocation(&reg, json_args, tid)?;
    self.async_functions.insert_async_invoke(invoke)
  }
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<crate::rpc::InvokeResponse> {
    self.async_functions.invoke_async_check(cookie, tid)
  }

  /// The queue length of both CPU and GPU queues
  fn queue_len(&self) -> (usize,usize) {
    (self.cpu_queue.queue_len(), self.gpu_queue.queue_len())
  }

  /// The number of functions currently running
  fn running_funcs(&self) -> u32 {
    self.running.load(std::sync::atomic::Ordering::Relaxed)
  }
}