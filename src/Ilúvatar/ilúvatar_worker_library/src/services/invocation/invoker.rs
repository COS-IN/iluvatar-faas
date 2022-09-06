use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration};
use crate::{worker_api::worker_config::{FunctionLimits, InvocationConfig}, services::invocation::invoker_structs::InvocationResult};
use crate::services::containers::{containermanager::ContainerManager, structs::{InsufficientCoresError, InsufficientMemoryError}};
use crate::rpc::{InvokeRequest, InvokeAsyncRequest, InvokeResponse};
use iluvatar_library::{utils::calculate_fqdn, transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, bail_error, continuation::Continuation};
use dashmap::DashMap;
use parking_lot::Mutex;
use tracing::{debug, error, warn, info};
use std::time::SystemTime;
use anyhow::Result;
use guid_create::GUID;
use super::invoker_structs::{QueueFuture, EnqueuedInvocation, InvocationResultPtr};

pub struct InvokerService {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: Arc<DashMap<String, InvocationResultPtr>>,
  pub running_functions: Arc<DashMap<String, u32>>,
  pub invoke_queue: Arc<Mutex<Vec<Arc<EnqueuedInvocation>>>>,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  // TODO: occasionally check if this died and re-run?
  _worker_thread: Option<std::thread::JoinHandle<()>>,
  continuation: Arc<Continuation>,
}

impl InvokerService {
    fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, worker_thread: Option<std::thread::JoinHandle<()>>, continuation: Arc<Continuation>) -> Self {
      InvokerService {
        cont_manager,
        async_functions: Arc::new(DashMap::new()),
        running_functions: Arc::new(DashMap::new()),
        invoke_queue: Arc::new(Mutex::new(Vec::new())),
        function_config,
        invocation_config,
        _worker_thread: worker_thread,
        continuation,
      }
    }

    pub fn boxed(cont_manager: Arc<ContainerManager>, tid: &TransactionId, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, continuation: Arc<Continuation>) -> Arc<Self> {
      let (tx, rx) = channel();
      let handle = match invocation_config.use_queue {
        true => Some(InvokerService::start_queue_thread(rx, tid)),
        false => None,
      };
      let i = Arc::new(InvokerService::new(cont_manager, function_config, invocation_config, handle, continuation));
      tx.send(i.clone()).unwrap();
      return i;
    }

    fn start_queue_thread(rx: Receiver<Arc<InvokerService>>, tid: &TransactionId) -> std::thread::JoinHandle<()> {
      debug!(tid=%tid, "Launching InvokerService queue thread");
      // TODO: smartly manage the queue, not just FIFO?
      // run on an OS thread here so we don't get blocked by out userland threading runtime
      // If this thread crashes, we'll never know and the worker will deadlock
      std::thread::spawn(move || {
        let tid: &TransactionId = &INVOKER_QUEUE_WORKER_TID;
        let invoker_svc: Arc<InvokerService> = match rx.recv() {
            Ok(svc) => svc,
            Err(_) => {
              error!(tid=%tid, "Invoker service thread failed to receive service from channel!");
              return;
            },
          };
          debug!(tid=%tid, "invoker worker started");
          invoker_svc.monitor_queue(tid, invoker_svc.clone());
        }
      )
    }

    /// loops forever on the invocation queue, running things when there are sufficient resources
    fn monitor_queue(&self, tid:&TransactionId, invoker_svc: Arc<InvokerService>) {
      debug!(tid=%tid, "invoker worker loop starting");
      let worker_rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => { 
          error!(tid=%tid, error=%e, "Tokio thread runtime failed to start");
          return ();
        },
      };
      self.continuation.thread_start(tid);
      while self.continuation.check_continue() {
        let mut queue = self.invoke_queue.lock();
        if queue.len() > 0 && self.has_resources_to_run() {
          let item = queue.remove(0);
          debug!(tid=%item.tid, "Dequeueing item");
          // TODO: continuity of spans here
          self.spawn_tokio_worker(&worker_rt, invoker_svc.clone(), item);
        } else {
          std::thread::sleep(Duration::from_millis(self.invocation_config.queue_sleep_ms));
        }
      }
      self.continuation.thread_exit(tid);
    }

    /// has_resources_to_run
    /// checks if the container manager (probably) has enough resources to run an invocation
    fn has_resources_to_run(&self) -> bool {
      self.cont_manager.free_cores() > 0
    }

    /// spawn_tokio_worker
    /// runs the specific invocation on a new tokio worker thread
    fn spawn_tokio_worker(&self, runtime: &tokio::runtime::Runtime, invoker_svc: Arc<InvokerService>, item: Arc<EnqueuedInvocation>) {
      let _handle = runtime.spawn(async move {
        debug!(tid=%item.tid, "Launching invocation thread for queued item");
        invoker_svc.invocation_worker_thread(item).await;
      });
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
    async fn invocation_worker_thread(&self, item: Arc<EnqueuedInvocation>) {
      match self.invoke_internal(&item.function_name, &item.function_version, &item.json_args, &item.tid).await {
        Ok(res) =>  {
          let mut result_ptr = item.result_ptr.lock();
          result_ptr.duration = res.1;
          result_ptr.result_json = res.0;
          result_ptr.completed = true;
          debug!(tid=%item.tid, "queued invocation completed successfully");
        },
        Err(cause) =>
        {
          if let Some(_core_err) = cause.downcast_ref::<InsufficientCoresError>() {
            debug!(tid=%item.tid, "Insufficient cores to run item right now");
            let mut queue = self.invoke_queue.lock();
            queue.insert(0, item.clone());
          } else if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
            warn!(tid=%item.tid, "Insufficient memory to run item right now");
            let mut queue = self.invoke_queue.lock();
            queue.insert(0, item.clone());
          } else {
            error!(tid=%item.tid, error=%cause, "Encountered unknown error while trying to run queued invocation");
            // TODO: insert smartly into queue
            let mut result_ptr = item.result_ptr.lock();
            if result_ptr.attempts >= self.invocation_config.retries {
              error!(tid=%item.tid, attempts=result_ptr.attempts, "Abandoning attempt to run invocation after attempts");
              result_ptr.duration = 0;
              result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", cause);
              result_ptr.completed = true;
            } else {
              result_ptr.attempts += 1;
              debug!(tid=%item.tid, attempts=result_ptr.attempts, "re-queueing invocation attempt after attempting");
              let mut queue = self.invoke_queue.lock();
              queue.push(item.clone());
            }
          }
        },
      };
    }

    pub fn queue_len(&self) -> usize {
      self.invoke_queue.lock().len()
    }

    /// Insert an invocation request into the queue and return a QueueFuture for it's execution result
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
    fn enqueue_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> QueueFuture {
      debug!(tid=%tid, "Enqueueing invocation");
      let fut = QueueFuture::new();
      let enqueue = Arc::new(EnqueuedInvocation::new(function_name, function_version, json_args, tid, fut.result.clone()));
      let mut invoke_queue = self.invoke_queue.lock();
      invoke_queue.push(enqueue);
      fut
    }

    /// synchronously run an invocation
    /// returns the json result and duration as a tuple
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, request), fields(tid=%request.transaction_id)))]
    pub async fn invoke(&self, request: InvokeRequest) -> Result<(String, u64)> {
      if self.invocation_config.use_queue {
        let fut = self.enqueue_invocation(request.function_name, request.function_version, request.json_args, request.transaction_id.clone()).await;
        info!(tid=%request.transaction_id, "Invocation complete");
        let fut = fut.lock();
        Ok( (fut.result_json.clone(), fut.duration) )  
      } else {
        self.queueless_invoke(request).await
      }
    }

    async fn queueless_invoke(&self, request: InvokeRequest) -> Result<(String, u64)> {
      match self.invoke_internal(&request.function_name, &request.function_version, &request.json_args, &request.transaction_id).await {
        Ok(res) =>  {
          Ok(res)
        },
        Err(cause) =>
        {
          if let Some(_core_err) = cause.downcast_ref::<InsufficientCoresError>() {
            warn!(tid=%request.transaction_id, "Insufficient cores to run item right now");
          } else if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
            warn!(tid=%request.transaction_id, "Insufficient memory to run item right now");
          } else {
            error!(tid=%request.transaction_id, error=%cause, "Encountered unknown error while trying to run queued invocation");
          }
          Err(cause)
        },
      }
    }

    fn track_running(&self, tid: &TransactionId) {
      self.running_functions.insert(tid.clone(), 1);
    }
    fn track_finished(&self, tid: &TransactionId) {
      self.running_functions.remove(tid);
    }
    pub fn get_running(&self) -> Vec<String> {
      let mut ret = vec![];
      for item in self.running_functions.iter() {
        let running = *item.value();
        if running > 0 {
          ret.push(item.key().clone());
        }
      }
      ret
    }
    pub fn get_running_string(&self) -> String {
      let mut ret = String::new();
      for item in self.running_functions.iter() {
        ret.push_str(item.key().as_str());
        ret.push(':');
      }
      ret
    }

    /// acquires a container and invokes the function inside it
    /// returns the json result and duration as a tuple
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
    async fn invoke_internal(&self, function_name: &String, function_version: &String, json_args: &String, tid: &TransactionId) -> Result<(String, u64)> {
      debug!(tid=%tid, "Internal invocation starting");

      let fqdn = calculate_fqdn(&function_name, &function_version);
      match self.cont_manager.acquire_container(&fqdn, tid).await {
        Ok(ctr_lock) => {
          self.track_running(tid);
          let start = SystemTime::now();
          let data = ctr_lock.invoke(json_args, self.function_config.timeout_sec).await?;
          let duration = match start.elapsed() {
            Ok(dur) => dur,
            Err(e) => bail_error!(tid=%tid, error=%e, "Timer error recording invocation duration"),
          }.as_millis() as u64;
          self.track_finished(tid);
          Ok((data, duration))
        },
        Err(cause) => Err(cause),
      }
    }

    /// Sets up an asyncronous invocation of the function
    /// Returns a lookup cookie the request can be found at
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(invoke_svc, request), fields(tid=%request.transaction_id)))]
    pub fn invoke_async(invoke_svc: Arc<Self>, request: InvokeAsyncRequest) -> Result<String> {
      debug!(tid=%request.transaction_id, "Inserting async invocation");
      let cookie = GUID::rand().to_string();
      if invoke_svc.invocation_config.use_queue {
        let fut = invoke_svc.enqueue_invocation(request.function_name, request.function_version, request.json_args, request.transaction_id.clone());
        invoke_svc.async_functions.insert(cookie.clone(), fut.result);
      } else {
        let result_ptr = InvocationResult::boxed();
        invoke_svc.async_functions.insert(cookie.clone(), result_ptr.clone());
        let transformed = InvokeRequest {
          function_name: request.function_name,
          function_version: request.function_version,
          memory: request.memory,
          json_args: request.json_args,
          transaction_id: request.transaction_id.clone(),
        };
        let cln = invoke_svc.clone();
        tokio::spawn(async move {
          match cln.queueless_invoke(transformed).await {
            Ok( (json, dur) ) => {
              let mut locked = result_ptr.lock();
              locked.duration = dur;
              locked.result_json = json;
              locked.attempts = 0;
              locked.completed = true;
            },
            Err(e) => {
              error!(tid=%request.transaction_id, error=%e, "Async invocation failed with error");
              let mut locked = result_ptr.lock();
              locked.duration = 0;
              locked.result_json = format!("{{ \"Error\": \"{}\" }}", e);
              locked.attempts = 0;
              locked.completed = true;
            },
          };
        });
      }
      Ok(cookie)
    }

    /// returns the async invoke entry if it exists
    /// None otherwise
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, cookie), fields(tid=%_tid)))]
    fn get_async_entry(&self, cookie: &String, _tid: &TransactionId) -> Option<InvocationResultPtr> {
      let i = self.async_functions.get(cookie);
      match i {
          Some(i) => Some(i.clone()),
          None => None,
      }
    }

    /// removes the async invoke entry from the tracked invocations
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, cookie), fields(tid=%_tid)))]
    fn remove_async_entry(&self, cookie: &String, _tid: &TransactionId) {
      self.async_functions.remove(cookie);
    }

    /// polls the invocation status
    /// Destructively returns results if they are found
    /// returns a JSON blob of "{ "Error": "Invocation not found" }" if the invocation is not found
    /// returns a JSON blob of "{ "Status": "Invocation not completed" }" if the invocation has not completed yet
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, cookie), fields(tid=%tid)))]
    pub fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<InvokeResponse> {
      let entry = match self.get_async_entry(cookie, tid) {
        Some(entry) => entry,
        None => return Ok(InvokeResponse {
          json_result: "{ \"Error\": \"Invocation not found\" }".to_string(),
          success: false,
          duration_ms: 0
        }),
      };

      let entry = entry.lock();
      if entry.completed {
        self.remove_async_entry(cookie, tid);
        return Ok(InvokeResponse {
          json_result: entry.result_json.to_string(),
          success: true,
          duration_ms: entry.duration,
        });
      }
      Ok(InvokeResponse {
        json_result: "{ \"Status\": \"Invocation not completed\" }".to_string(),
        success: false,
        duration_ms: 0
      })
    }
}
