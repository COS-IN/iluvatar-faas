use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration};
use crate::worker_api::worker_config::FunctionLimits;
use crate::services::containers::{containermanager::ContainerManager, structs::{InsufficientCoresError, InsufficientMemoryError}};
use crate::{rpc::{InvokeRequest, InvokeAsyncRequest, InvokeResponse}, utils::calculate_fqdn, transaction::{TransactionId, INVOKER_QUEUE_WORKER_TID}, bail_error};
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
  pub invoke_queue: Arc<Mutex<Vec<Arc<EnqueuedInvocation>>>>,
  pub config: Arc<FunctionLimits>,
  // TODO: occasionally check if this died and re-run?
  _worker_thread: std::thread::JoinHandle<()>,
}

impl InvokerService {
    fn new(cont_manager: Arc<ContainerManager>, config: Arc<FunctionLimits>, worker_thread: std::thread::JoinHandle<()>) -> Self {
      InvokerService {
        cont_manager,
        async_functions: Arc::new(DashMap::new()),
        invoke_queue: Arc::new(Mutex::new(Vec::new())),
        config,
        _worker_thread: worker_thread,
      }
    }

    pub fn boxed(cont_manager: Arc<ContainerManager>, tid: &TransactionId, config: Arc<FunctionLimits>) -> Arc<Self> {
      let (tx, rx) = channel();
      let handle = InvokerService::start_queue_thread(rx, tid);
      let i = Arc::new(InvokerService::new(cont_manager, config, handle));
      tx.send(i.clone()).unwrap();
      return i;
    }

    fn start_queue_thread(rx: Receiver<Arc<InvokerService>>, tid: &TransactionId) -> std::thread::JoinHandle<()> {
      debug!("[{}] Launching InvokerService queue thread", tid);
      // TODO: smartly manage the queue, not just FIFO?
      // run on an OS thread here so we don't get blocked by out userland threading runtime
      // If this thread crashes, we'll never know and the worker will deadlock
      std::thread::spawn(move || {
        let tid: &TransactionId = &INVOKER_QUEUE_WORKER_TID;
        let invoker_svc: Arc<InvokerService> = match rx.recv() {
            Ok(svc) => svc,
            Err(_) => {
              error!("[{}] invoker service thread failed to receive service from channel!", tid);
              return;
            },
          };
          debug!("[{}] invoker worker started", tid);
          invoker_svc.monitor_queue(tid, invoker_svc.clone());
        }
      )
    }

    /// loops forever on the invocation queue, running things when there are sufficient resources
    fn monitor_queue(&self, tid:&TransactionId, invoker_svc: Arc<InvokerService>) {
      debug!("[{}] invoker worker loop starting", tid);
      let worker_rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => { 
          error!("[{}] tokio thread runtime failed to start {}", tid, e);
          return ();
        },
      };
      loop {
        if self.has_resources_to_run() {
          let mut queue = self.invoke_queue.lock();
          if queue.len() > 0 {
            let item = queue.remove(0);
            debug!("[{}] Dequeueing item", &item.tid);
            self.spawn_tokio_worker(&worker_rt, invoker_svc.clone(), item);
          }
        } else {
          std::thread::sleep(Duration::from_millis(1));
        }
      }
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
        debug!("[{}] Launching invocation thread for queued item", &item.tid);
        invoker_svc.invocation_worker_thread(item).await;
      });
    }

    async fn invocation_worker_thread(&self, item: Arc<EnqueuedInvocation>) {
      match self.invoke_internal(&item.function_name, &item.function_version, &item.json_args, &item.tid).await {
          Ok(res) =>  {
            let mut result_ptr = item.result_ptr.lock();
            result_ptr.duration = res.1;
            result_ptr.result_json = res.0;
            result_ptr.completed = true;
            debug!("[{}] queued invocation completed successfully", &item.tid);
          },
          Err(cause) =>
          {
            if let Some(_core_err) = cause.downcast_ref::<InsufficientCoresError>() {
              debug!("[{}] Insufficient cores to run item right now", &item.tid);
              let mut queue = self.invoke_queue.lock();
              queue.insert(0, item.clone());
            } else if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
              warn!("[{}] Insufficient memory to run item right now", &item.tid);
              let mut queue = self.invoke_queue.lock();
              queue.insert(0, item.clone());
            } else {
              error!("[{}] Encountered unknown error while trying to run queued invocation '{}'", &item.tid, cause);
              // TODO: insert smartly into queue
              let mut result_ptr = item.result_ptr.lock();
              if result_ptr.attempts >= self.config.retries {
                error!("[{}] Abandoning attempt to run invocation after {} errors", &item.tid, result_ptr.attempts);
                result_ptr.duration = 0;
                result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", cause);
                result_ptr.completed = true;
              } else {
                result_ptr.attempts += 1;
                debug!("[{}] re-queueing invocation attempt with {} errors", &item.tid, result_ptr.attempts);
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
    #[tracing::instrument(skip(self))]
    fn enqueue_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> QueueFuture {
      debug!("[{}] Enqueueing invocation", tid);
      let fut = QueueFuture::new();
      let enqueue = Arc::new(EnqueuedInvocation::new(function_name, function_version, json_args, tid, fut.result.clone()));
      let mut invoke_queue = self.invoke_queue.lock();
      invoke_queue.push(enqueue);
      fut
    }

    /// synchronously run an invocation
    /// returns the json result and duration as a tuple
    #[tracing::instrument(skip(self))]
    pub async fn invoke(&self, request: InvokeRequest) -> Result<(String, u64)> {
      let fut = self.enqueue_invocation(request.function_name, request.function_version, request.json_args, request.transaction_id.clone()).await;
      info!("[{}] Invocation complete", request.transaction_id);
      let fut = fut.lock();
      Ok( (fut.result_json.clone(), fut.duration) )
    }

    /// acquires a container and invokes the function inside it
    /// returns the json result and duration as a tuple
    async fn invoke_internal(&self, function_name: &String, function_version: &String, json_args: &String, tid: &TransactionId) -> Result<(String, u64)> {
      debug!("[{}] Internal invocation starting", tid);

      let fqdn = calculate_fqdn(&function_name, &function_version);
      match self.cont_manager.acquire_container(&fqdn, tid).await {
        Ok(ctr_lock) => {
          let start = SystemTime::now();
          let data = ctr_lock.invoke(json_args).await?;
          let duration = match start.elapsed() {
            Ok(dur) => dur,
            Err(e) => bail_error!("[{}] timer error recording invocation duration '{}'", tid, e),
          }.as_millis() as u64;
          Ok((data, duration))
        },
        Err(cause) => Err(cause),
      }
    }

    /// Sets up an asyncronous invocation of the function
    /// Returns a lookup cookie the request can be found at
    pub fn invoke_async(&self, request: InvokeAsyncRequest) -> Result<String> {
      debug!("[{}] Inserting async invocation", request.transaction_id);
      let fut = self.enqueue_invocation(request.function_name, request.function_version, request.json_args, request.transaction_id.clone());
      let cookie = GUID::rand().to_string();
      self.async_functions.insert(cookie.clone(), fut.result);
      Ok(cookie)
    }

    /// returns the async invoke entry if it exists
    /// None otherwise
    fn get_async_entry(&self, cookie: &String) -> Option<InvocationResultPtr> {
      let i = self.async_functions.get(cookie);
      match i {
          Some(i) => Some(i.clone()),
          None => None,
      }
    }

    /// removes the async invoke entry from the tracked invocations
    fn remove_async_entry(&self, cookie: &String) {
      self.async_functions.remove(cookie);
    }

    /// polls the invocation status
    /// Destructively returns results if they are found
    /// returns a JSON blob of "{ "Error": "Invocation not found" }" if the invocation is not found
    /// returns a JSON blob of "{ "Status": "Invocation not completed" }" if the invocation has not completed yet
    pub fn invoke_async_check(&self, cookie: &String) -> Result<InvokeResponse> {
      let entry = match self.get_async_entry(cookie) {
        Some(entry) => entry,
        None => return Ok(InvokeResponse {
          json_result: "{ \"Error\": \"Invocation not found\" }".to_string(),
          success: false,
          duration_ms: 0
        }),
      };

      let entry = entry.lock();
      if entry.completed {
        self.remove_async_entry(cookie);
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
