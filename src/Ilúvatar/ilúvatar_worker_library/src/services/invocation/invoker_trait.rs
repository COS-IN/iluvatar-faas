use std::{sync::Arc, time::Duration};
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::logging::LocalTime;
use iluvatar_library::{utils::calculate_fqdn, transaction::{TransactionId}, threading::EventualItem};
use time::OffsetDateTime;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{debug, error, info};
use anyhow::Result;
use super::invoker_structs::EnqueuedInvocation;
use crate::rpc::InvokeResponse;

/// Check the invocation queue, running things when there are sufficient resources
#[cfg_attr(feature = "full_spans", tracing::instrument(skip(invoker_svc), fields(tid=%_tid)))]
pub async fn monitor_queue<T: Invoker + 'static>(invoker_svc: Arc<T>, _tid: TransactionId) {
  loop {
    if let Some(peek_item) = invoker_svc.peek_queue() {
      if let Some(permit) = invoker_svc.acquire_resources_to_run(&peek_item) {
        let item = invoker_svc.pop_queue();
        debug!(tid=%item.tid, "Dequeueing item");
        // TODO: continuity of spans here
        invoker_svc.spawn_tokio_worker(invoker_svc.clone(), item, permit);  
      }else { break; }
      // nothing can be run, or nothing to run
    } else { break; }
  }
}

#[tonic::async_trait]
pub trait Invoker: Send + Sync {
  fn cont_manager(&self) -> Arc<ContainerManager>;
  fn function_config(&self) -> Arc<FunctionLimits>;
  fn invocation_config(&self) -> Arc<InvocationConfig>;
  fn timer(&self) -> &LocalTime;
  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<(String, Duration)>;
  fn async_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<String>;
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<InvokeResponse>;

  /// The length of a queue, if the implementation has one
  /// Default is 0 if not overridden
  /// An implementing struct only needs to implement this if it uses [monitor_queue]
  fn queue_len(&self) -> usize { 0 }
  /// A peek at the first item in the queue
  /// Returns [Some(Arc<EnqueuedInvocation>)] if there is anything in the queue, [None] otherwise
  /// An implementing struct only needs to implement this if it uses [monitor_queue]
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {todo!()}
  /// Destructively return the first item in the queue
  /// An implementing struct only needs to implement this if it uses [monitor_queue]
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {todo!()}

  /// Returns an owned permit if there are sufficient resources to run a function
  fn acquire_resources_to_run(&self, item: &Arc<EnqueuedInvocation>) -> Option<OwnedSemaphorePermit> {
    match self.cont_manager().try_acquire_cores(&item.fqdn) {
      Ok(c) => Some(c),
      Err(e) => { 
        match e {
          tokio::sync::TryAcquireError::Closed => error!(tid=%item.tid, "Container manager `try_acquire_cores` returned a closed error!"),
          tokio::sync::TryAcquireError::NoPermits => (),
        };
        None
      },
    }
  }

  /// Insert an item into the queue, optionally at a specific index
  /// If not specified, added to the end
  /// Wakes up the queue monitor thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, _item, _index), fields(tid=%_item.tid)))]
  fn add_item_to_queue(&self, _item: &Arc<EnqueuedInvocation>, _index: Option<usize>) { }

  /// Runs the specific invocation inside a new tokio worker thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, invoker_svc, item), fields(tid=%item.tid)))]
  fn spawn_tokio_worker(&self, invoker_svc: Arc<dyn Invoker>, item: Arc<EnqueuedInvocation>, permit: OwnedSemaphorePermit) {
    let _handle = tokio::spawn(async move {
      debug!(tid=%item.tid, "Launching invocation thread for queued item");
      invoker_svc.invocation_worker_thread(item, permit).await;
    });
  }

  /// Handle executing an invocation, plus account for its success or failure
  /// On success, the results are moved to the pointer and it is signaled
  /// On failure, [Invoker::handle_invocation_error] is called
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
  async fn invocation_worker_thread(&self, item: Arc<EnqueuedInvocation>, permit: OwnedSemaphorePermit) {
    match self.invoke_internal(&item.fqdn, &item.function_name, &item.function_version, &item.json_args, &item.tid, item.queue_insert_time, Some(permit)).await {
      Ok( (json, duration) ) =>  {
        let mut result_ptr = item.result_ptr.lock();
        result_ptr.duration = duration;
        result_ptr.result_json = json;
        result_ptr.completed = true;
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
  /// By default always logs the error and marks it as failed
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, cause), fields(tid=%item.tid)))]
  fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
    let mut result_ptr = item.result_ptr.lock();
    error!(tid=%item.tid, attempts=result_ptr.attempts, "Abandoning attempt to run invocation after attempts");
    result_ptr.duration = Duration::from_micros(0);
    result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", cause);
    result_ptr.completed = true;
    item.signal();
  }

  /// Forms invocation data into a [EnqueuedInvocation] that is returned
  /// The default implementation also calls [Invoker::add_item_to_queue] to optionally insert that item into the implementation's queue
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
  fn enqueue_new_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Arc<EnqueuedInvocation> {
    debug!(tid=%tid, "Enqueueing invocation");
    let fqdn = calculate_fqdn(&function_name, &function_version);
    let enqueue = Arc::new(EnqueuedInvocation::new(fqdn, function_name, function_version, json_args, tid, self.timer().now()));
    self.add_item_to_queue(&enqueue, None);
    enqueue
  }

  /// acquires a container and invokes the function inside it
  /// returns the json result and duration as a tuple
  /// The optional [permit] is dropped to return held resources
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args, queue_insert_time), fields(tid=%tid)))]
  async fn invoke_internal(&self, fqdn: &String, _function_name: &String, _function_version: &String, json_args: &String, tid: &TransactionId, queue_insert_time: OffsetDateTime, permit: Option<OwnedSemaphorePermit>) -> Result<(String, Duration)> {
    debug!(tid=%tid, "Internal invocation starting");
    let timer = self.timer();
    // take run time now because we may have to wait to get a container
    let run_time = timer.now_str();

    let ctr_mgr = self.cont_manager();
    let ctr_lock = match ctr_mgr.acquire_container(fqdn, tid) {
      EventualItem::Future(f) => f.await?,
      EventualItem::Now(n) => n?,
    };
    info!(tid=%tid, insert_time=%timer.format_time(queue_insert_time)?, run_time=%run_time?, "Item starting to execute");
    let (data, duration) = ctr_lock.invoke(json_args).await?;
    drop(permit);
    Ok((data.result_string()?, duration))
  }
}
