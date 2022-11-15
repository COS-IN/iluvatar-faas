use std::{sync::Arc, time::Duration};
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{utils::calculate_fqdn, transaction::{TransactionId}, threading::EventualItem};
use tracing::{debug, error};
use anyhow::Result;
use super::invoker_structs::EnqueuedInvocation;
use crate::rpc::InvokeResponse;

#[tonic::async_trait]
pub trait Invoker: Send + Sync {
  fn cont_manager(&self) -> Arc<ContainerManager>;
  fn function_config(&self) -> Arc<FunctionLimits>;
  fn invocation_config(&self) -> Arc<InvocationConfig>;
  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<(String, Duration)>;
  fn async_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<String>;
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<InvokeResponse>;

  /// checks if the container manager (probably) has enough resources to run an invocation
  fn has_resources_to_run(&self) -> bool {
    self.cont_manager().free_cores() > 0
  }
  /// The length of a queue, if the implementation has one
  /// Default is 0 if not overridden
  fn queue_len(&self) -> usize {
    0
  }

  /// Insert an item into the queue, optionally at a specific index
  /// If not specified, added to the end
  /// Wakes up the queue monitor thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, _item, _index), fields(tid=%_item.tid)))]
  fn add_item_to_queue(&self, _item: &Arc<EnqueuedInvocation>, _index: Option<usize>) { }

  /// Runs the specific invocation inside a new tokio worker thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, invoker_svc, item), fields(tid=%item.tid)))]
  fn spawn_tokio_worker(&self, invoker_svc: Arc<dyn Invoker>, item: Arc<EnqueuedInvocation>) {
    let _handle = tokio::spawn(async move {
      debug!(tid=%item.tid, "Launching invocation thread for queued item");
      invoker_svc.invocation_worker_thread(item).await;
    });
  }

  /// Handle executing an invocation, plus account for its success or failure
  /// On success, the results are moved to the pointer and it is signaled
  /// On failure, [Invoker::handle_invocation_error] is called
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
  async fn invocation_worker_thread(&self, item: Arc<EnqueuedInvocation>) {
    match self.invoke_internal(&item.function_name, &item.function_version, &item.json_args, &item.tid).await {
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

  /// Insert an invocation request into the queue and return a QueueFuture for it's execution result
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
  fn enqueue_new_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Arc<EnqueuedInvocation> {
    debug!(tid=%tid, "Enqueueing invocation");
    let enqueue = Arc::new(EnqueuedInvocation::new(function_name, function_version, json_args, tid));
    self.add_item_to_queue(&enqueue, None);
    enqueue
  }

  /// acquires a container and invokes the function inside it
  /// returns the json result and duration as a tuple
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, function_name, function_version, json_args), fields(tid=%tid)))]
  async fn invoke_internal(&self, function_name: &String, function_version: &String, json_args: &String, tid: &TransactionId) -> Result<(String, Duration)> {
    debug!(tid=%tid, "Internal invocation starting");

    let fqdn = calculate_fqdn(&function_name, &function_version);
    let ctr_mgr = self.cont_manager();
    let ctr_lock = match ctr_mgr.acquire_container(fqdn, tid) {
      EventualItem::Future(f) => f.await?,
      EventualItem::Now(n) => n?,
    };
    let (data, duration) = ctr_lock.invoke(json_args).await?;
    Ok((data.result_string()?, duration))
  }
}
