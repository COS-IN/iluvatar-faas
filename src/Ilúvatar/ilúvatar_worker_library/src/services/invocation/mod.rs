use std::sync::Arc;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::transaction::TransactionId;
use tokio::sync::Semaphore;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use self::avail_scale_invoke::AvailableScalingInvoker;
use self::cold_priority_invoke::ColdPriorityInvoker;
use self::invoker_structs::{EnqueuedInvocation, InvocationResultPtr};
use self::queueing_invoker::QueueingInvoker;
use self::{queueless::QueuelessInvoker, fcfs_invoke::FCFSInvoker, minheap_invoke::MinHeapInvoker};
use self::{minheap_ed_invoke::MinHeapEDInvoker, fcfs_bypass_invoke::FCFSBypassInvoker, minheap_bypass_invoke::MinHeapBPInvoker, minheap_ed_bypass_invoke::MinHeapEDBPInvoker, minheap_iat_bypass_invoke::MinHeapIATBPInvoker };
use self::minheap_iat_invoke::MinHeapIATInvoker;
use super::containers::containermanager::ContainerManager;
use super::registration::RegisteredFunction;

pub mod invoker_structs;
pub mod queueless;
pub mod async_tracker;
pub mod fcfs_invoke;
pub mod minheap_invoke;
pub mod minheap_ed_invoke;
pub mod minheap_iat_invoke;
pub mod fcfs_bypass_invoke;
pub mod minheap_bypass_invoke;
pub mod minheap_ed_bypass_invoke;
pub mod minheap_iat_bypass_invoke;
mod cold_priority_invoke;
mod avail_scale_invoke;
pub mod queueing_invoker;

#[tonic::async_trait]
/// A trait representing the functionality a queue policy must implement
/// Overriding functions _must_ re-implement [info] level log statements for consistency
pub trait InvokerQueuePolicy: Send + Sync {
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

  /// Insert an item into the queue, optionally at a specific index
  /// If not specified, added to the end
  /// Wakes up the queue monitor thread
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, _item, _index), fields(tid=%_item.tid)))]
  fn add_item_to_queue(&self, _item: &Arc<EnqueuedInvocation>, _index: Option<usize>) { }

  /// Concurrency
  fn concurrency_semaphore(&self) -> Option<&Arc<Semaphore>>;
  fn bypass_running(&self) -> Option<&std::sync::atomic::AtomicU32> { None }
}

/// Returns a semaphore if there is an item in the option
/// Raises an error if the value is 0
fn create_concurrency_semaphore(permits: Option<u32>) -> Result<Option<Arc<Semaphore>>> {
  match permits {
    Some(0) => anyhow::bail!("Invoker concurrency semaphore cannot have 0 permits"),
    Some(p) => Ok(Some(Arc::new(Semaphore::new(p as usize)))),
    None => Ok(None),
  }
}

#[tonic::async_trait]
/// A trait representing the functionality a queue policy must implement
/// Overriding functions _must_ re-implement [info] level log statements for consistency
/// Re-implementers **must** duplicate [tracing::info] logs for consistency
pub trait Invoker: Send + Sync {
  /// A synchronous invocation against this invoker
  async fn sync_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<InvocationResultPtr>;

  /// Launch an async invocation of the function
  /// Return a lookup cookie that can be queried for results
  fn async_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<String>;

  /// Check the status of the result, if found is returned destructively
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<crate::rpc::InvokeResponse>;
  /// Number of invocations enqueued
  fn queue_len(&self) -> usize;
  /// Number of running invocations
  fn running_funcs(&self) -> u32;
}

pub struct InvokerFactory {
  cont_manager: Arc<ContainerManager>, 
  function_config: Arc<FunctionLimits>, 
  invocation_config: Arc<InvocationConfig>,
  cmap: Arc<CharacteristicsMap>,
}

impl InvokerFactory {
  pub fn new(cont_manager: Arc<ContainerManager>,
    function_config: Arc<FunctionLimits>,
    invocation_config: Arc<InvocationConfig>,
    cmap: Arc<CharacteristicsMap>) -> Self {

    InvokerFactory {
      cont_manager,
      function_config,
      invocation_config,
      cmap,
    }
  }

  fn get_invoker_queue(&self, tid: &TransactionId)  -> Result<Arc<dyn InvokerQueuePolicy>> {
    let r: Arc<dyn InvokerQueuePolicy> = match self.invocation_config.queue_policy.to_lowercase().as_str() {
      "none" => QueuelessInvoker::new()?,
      "fcfs" => FCFSInvoker::new(self.invocation_config.clone())?,
      "minheap" => MinHeapInvoker::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "minheap_ed" => MinHeapEDInvoker::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "minheap_iat" => MinHeapIATInvoker::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "minheap_iat_bypass" => MinHeapIATBPInvoker::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "minheap_ed_bypass" => MinHeapEDBPInvoker::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "minheap_bypass" => MinHeapBPInvoker::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "fcfs_bypass" => FCFSBypassInvoker::new(self.invocation_config.clone(), tid)?,
      "cold_pri" => ColdPriorityInvoker::new(self.cont_manager.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?,
      "scaling" => AvailableScalingInvoker::new(self.cont_manager.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?,
      unknown => panic!("Unknown queueing policy '{}'", unknown),
    };
    Ok(r)
  }

  pub fn get_invoker_service(&self, tid: &TransactionId) -> Result<Arc<dyn Invoker>> {
    let q = self.get_invoker_queue(tid)?;
    let invoker = QueueingInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone(), q)?;
    Ok(invoker)
  }
}
