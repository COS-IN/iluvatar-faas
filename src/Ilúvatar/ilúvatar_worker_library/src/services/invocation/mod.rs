use std::sync::Arc;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::transaction::TransactionId;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use self::avail_scale_q::AvailableScalingQueue;
use self::cold_priority_q::ColdPriorityQueue;
use self::invoker_structs::{EnqueuedInvocation, InvocationResultPtr};
use self::queueing_invoker::QueueingInvoker;
use self::{queueless::Queueless, fcfs_q::FCFSQueue, minheap_q::MinHeapQueue};
use self::{minheap_ed_q::MinHeapEDQueue, fcfs_bypass_q::FCFSBypassQueue, minheap_bypass_q::MinHeapBPQueue, minheap_ed_bypass_q::MinHeapEDBPQueue, minheap_iat_bypass_q::MinHeapIATBPQueue };
use self::minheap_iat_q::MinHeapIATQueue;
use super::containers::containermanager::ContainerManager;
use super::registration::RegisteredFunction;

pub mod invoker_structs;
pub mod queueless;
pub mod async_tracker;
pub mod fcfs_q;
pub mod minheap_q;
pub mod minheap_ed_q;
pub mod minheap_iat_q;
pub mod fcfs_bypass_q;
pub mod minheap_bypass_q;
pub mod minheap_ed_bypass_q;
pub mod minheap_iat_bypass_q;
mod cold_priority_q;
mod avail_scale_q;
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

  fn bypass_running(&self) -> Option<&std::sync::atomic::AtomicU32> { None }
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
      "none" => Queueless::new()?,
      "fcfs" => FCFSQueue::new()?,
      "minheap" => MinHeapQueue::new(tid, self.cmap.clone())?,
      "minheap_ed" => MinHeapEDQueue::new(tid, self.cmap.clone())?,
      "minheap_iat" => MinHeapIATQueue::new(tid, self.cmap.clone())?,
      "minheap_iat_bypass" => MinHeapIATBPQueue::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "minheap_ed_bypass" => MinHeapEDBPQueue::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "minheap_bypass" => MinHeapBPQueue::new(self.invocation_config.clone(), tid, self.cmap.clone())?,
      "fcfs_bypass" => FCFSBypassQueue::new(self.invocation_config.clone(), tid)?,
      "cold_pri" => ColdPriorityQueue::new(self.cont_manager.clone(), tid, self.cmap.clone())?,
      "scaling" => AvailableScalingQueue::new(self.cont_manager.clone(), tid, self.cmap.clone())?,
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
