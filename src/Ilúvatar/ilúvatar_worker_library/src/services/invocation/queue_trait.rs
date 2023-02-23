use std::sync::Arc;
use super::invoker_structs::EnqueuedInvocation;

#[allow(dyn_drop)]
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
}
