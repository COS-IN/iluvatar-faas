use std::{sync::Arc, cmp::Ordering};
use anyhow::Result;
use iluvatar_library::transaction::TransactionId;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::sync::Notify;

use crate::services::registration::RegisteredFunction;

use super::{InvocationResultPtr, InvocationResult};

//  CPU focused queues
pub mod queueless;
pub mod fcfs;
pub mod minheap;
pub mod minheap_ed;
pub mod minheap_iat;
pub mod cold_priority;
pub mod avail_scale;
pub mod oldest_gpu;

// GPU focused queues
pub mod fcfs_gpu;

#[derive(Debug, serde::Deserialize)]
/// The policy by which polymorphic functions will be enqueued in the CPU/GPU/etc. queues
pub enum EnqueueingPolicy {
  /// Invocations will be placed in any relevant queue, and the first one to start first wins
  All,
  /// Always enqueue on the compute that gives shortest compute time
  ShortestExecTime,
  /// Always enqueue on CPU
  /// Assumes all functions can run on CPU, assumption may break in the future
  AlwaysCPU,
  /// Enqueue based on shortest estimated completion time
  EstCompTime,
}

#[tonic::async_trait]
/// A trait representing the functionality a queue policy must implement
pub trait InvokerQueuePolicy: Send + Sync {
  /// The length of a queue, if the implementation has one
  fn queue_len(&self) -> usize;

  /// The estimated time of running everything in the queue
  /// In seconds
  fn est_queue_time(&self) -> f64;

  /// A peek at the first item in the queue.
  /// Returns an [EnqueuedInvocation] if there is anything in the queue, [None] otherwise.
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>>;

  /// Destructively return the first item in the queue.
  /// This function will only be called if something is known to be un the queue, so using `unwrap` to remove an [Option] is safe
  fn pop_queue(&self) -> Arc<EnqueuedInvocation>;

  /// Insert an item into the queue, optionally at a specific index
  /// If not specified, added to the end
  /// If an error is returned, the item was not put enqueued
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, _item, _index), fields(tid=%_item.tid)))]
  fn add_item_to_queue(&self, _item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()>;
}

#[tonic::async_trait]
/// A trait for a device-specific queue
/// The implementer is responsible for invoking functions it is directed to enqueue
pub trait DeviceQueue: Send + Sync {
  /// The length of items waiting to be ran on the device
  fn queue_len(&self) -> usize;

  /// The estimated time from now the item would be completed if run on the device
  /// In seconds
  fn est_completion_time(&self, reg: &Arc<RegisteredFunction>) -> f64;

  /// Insert an item into the queue
  /// If an error is returned, the item was not put enqueued
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
  fn enqueue_item(&self, item: &Arc<EnqueuedInvocation>) -> Result<()>;

  /// Number of invocations currently running
  fn running(&self) -> u32;
}

#[derive(Debug)]
/// A struct to hold a function while it is in the invocation queue
pub struct EnqueuedInvocation {
  pub registration: Arc<RegisteredFunction>,
  /// Pointer where results will be stored on invocation completion
  pub result_ptr: InvocationResultPtr,
  pub json_args: String, 
  pub tid: TransactionId,
  signal: Notify,
  /// Used to ensure an invocation is started only once
  /// Items can currently be placed into multiple queues if they can run on multiple resources
  pub started: Mutex<bool>,
  /// The local time at which the item was inserted into the queue
  pub queue_insert_time: OffsetDateTime,
  /// The estimated time the invocation will take to execute, in seconds
  pub est_execution_time: f64,
}

impl EnqueuedInvocation {
  pub fn new(registration: Arc<RegisteredFunction>, json_args: String, tid: TransactionId, queue_insert_time: OffsetDateTime, est_execution_time: f64) -> Self {
    EnqueuedInvocation {
      registration, est_execution_time, json_args, tid, queue_insert_time,
      result_ptr: InvocationResult::boxed(),
      signal: Notify::new(),
      started: Mutex::new(false),
    }
  }

  /// Block the current thread until the invocation is complete
  /// Only one waiter on an invocation is currently supported
  /// If the invocation is complete, this will return immediately
  pub async fn wait(&self, _tid: &TransactionId) -> Result<()> {
    self.signal.notified().await;
    Ok(())
  }

  /// Signal the current waiter to wake up because the invocation result is available
  /// If no one is waiting, the next person to check will continue immediately
  pub fn signal(&self) {
    self.signal.notify_one();
  }
}

pub struct MinHeapEnqueuedInvocation<T: Ord> {
  pub item: Arc<EnqueuedInvocation>,
  priority: T
}

impl<T: Ord> MinHeapEnqueuedInvocation<T> {
  pub fn new( item: Arc<EnqueuedInvocation>, priority: T ) -> Self {
    MinHeapEnqueuedInvocation { item, priority, }
  }
}
pub type MinHeapFloat = MinHeapEnqueuedInvocation<OrderedFloat<f64>>;
impl MinHeapFloat {
  pub fn new_f( item: Arc<EnqueuedInvocation>, priority: f64 ) -> Self {
    MinHeapEnqueuedInvocation { item, priority:OrderedFloat(priority), }
  }
}

impl<T: Ord> Eq for MinHeapEnqueuedInvocation<T> {
}
impl<T: Ord> Ord for MinHeapEnqueuedInvocation<T> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.priority.cmp(&other.priority).reverse()
  }
}
impl<T: Ord> PartialOrd for MinHeapEnqueuedInvocation<T> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.priority.cmp(&other.priority).reverse())
  }
}
impl<T: Ord> PartialEq for MinHeapEnqueuedInvocation<T> {
  fn eq(&self, other: &Self) -> bool {
    self.priority == other.priority
  }
}

#[cfg(test)]
mod heapstructs {
  use iluvatar_library::logging::LocalTime;
  use super::*;
  use std::collections::BinaryHeap;

  fn min_item(name: &str, priority: f64, clock: &LocalTime) -> MinHeapFloat {
    let rf = Arc::new(RegisteredFunction {
      function_name: name.to_string(),
      function_version: name.to_string(),
      fqdn: name.to_string(),
      image_name: name.to_string(),
      memory: 1,
      cpus: 1,
      snapshot_base: "".to_string(),
      parallel_invokes: 1,
      isolation_type: iluvatar_library::types::Isolation::CONTAINERD,
      supported_compute: iluvatar_library::types::Compute::CPU,
    });
    MinHeapEnqueuedInvocation::new_f(Arc::new(EnqueuedInvocation::new(rf,name.to_string(),name.to_string(), clock.now(), 0.0)), priority)
  }

  #[test]
  fn min_f64() {
    let clock = LocalTime::new(&"clock".to_string()).unwrap();
    let mut heap = BinaryHeap::new();
    let item1 = min_item("1", 1.0, &clock);
    let item2 = min_item("2", 2.0, &clock);
    let item3 = min_item("3", 3.0, &clock);
    heap.push(item1);
    heap.push(item2);
    heap.push(item3);
    assert_eq!(heap.pop().expect("first item should exist").item.registration.function_name, "1");
    assert_eq!(heap.pop().expect("second item should exist").item.registration.function_name, "2");
    assert_eq!(heap.pop().expect("third item should exist").item.registration.function_name, "3");
  }

  fn item_i64(name: &str, priority: i64, clock: &LocalTime) -> MinHeapEnqueuedInvocation<i64> {
    let rf = Arc::new(RegisteredFunction {
      function_name: name.to_string(),
      function_version: name.to_string(),
      fqdn: name.to_string(),
      image_name: name.to_string(),
      memory: 1,
      cpus: 1,
      snapshot_base: "".to_string(),
      parallel_invokes: 1,
      isolation_type: iluvatar_library::types::Isolation::CONTAINERD,
      supported_compute: iluvatar_library::types::Compute::CPU,
    });
    MinHeapEnqueuedInvocation::new(Arc::new(EnqueuedInvocation::new(rf,name.to_string(),name.to_string(), clock.now(), 0.0)), priority)
  }
  #[test]
  fn min_i64() {
    let clock = LocalTime::new(&"clock".to_string()).unwrap();
    let mut heap = BinaryHeap::new();
    let item1 = item_i64("1", 1, &clock);
    let item2 = item_i64("2", 2, &clock);
    let item3 = item_i64("3", 3, &clock);
    heap.push(item1);
    heap.push(item2);
    heap.push(item3);
    assert_eq!(heap.pop().expect("first item should exist").item.registration.function_name, "1");
    assert_eq!(heap.pop().expect("second item should exist").item.registration.function_name, "2");
    assert_eq!(heap.pop().expect("third item should exist").item.registration.function_name, "3");
  }
  fn item_datetime(name: &str, clock: &LocalTime) -> MinHeapEnqueuedInvocation<OffsetDateTime> {
    let t = clock.now();
    let rf = Arc::new(RegisteredFunction {
      function_name: name.to_string(),
      function_version: name.to_string(),
      fqdn: name.to_string(),
      image_name: name.to_string(),
      memory: 1,
      cpus: 1,
      snapshot_base: "".to_string(),
      parallel_invokes: 1,
      isolation_type: iluvatar_library::types::Isolation::CONTAINERD,
      supported_compute: iluvatar_library::types::Compute::CPU,
    });
    MinHeapEnqueuedInvocation::new(Arc::new(EnqueuedInvocation::new(rf,name.to_string(),name.to_string(), t, 0.0)), t)
  }
  #[test]
  fn min_datetime() {
    let clock = LocalTime::new(&"clock".to_string()).unwrap();
    let mut heap = BinaryHeap::new();
    let item1 = item_datetime("1", &clock);
    let item2 = item_datetime("2", &clock);
    let item3 = item_datetime("3", &clock);
    heap.push(item1);
    heap.push(item2);
    heap.push(item3);
    assert_eq!(heap.pop().expect("first item should exist").item.registration.function_name, "1");
    assert_eq!(heap.pop().expect("second item should exist").item.registration.function_name, "2");
    assert_eq!(heap.pop().expect("third item should exist").item.registration.function_name, "3");
  }
}
