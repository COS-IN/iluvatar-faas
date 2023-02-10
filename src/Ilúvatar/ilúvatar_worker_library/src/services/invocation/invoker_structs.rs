use std::{sync::Arc, time::Duration, cmp::Ordering};
use iluvatar_library::transaction::TransactionId;
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::sync::Notify;
use anyhow::Result;
use crate::services::{containers::structs::ParsedResult, registration::RegisteredFunction};
use ordered_float::OrderedFloat;

#[derive(Debug)]
#[allow(unused)]
pub struct InvocationResult {
  /// The output from the invocation
  pub result_json: String,
  /// The invocation time as tracked by the worker
  pub duration: Duration,
  pub attempts: u32,
  pub completed: bool,
  /// The invocation time as recorded by the platform inside the container
  pub exec_time: f64,
  pub worker_result: Option<ParsedResult>
}
impl InvocationResult {
  pub fn boxed() -> InvocationResultPtr {
    Arc::new(Mutex::new(InvocationResult {
      completed: false,
      duration: Duration::from_micros(0),
      result_json: "".to_string(),
      attempts: 0,
      exec_time: 0.0,
      worker_result: None,
    }))
  }
}

pub type InvocationResultPtr = Arc<Mutex<InvocationResult>>;

#[derive(Debug)]
/// A struct to hold a function while it is in the invocation queue
pub struct EnqueuedInvocation {
  pub registration: Arc<RegisteredFunction>,
  /// Pointer where results will be stored on invocation completion
  pub result_ptr: InvocationResultPtr,
  pub json_args: String, 
  pub tid: TransactionId,
  signal: Notify,
  /// The local time at which the item was inserted into the queue
  pub queue_insert_time: OffsetDateTime,
}

impl EnqueuedInvocation {
  pub fn new(registration: Arc<RegisteredFunction>, json_args: String, tid: TransactionId, queue_insert_time: OffsetDateTime) -> Self {
    EnqueuedInvocation {
      registration,
      result_ptr: InvocationResult::boxed(),
      json_args,
      tid,
      signal: Notify::new(),
      queue_insert_time
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
      isolation_type: iluvatar_library::types::Isolation::SIMULATION,
      supported_compute: iluvatar_library::types::Compute::CPU,
    });
    MinHeapEnqueuedInvocation::new_f(Arc::new(EnqueuedInvocation::new(rf,name.to_string(),name.to_string(), clock.now())), priority)
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
      isolation_type: iluvatar_library::types::Isolation::SIMULATION,
      supported_compute: iluvatar_library::types::Compute::CPU,
    });
    MinHeapEnqueuedInvocation::new(Arc::new(EnqueuedInvocation::new(rf,name.to_string(),name.to_string(), clock.now())), priority)
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
      isolation_type: iluvatar_library::types::Isolation::SIMULATION,
      supported_compute: iluvatar_library::types::Compute::CPU,
    });
    MinHeapEnqueuedInvocation::new(Arc::new(EnqueuedInvocation::new(rf,name.to_string(),name.to_string(), t)), t)
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
