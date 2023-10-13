use super::{InvocationResult, InvocationResultPtr};
use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::{ContainerState, ParsedResult};
use crate::services::registration::RegisteredFunction;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use std::time::Duration;
use std::{cmp::Ordering, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::Notify;
use tracing::{debug, error};

//  CPU focused queues
pub mod avail_scale;
pub mod cold_priority;
pub mod fcfs;
pub mod minheap;
pub mod minheap_ed;
pub mod minheap_iat;
pub mod queueless;

// GPU focused queues
pub mod fcfs_gpu;
pub mod oldest_gpu;

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
pub trait InvokerCpuQueuePolicy: Send + Sync {
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
    fn add_item_to_queue(&self, _item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()>;

    /// Get the estimated wall-clock time of the function using the global [CharacteristicsMap]
    /// Checks container availability toget cold/warm/prewarm time
    fn est_wall_time(
        &self,
        item: &Arc<EnqueuedInvocation>,
        cont_manager: &Arc<ContainerManager>,
        cmap: &Arc<CharacteristicsMap>,
    ) -> Result<f64> {
        Ok(
            match cont_manager.container_available(&item.registration.fqdn, iluvatar_library::types::Compute::CPU) {
                ContainerState::Warm => cmap.get_warm_time(&item.registration.fqdn),
                ContainerState::Prewarm => cmap.get_prewarm_time(&item.registration.fqdn),
                _ => cmap.get_cold_time(&item.registration.fqdn),
            },
        )
    }
}

#[tonic::async_trait]
/// A trait for a device-specific queue
/// The implementer is responsible for invoking functions it is directed to enqueue
pub trait DeviceQueue: Send + Sync {
    /// The length of items waiting to be ran on the device
    fn queue_len(&self) -> usize;

    /// The estimated time from now the item would be completed if run on the device
    /// In seconds
    fn est_completion_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64;

    /// Insert an item into the queue
    /// If an error is returned, the item was not put enqueued
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
}

impl EnqueuedInvocation {
    pub fn new(
        registration: Arc<RegisteredFunction>,
        json_args: String,
        tid: TransactionId,
        queue_insert_time: OffsetDateTime,
    ) -> Self {
        EnqueuedInvocation {
            registration,
            json_args,
            tid,
            queue_insert_time,
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

    /// Lock item if it has not already been, meaning it can be run here.
    /// If so this will return `true` if this item has not been locked by another resource yet.
    /// `false` means another has taken it
    pub fn lock(&self) -> bool {
        let mut started = self.started.lock();
        match *started {
            true => false,
            false => {
                *started = true;
                true
            }
        }
    }

    /// Reset the marker stating this as locked, so it can be attempted to run again
    pub fn unlock(&self) {
        *self.started.lock() = false;
    }

    pub fn mark_successful(&self, result: ParsedResult, duration: Duration, compute: Compute, state: ContainerState) {
        let mut result_ptr = self.result_ptr.lock();
        result_ptr.duration = duration;
        result_ptr.exec_time = result.duration_sec;
        result_ptr.result_json = result
            .result_string()
            .unwrap_or_else(|cause| format!("{{ \"Error\": \"{}\" }}", cause));
        result_ptr.completed = true;
        result_ptr.worker_result = Some(result);
        result_ptr.compute = compute;
        result_ptr.container_state = state;
        self.signal();
        debug!(tid=%self.tid, "queued invocation completed successfully");
    }

    /// Increment the attempts counter on the function
    /// If the number of errors has exceeded the retries, mark it as complete and return `false` to indicate no re-trying
    pub fn increment_error_retry(&self, error: anyhow::Error, retries: u32) -> bool {
        let mut result_ptr = self.result_ptr.lock();
        if result_ptr.attempts >= retries {
            error!(tid=%self.tid, attempts=result_ptr.attempts, "Abandoning attempt to run invocation after attempts");
            result_ptr.duration = Duration::from_micros(0);
            result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", error);
            result_ptr.completed = true;
            self.signal();
            return false;
        } else {
            result_ptr.attempts += 1;
            self.unlock();
            return true;
        }
    }
}

pub struct MinHeapEnqueuedInvocation<T: Ord> {
    pub item: Arc<EnqueuedInvocation>,
    priority: T,
    est_wall_time: f64,
}

impl<T: Ord> MinHeapEnqueuedInvocation<T> {
    pub fn new(item: Arc<EnqueuedInvocation>, priority: T, est_wall_time: f64) -> Self {
        MinHeapEnqueuedInvocation {
            item,
            priority,
            est_wall_time,
        }
    }
}
pub type MinHeapFloat = MinHeapEnqueuedInvocation<OrderedFloat<f64>>;
impl MinHeapFloat {
    pub fn new_f(item: Arc<EnqueuedInvocation>, priority: f64, est_wall_time: f64) -> Self {
        MinHeapEnqueuedInvocation {
            item,
            priority: OrderedFloat(priority),
            est_wall_time,
        }
    }
}

impl<T: Ord> Eq for MinHeapEnqueuedInvocation<T> {}
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
    use super::*;
    use iluvatar_library::logging::LocalTime;
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
        MinHeapEnqueuedInvocation::new_f(
            Arc::new(EnqueuedInvocation::new(
                rf,
                name.to_string(),
                name.to_string(),
                clock.now(),
            )),
            priority,
            0.0,
        )
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
        assert_eq!(
            heap.pop()
                .expect("first item should exist")
                .item
                .registration
                .function_name,
            "1"
        );
        assert_eq!(
            heap.pop()
                .expect("second item should exist")
                .item
                .registration
                .function_name,
            "2"
        );
        assert_eq!(
            heap.pop()
                .expect("third item should exist")
                .item
                .registration
                .function_name,
            "3"
        );
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
        MinHeapEnqueuedInvocation::new(
            Arc::new(EnqueuedInvocation::new(
                rf,
                name.to_string(),
                name.to_string(),
                clock.now(),
            )),
            priority,
            0.0,
        )
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
        assert_eq!(
            heap.pop()
                .expect("first item should exist")
                .item
                .registration
                .function_name,
            "1"
        );
        assert_eq!(
            heap.pop()
                .expect("second item should exist")
                .item
                .registration
                .function_name,
            "2"
        );
        assert_eq!(
            heap.pop()
                .expect("third item should exist")
                .item
                .registration
                .function_name,
            "3"
        );
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
        MinHeapEnqueuedInvocation::new(
            Arc::new(EnqueuedInvocation::new(rf, name.to_string(), name.to_string(), t)),
            t,
            0.0,
        )
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
        assert_eq!(
            heap.pop()
                .expect("first item should exist")
                .item
                .registration
                .function_name,
            "1"
        );
        assert_eq!(
            heap.pop()
                .expect("second item should exist")
                .item
                .registration
                .function_name,
            "2"
        );
        assert_eq!(
            heap.pop()
                .expect("third item should exist")
                .item
                .registration
                .function_name,
            "3"
        );
    }
}
