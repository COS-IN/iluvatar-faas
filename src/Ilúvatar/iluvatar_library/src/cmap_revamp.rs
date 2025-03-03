use dashmap::DashMap;
use std::sync::Arc;

// A better characteristics map system
//
// What we 'want' it to do:
//     Store these per-fqdn
//     Store only f64 values
//     Have a unique entry for all values in <T>, where T is some enum
//     Support a unique entry for MIN, MAX, AVG, LATEST of each <T>
//         Do we want to require external separate enum values for each compute, CpuExecTime, GpuExecTime, etc.?
//         Or have the cmap internally track CPU, GPU, etc. distinction?
//     Minimal locking on add / update and retrieval
//     An optional read-only wrapper to forward cmap to a simulation
//     Easy cloning of all of an FQDN's values
//     easy cloning / serializing to send to controller for LB purposes
//     Allow updating multiple enum entries in one call
//     Allow retrieving multiple enum entries in one call
//
// Not implemented yet:
//     cloning
//     exporting data
//     multiple retrieval

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[repr(usize)]
pub enum Chars {
    /// Running avg of _all_ times on CPU for invocations.
    /// Recorded by invoke_on_container_2
    CpuExecTime = 0,
    /// Time on CPU for a warm invocation.
    /// Recorded by invoke_on_container_2
    CpuWarmTime,
    /// Time on CPU for a pre-warmed invocation.
    /// The container was previously started, but no invocation was run on it
    /// Recorded by invoke_on_container_2
    CpuPreWarmTime,
    /// E2E time for a CPU cold start.
    /// Recorded by invoke_on_container_2
    CpuColdTime,
    /// Running avg of _all_ times on GPU for invocations.
    /// Recorded by invoke_on_container_2
    GpuExecTime,
    /// Time on GPU for a warm invocation.
    /// Recorded by invoke_on_container_2
    GpuWarmTime,
    /// Time on GPU for a pre-warmed invocation.
    /// The container was previously started, but no invocation was run on it
    /// Recorded by invoke_on_container_2
    GpuPreWarmTime,
    /// E2E time for a GPU cold start.
    /// Recorded by invoke_on_container_2
    GpuColdTime,
    /// The last time an invocation happened.
    /// Recorded internally by the [CharacteristicsMap::add_iat] function
    LastInvTime,
    /// The running avg IAT.
    /// Recorded by iluvatar_worker_library::worker_api::iluvatar_worker::IluvatarWorkerImpl::invoke and iluvatar_worker_library::worker_api::iluvatar_worker::IluvatarWorkerImpl::invoke_async
    IAT,
    /// The running avg memory usage
    /// TODO: record this somewhere
    MemoryUsage,
    /// Total end to end latency: queuing plus execution
    E2ECpu,
    E2EGpu,

    /// Time estimate for the E2E GPU and CPU time
    EstGpu,
    EstCpu,

    /// Also store the error?
    QueueErrGpu,
    QueueErrCpu,
}
impl Max for Chars {
    const MAX: usize = Self::QueueErrCpu as usize;
}
impl num_traits::AsPrimitive<usize> for Chars {
    #[inline(always)]
    fn as_(self) -> usize {
        self as usize
    }
}

/// Trait ensuring that there is a maximal value that is also constant.
/// Used to index into the char map fqdn array.
pub trait Max {
    const MAX: usize;
}

#[repr(usize)]
pub enum Value {
    Min = 0,
    Max,
    Avg,
    Latest,
}

pub trait CharMap<T: num_traits::AsPrimitive<usize> + Max> {
    fn update(&self, fqdn: &str, key: T, value: f64);

    fn get(&self, fqdn: &str, key: T, value: Value) -> f64;
    fn get_min(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Min)
    }
    fn get_max(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Max)
    }
    fn get_avg(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Avg)
    }
    fn get_latest(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Latest)
    }

    /// A read-only pointer to this char map.
    /// Others can still write to it.
    fn read_only(&self, slf: Arc<dyn CharMap<T>>) -> Arc<dyn CharMap<T>> {
        Arc::new(CharMapRO { inner: slf })
    }
}

struct CharMapRO<T: num_traits::AsPrimitive<usize>> {
    inner: Arc<dyn CharMap<T>>,
}
impl<T: Max + num_traits::AsPrimitive<usize>> CharMap<T> for CharMapRO<T> {
    fn update(&self, _fqdn: &str, _key: T, _value: f64) {
        // do nothing
    }

    fn get(&self, fqdn: &str, key: T, value: Value) -> f64 {
        self.inner.get(fqdn, key, value)
    }
}

pub struct CharMapRW<const T: usize> {
    data: DashMap<String, [f64; T]>,
}
impl<T: Max + num_traits::AsPrimitive<usize>, const S: usize> CharMap<T> for CharMapRW<{ S }> {
    fn update(&self, fqdn: &str, key: T, value: f64) {
        match self.data.get_mut(fqdn) {
            None => {
                self.data.insert(fqdn.to_string(), [value; S]);
            },
            Some(mut d) => {
                let arr = d.value_mut();
                let min_pos = key.as_() + Value::Min as usize;
                arr[min_pos] = f64::min(arr[min_pos], value);
                let max_pos = key.as_() + Value::Max as usize;
                arr[max_pos] = f64::max(arr[max_pos], value);
                let avg_pos = key.as_() + Value::Avg as usize;
                arr[avg_pos] = arr[avg_pos] * 0.9 + value * 0.1;
                arr[key.as_() + Value::Latest as usize] = value;
            },
        };
    }

    fn get(&self, fqdn: &str, key: T, value: Value) -> f64 {
        match self.data.get(fqdn) {
            None => 0.0,
            Some(d) => d.value()[key.as_() + value as usize],
        }
    }
}

impl<const S: usize> CharMapRW<S> {
    pub fn boxed<T: Max + num_traits::AsPrimitive<usize>>() -> Arc<dyn CharMap<T>> {
        Arc::new(Self { data: DashMap::new() })
    }
}

#[cfg(test)]
mod char_map_tests {
    use super::*;

    #[test]
    fn compile_test() {
        let _cmap: Arc<dyn CharMap<Chars>> = CharMapRW::<{ Chars::MAX }>::boxed();
    }

    #[test]
    fn missing_get_returns_zero() {
        let cmap = CharMapRW::<{ Chars::MAX }>::boxed();
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 0.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 0.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 0.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 0.0);
    }

    #[test]
    fn first_sets_all() {
        let cmap = CharMapRW::<{ Chars::MAX }>::boxed();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 1.0);
    }

    #[test]
    fn get_helpers_work() {
        let cmap = CharMapRW::<{ Chars::MAX }>::boxed();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        cmap.update("f1", Chars::CpuExecTime, 2.0);
        cmap.update("f1", Chars::CpuExecTime, 5.0);
        cmap.update("f1", Chars::CpuExecTime, 4.0);
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Min),
            cmap.get_min("f1", Chars::CpuExecTime)
        );
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Max),
            cmap.get_max("f1", Chars::CpuExecTime)
        );
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Avg),
            cmap.get_avg("f1", Chars::CpuExecTime)
        );
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Latest),
            cmap.get_latest("f1", Chars::CpuExecTime)
        );
    }

    #[test]
    fn second_updates() {
        let cmap = CharMapRW::<{ Chars::MAX }>::boxed();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        cmap.update("f1", Chars::CpuExecTime, 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0 * 0.9 + 2.0 * 0.1);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 2.0);
    }

    #[test]
    fn read_only_cannot_update() {
        let cmap = CharMapRW::<{ Chars::MAX }>::boxed();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        cmap.update("f1", Chars::CpuExecTime, 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0 * 0.9 + 2.0 * 0.1);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 2.0);

        let ro_cmap = cmap.read_only(cmap.clone());
        ro_cmap.update("f1", Chars::CpuExecTime, 3.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0 * 0.9 + 2.0 * 0.1);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 2.0);

        cmap.update("f1", Chars::CpuExecTime, 3.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 3.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.29);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 3.0);
    }
}
