use dashmap::DashMap;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[repr(u32)]
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
    fn as_(self) -> usize {
        self as usize
    }
}

/// Trait ensuring that there is a maximal value that is also constant
pub trait Max {
    const MAX: usize;
}

pub enum Value {
    Min = 0,
    Max,
    Avg,
    Latest
}

pub trait CharMap<T: num_traits::AsPrimitive<usize>> {
    fn update(&self, _fqdn: &str, _key: T, _value: f64) { }

    // Other interface where caller has enum entry per-compute
    fn get(&self, _fqdn: &str, _key: T) -> f64 {
        0.0
    }

    fn register(&self, fqdn: String);
}

/// A better characteristics map
///
/// What we 'want' it to do:
///     Store these per-fqdn
///     Store only f64 values
///     Have a unique entry for all values in <T>
///     Support a unique entry for MIN, MAX, AVG, LATEST of each <T>
///         Do we want to require external separate enum values for each compute, CpuExecTime, GpuExecTime, etc.?
///         Or have the cmap internally track CPU, GPU, etc. distinction?
///     Minimal locking on add / update and retrieval
///     An optional read-only wrapper to forward cmap to a simulation
///     Easy cloning of all of an FQDN's values
///     easy cloning / serializing to send to controller for LB purposes
///     Allow updating multiple enum entries in one call
///     Allow retrieving multiple enum entries in one call
pub struct CharMapRW<const T: usize> {
    data: DashMap<String, [f64; T]>,
}
impl<T: Max + num_traits::AsPrimitive<usize>, const S: usize> CharMap<T> for CharMapRW<{ S }> {
    fn register(&self, fqdn: String) {
        self.data.insert(fqdn, [0.0; S]);
    }
}

impl<const T: usize> CharMapRW<T> {
    pub fn new() -> Self {
        Self { data: DashMap::new() }
    }

}


#[cfg(test)]
mod char_tests {
    use super::*;

    #[test]
    fn compile_test() {
        let _cmap = CharMapRW::<{ Chars::MAX }>::new();
    }

    #[test]
    fn register() {
        let cmap: Box<dyn CharMap<Chars>> = Box::new(CharMapRW::<{ Chars::MAX }>::new());
        cmap.register("f1".to_owned());
        cmap.register("f2".to_owned());
        cmap.register("f3".to_owned());
    }

    #[test]
    fn get() {
        let cmap: Box<dyn CharMap<Chars>> = Box::new(CharMapRW::<{ Chars::MAX }>::new());
        cmap.register("f1".to_owned());
        cmap.get("f1", Chars::CpuExecTime);
    }
}