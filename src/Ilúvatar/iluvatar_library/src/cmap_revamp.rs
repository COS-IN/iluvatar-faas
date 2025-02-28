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

pub enum Value {
    Min = 0,
    Max,
    Avg,
    Latest
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
pub struct CharMap<T> {
    data: Vec<T>
}
impl<T> CharMap<T> {
    pub fn new() -> Self {
        Self { data: vec![] }
    }

    // Possible interface where internally we track per-compute
    pub fn get_value(fqdn: &str, key: T, entry: Value) -> f64 {
        0.0
    }

    // Other interface where caller has enum entry per-compute
    pub fn get(fqdn: &str, key: T) -> f64 {
        0.0
    }
}


#[cfg(test)]
mod char_tests {
    use super::*;

    #[test]
    fn compile_test() {
        let _cmap = CharMap::<Chars>::new();
    }
}