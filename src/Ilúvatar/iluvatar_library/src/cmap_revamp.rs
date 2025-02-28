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

/// A better characteristics map
///
/// We need to support MIN, MAX, AVG, LATEST
///
pub struct CharMap<T> {
    data: Vec<T>
}
impl<T> CharMap<T> {
    pub fn new() -> Self {
        Self { data: vec![] }
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