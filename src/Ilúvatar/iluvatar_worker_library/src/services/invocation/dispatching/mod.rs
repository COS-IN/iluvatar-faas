pub mod landlord;
pub mod popular;
pub mod queueing_dispatcher;

#[derive(Debug, Copy, Clone, serde::Deserialize)]
/// The policy by which polymorphic functions will be enqueued in the CPU/GPU/etc. queues
pub enum EnqueueingPolicy {
    /// Invocations will be placed in any relevant queue, and the first one to start first wins
    All,
    /// Use ratio of CPU/GPU
    Speedup,
    /// Always enqueue on the compute that gives shortest compute time
    ShortestExecTime,
    /// Always enqueue on CPU
    /// Assumes all functions can run on CPU, assumption may break in the future
    AlwaysCPU,
    /// Enqueue based on shortest estimated completion time
    EstCompTime,
    /// Multi-armed bandit for polymorphic functions.
    UCB1,
    MWUA,
    // /// Locality/E2E time
    HitTput,
    /// Always GPU for polymorphic functions
    AlwaysGPU,
    /// Landlord-based policy
    Landlord,
    LandlordFixed,
    LRU,
    LFU,
    TopAvg,
    Popular,
    PopularEstTimeDispatch,
    PopularQueueLenDispatch,
    LeastPopular,
}
