# Estimation Paths for CPU/GPU E2E Time

This document summarizes how end-to-end (e2e) execution time is estimated in the worker stack, and how invoker APIs connect to CPU/GPU queue implementations.

## 1) Top-Level API: `Invoker::est_e2e_time`

`est_e2e_time` is defined on the `Invoker` trait and implemented by `QueueingDispatcher`.
It delegates to the selected device queue's `est_completion_time(...).0`.

So the real estimation logic lives in device queue implementations.

Relevant code:
- `iluvatar_worker_library/src/services/invocation/mod.rs`
- `iluvatar_worker_library/src/services/invocation/dispatching/queueing_dispatcher.rs`

## 2) CPU E2E Estimation (`CpuQueueingInvoker`)

CPU estimate is:

`estimated_queue_delay + estimated_runtime`

Queue delay:
- `0` if `queue_len <= available_cores`
- else: `est_queue_time / min(total_cores, queue_len)` (processor-sharing approximation)

Runtime is chosen from container state:
- Warm: `Chars::CpuWarmTime`
- Prewarm: `Chars::CpuPreWarmTime`
- Cold: `Chars::CpuColdTime`

Relevant code:
- `iluvatar_worker_library/src/services/invocation/cpu_q_invoke.rs`

## 3) GPU E2E Estimation Methods

There are multiple GPU queue backends.

### 3.1 Serial GPU Queue (`GpuQueueingInvoker`)

Estimate:

`(queue.est_queue_time / max_concurrency) + runtime_from_state`

Runtime from state uses:
- `Chars::GpuWarmTime`
- `Chars::GpuPreWarmTime`
- `Chars::GpuColdTime`

Relevant code:
- `iluvatar_worker_library/src/services/invocation/gpu_q_invoke.rs`

### 3.2 MQFQ GPU Queue (`MQFQ`)

MQFQ supports multiple queue-time estimators:
- `V1`
- `V2`
- `V3`
- `LinReg`
- `PerFuncLinReg`
- `FallbackLinReg`
- `GlobalLinReg`

Then it combines queue-time with:
- `GpuExecTime` average
- optional `QueueErrGpu`
- optional cap (`max_est_sec`)

Relevant code:
- `iluvatar_worker_library/src/services/invocation/queueing/gpu_mqfq.rs`

## 4) Where `est_queue_time` Comes From

CPU queue policies (`fcfs`, `minheap`, etc.) maintain internal aggregate `est_time`:
- increment on enqueue
- decrement on dequeue
- per-item wall time derived from cmap + container state

GPU queue policies use a similar pattern with GPU-specific batching/flow semantics.

Relevant code:
- `iluvatar_worker_library/src/services/invocation/queueing/mod.rs`
- `iluvatar_worker_library/src/services/invocation/queueing/fcfs.rs`
- `iluvatar_worker_library/src/services/invocation/queueing/fcfs_gpu.rs`

## 5) Dispatch-Time Estimation (Policy Layer)

Before enqueueing, many dispatch policies call `q.est_completion_time` for CPU/GPU:

- `EstCompTime`: choose min estimated completion time
- `ShortestExecTime`: choose by `CpuExecTime` vs `GpuExecTime` only
- `EstSpeedup`: speedup-gated, then queue-aware estimate
- `Greedy` and `Landlord`: refine GPU estimate via `Chars::EstGpu` feedback/filtering

Relevant code:
- `iluvatar_worker_library/src/services/invocation/dispatching/queueing_dispatcher.rs`
- `iluvatar_worker_library/src/services/invocation/dispatching/landlord.rs`

## 6) Feedback Loop: Actuals Update Future Estimates

On invocation completion, the worker records:
- exec/cold/warm timings
- actual e2e (`E2ECpu`/`E2EGpu`)
- residual error (`QueueErrCpu`/`QueueErrGpu`)

These updates feed subsequent estimates.

Relevant code:
- `iluvatar_worker_library/src/services/invocation/mod.rs` (`invoke_on_container_2`)

## 7) `est_invoke_time` RPC and How It Uses Estimators

Current RPC behavior:
- Running/waiting counts from `invoker.running_funcs()` and `invoker.queue_len()`
- CPU estimated wait from `invoker.est_e2e_time(...)` when registration is available
- GPU estimated wait in response map currently from queue load average

Relevant code:
- `iluvatar_worker_library/src/worker_api/iluvatar_worker.rs`
