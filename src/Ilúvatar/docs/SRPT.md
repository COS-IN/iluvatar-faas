# Container-level SRPT and remaining-time accounting

This document describes the container-level EEVDF/SRPT work in the existing
`iluvatar_finesched` scheduler, how the worker supplies duration estimates, and
how to inspect the state while an invocation runs.

## Goal and accounting units

The worker's function-duration estimate (`CpuWarmTime`, falling back to
`CpuExecTime`) is a **wallclock duration**. A container may run several
application threads at once. Summing their `delta_exec` values gives aggregate
CPU core-time, which can grow roughly `k` times faster than wall time for a
`k`-way parallel invocation.

The scheduler therefore tracks two separate quantities:

- `ewma_wall_ns - elapsed_wall_ns` estimates wallclock time remaining.
- `cpu_used_ns` and shared `vtime` accumulate CPU time across the container's
  threads for accounting and scheduling.

Aggregate CPU time must never be subtracted directly from `ewma_wall_ns`. That
would make an 8-thread function appear to have exhausted its wallclock estimate
after about one eighth of its run.

The design relies on the worker invariant that one container has at most one
active invocation. That invocation may create multiple threads lazily.

## Data and control flow

### BPF state

`iluvatar_finesched/src/bpf/finesched.bpf.c` defines the
`BPF_MAP_TYPE_CGRP_STORAGE` map `cgrp_ctx_map`. Each container's cgroup has one
shared context containing:

- `vtime`: shared virtual runtime, incremented by aggregate CPU core-time.
- `inv_start_ns`, `ewma_wall_ns`: inputs for the wallclock remaining estimate.
- `cpu_used_ns`: aggregate CPU time for the current invocation.
- `ema_par_q8`: learned CPU-time/wall-time ratio in Q8; 256 means 1x.
- `nr_runnable`, `last_quiescent_ns`: runnable and idle tracking.
- `invocation_id`, `pending_invocation_id`: current and worker-supplied
  invocation generations.
- A BPF spin lock for updates shared by threads on different CPUs.

The task context also records each task's `last_sum_exec`, domain, and whether
it is counted as runnable in the cgroup context.

### Worker estimate updates

At container assignment, `iluvatar_worker_library` reads the current function
duration estimate, converts seconds to nanoseconds, and calls
`iluvatar_finesched::update_cgroup_wall_estimate`. The worker opens the
container's cgroup directory, uses its FD to look up or create cgroup storage,
and writes the estimate plus the pending invocation generation. The scheduler
pins the map at startup so the worker can update it while FineSched is running.

The worker refreshes the estimate at assignment, so a newer function EWMA is
picked up by the next assignment/reuse. It does not currently push an EWMA
change asynchronously into every already-associated container. The userspace
helper reads and rewrites the whole map value to preserve BPF-owned fields;
this assumes the update occurs at the invocation boundary, before the new
invocation starts mutating the context.

The map path currently used by the code is
`/sys/fs/bpf/scx1/cgrp_ctx_map`; the map is loaded and managed by the existing
`iluvatar_finesched` scheduler.

### Invocation and scheduling updates

The BPF callbacks maintain shared state:

1. `enqueue` counts a task as runnable in its container.
2. `running` removes that task from the runnable count. When it observes a new
   pending invocation generation, it closes the previous sample, updates the
   parallelism EMA from aggregate CPU time divided by wall time, and starts a
   fresh sample.
3. `stopping` adds the task's `sum_exec_runtime` delta to `cpu_used_ns` and
   shared `vtime`. It does not subtract that delta from the wallclock estimate.
4. `quiescent` removes a sleeping task from the runnable count and records the
   time when the container becomes idle.

For non-FIFO domain-queue insertion, FineSched uses the cgroup context when it
is available and retains the existing task-based path as a fallback. The
integer bounded weight is derived from estimated remaining core-time, with the
request capped at 20 ms. The virtual deadline uses
`v_d = v_e + r_i / w_f`. A single-threaded estimate at or below 5 ms receives
`SCX_SLICE_INF`; other work starts with a 2 ms slice, subject to the existing
busypolling adjustment. The 5 ms `MAX_LAG_NS` bound limits how far behind the
global virtual-time reference an enqueue can be.

The constants and ABI are in `iluvatar_finesched/src/bpf/intf.h`; cgroup
accounting and enqueue calculations are in `iluvatar_finesched/src/bpf/utils.bpf.c`.

## Inspecting an invocation

The map stores state; it does not print it. The current code has a worker-side
map **update** function, but does not yet have a read-and-print watch command.
For a quick one-off lookup, open the target cgroup in the same root shell that
runs `bpftool`. `BPF_MAP_TYPE_CGRP_STORAGE` uses the cgroup FD as its lookup key
([kernel map documentation](https://docs.kernel.org/6.10/bpf/map_cgrp_storage.html)):

```bash
sudo bash
CGROUP_PATH=/sys/fs/cgroup/scx.slice/docker-REPLACE_ME.scope
exec 3<"$CGROUP_PATH"
while sleep 0.2; do
  bpftool -p map lookup pinned /sys/fs/bpf/scx1/cgrp_ctx_map key hex 03 00 00 00
done
```

Replace `REPLACE_ME` with the container ID from its cgroup scope name.

Here FD 3 is inherited by `bpftool`, so the four key bytes identify that open
cgroup. Run the lookup repeatedly during an invocation to observe the values.
Use the container cgroup containing the task being probed; the context is shared
by all tasks in that cgroup, not stored as a per-task remaining-time value.

Interpret the fields as follows:

- `ewma_wall_ns`: expected wallclock duration supplied by the worker.
- `inv_start_ns`: monotonic timestamp when the current invocation first ran.
- `remaining_wall_ns = max(0, ewma_wall_ns - (monotonic_now_ns - inv_start_ns))`.
- `cpu_used_ns`: aggregate core-time; for a parallel workload it should grow
  faster than wall time.
- `ema_par_q8 / 256`: learned parallelism. It is updated when the next
  invocation generation starts, so it may not change during the current run.
- `nr_runnable`: approximate count of runnable threads in the cgroup.
- `vtime`: shared core-time virtual runtime; it persists across invocations.
- `invocation_id` and `pending_invocation_id`: show whether BPF has consumed the
  generation most recently supplied by the worker.

The current map does not store the latest calculated request, weight, deadline,
or slice. Those choices need temporary sampled BPF logging at enqueue (or extra
snapshot fields) if they need to be inspected directly.

`bpf_ktime_get_ns()` uses monotonic time; use `CLOCK_MONOTONIC` when deriving
elapsed time in userspace ([kernel timekeeping documentation](https://docs.kernel.org/core-api/timekeeping.html)).
An absent or zero `inv_start_ns` means the invocation has not started in BPF
yet. A zero `nr_runnable` alone does not prove completion: the invocation may
have all its threads blocked or sleeping.

For a more usable probe, add a read-only Rust watch helper beside
`update_cgroup_wall_estimate` that looks up this context every 100–250 ms and
prints one line with timestamp, invocation generation, elapsed and remaining
wall time, CPU time, learned parallelism, runnable count, and vtime. Reading the
map from userspace avoids emitting a BPF log line on every scheduling callback.

## Approximate correctness checks

Start with controlled workloads and compare the map values with measured wall
and CPU time:

| Aggregate CPU time | Wall time | Expected observed parallelism |
|---:|---:|---:|
| 100 ms | 100 ms | about 1x |
| 400 ms | 100 ms | about 4x |
| 800 ms | 100 ms | about 8x |

Then check the full lifecycle:

1. **Unit check:** for an 8-thread workload, remaining wall time must not reach
   zero after roughly one eighth of the expected wall duration. `cpu_used_ns`
   can increase about eight times faster; `remaining_wall_ns` must decrease
   according to elapsed wall time.
2. **Generation check:** run two invocations in a warm container. Confirm the
   generation advances, `cpu_used_ns` restarts for the new invocation, and
   `vtime` continues across invocations.
3. **Yield check:** block briefly inside an invocation. The generation should
   remain unchanged, and the wallclock estimate should continue to reflect
   elapsed wall time.
4. **Lazy parallelism check:** begin with one thread and create more during the
   invocation. Confirm their CPU time contributes to the same cgroup context
   and `ema_par_q8` moves toward the measured CPU/wall ratio on the following
   generation.
5. **Runnable check:** after all container threads block or exit,
   `nr_runnable` should return to zero. Treat it as a coarse signal because
   callback transitions and thread behavior affect the count.
6. **Scheduling check:** temporarily log the calculated request, weight,
   deadline, and slice at enqueue, then compare short and long function latency
   under load against CFS or the previous FineSched policy. Confirm that short
   single-threaded work gets the short-work slice treatment and longer or
   parallel work gets the nominal default slice (including the
   busypolling-factor adjustment).
7. **Failure cases:** exercise unknown duration (`ewma_wall_ns = 0`), an absent
   pinned map, a disappearing cgroup, and repeated warm-container assignment.
   Confirm the worker logs update failures and the scheduler continues safely.

These checks are approximate; they do not replace verifier, concurrency, or
end-to-end scheduler validation. The implementation has not yet been built or
loaded with the BPF verifier. For load failures, follow
[`debug-bpf.md`](./debug-bpf.md).
