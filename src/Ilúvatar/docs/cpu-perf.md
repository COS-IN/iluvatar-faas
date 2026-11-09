## Observations 
- Similar vs. CFS
- CPU-intensive functions suffer slightly (2%)
- Perf stat tracing: didnt reveal much so far
- Multiple frequency levels (big/little cores) complicate things a bit , also an opportunity
- Energy showed early promise, then regressed. similar to cfs now. 
- testing with basic consistent_hashing 
- could try splitting functions into f1 f2 etc 


## Cleaning finesched implementations 
- 48 cpu limit remove, add freq domains 
- Simulator testing!! Starvation, busy% for functions analysis (can tend towards 0 if function blocked by external IO). 
- 


### Warm invocation latency

 Function    FineSched mean     CFS mean    FineSched Δ    FineSched p95      CFS p95
━━━━━━━━━━  ━━━━━━━━━━━━━━━━  ━━━━━━━━━━━  ━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━  ━━━━━━━━━━━
 BFS               31.32 ms     29.92 ms          +4.7%         44.62 ms     39.18 ms
──────────  ────────────────  ───────────  ─────────────  ───────────────  ───────────
 gzip             183.18 ms    172.53 ms          +6.2%        236.91 ms    198.10 ms
──────────  ────────────────  ───────────  ─────────────  ───────────────  ───────────
 hello              5.86 ms      6.05 ms          −3.1%          8.19 ms      8.12 ms
──────────  ────────────────  ───────────  ─────────────  ───────────────  ───────────
 pyaes            172.08 ms    171.66 ms          +0.2%        212.26 ms    205.39 ms

All 650 invocations succeeded in both runs. FineSched’s gzip slowdown is mostly execution time itself: 176.18 ms versus 166.03 ms under CFS.

### Perf counters

Counters below use the identical 35–215 second trace interval.

 Counter              FineSched / sec    CFS / sec    FineSched vs CFS
━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━
 Cycles                        2.116B       2.891B              −26.8%
───────────────────  ─────────────────  ───────────  ──────────────────
 Instructions                  2.778B       3.889B              −28.6%
───────────────────  ─────────────────  ───────────  ──────────────────
 Cache references              111.5M       160.4M              −30.5%
───────────────────  ─────────────────  ───────────  ──────────────────
 Cache misses                   22.7M        30.2M              −24.8%
───────────────────  ─────────────────  ───────────  ──────────────────
 Branches                      489.3M       699.1M              −30.0%
───────────────────  ─────────────────  ───────────  ──────────────────
 Context switches               9,923       16,048              −38.2%
───────────────────  ─────────────────  ───────────  ──────────────────
 CPU migrations                   260          529              −50.9%
───────────────────  ─────────────────  ───────────  ──────────────────
 IPC                            1.313        1.345               −2.4%
───────────────────  ─────────────────  ───────────  ──────────────────
 Cache-miss ratio              20.36%       18.82%            +1.55 pp
───────────────────  ─────────────────  ───────────  ──────────────────
 Branch-miss ratio              2.79%        2.67%            +0.12 pp

### CPU activity and frequency

 Scheduler    Average busy cores    Busy-weighted frequency
━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━
 FineSched                 1.103                  2.338 GHz
───────────  ────────────────────  ─────────────────────────
 CFS                       1.276                  2.537 GHz

## Inventory

 Layer                        Available profiling                                                    Readiness
━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Invocation-level             E2E, worker, code duration, CPU utilization, cold/warm status,         Ready
                              transaction IDs
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Worker/FineSched logs        Domain assignment/release, queue state, invocation completion,         Ready
                              cgroup migration, errors
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Hardware counters            perf stat: cycles, instructions, IPC, cache references/misses,         Ready with sudo; verified
                              context switches, migrations
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 CPU frequency/utilization    turbostat, mpstat, pidstat, sar                                        Installed; needs run validation
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Scheduler tracing            perf sched, trace-cmd, sched tracepoints                               Installed
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 FineSched BPF state          Pinned gMap, cMap, gStats; per-domain utilization, average             Implemented, but gStats is not currently exported periodically
                              utilization, DSQ length, frequency
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Targeted BPF tracing         17 v-020 profiles for CPU counters, DSQ length, execution time,        Ported locally, but currently needs compatibility repair
                              wait time, wakeups, queue time, timeslices
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Call stacks                  perf record, existing Ansible worker_perf.yml, Rust tracing            Available
                              flamegraphs
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Energy                       RAPL, perf energy counters, process utilization, CPU frequency,        Implemented but disabled in current configs
                              IPMI
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Legacy analysis              v-020 tail latency, domain heatmaps, C-state, energy, perf-event,      Available remotely; the active single-run path is already ported
                              makespan and slowdown plots                                            locally
───────────────────────────  ─────────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────
 Poly-dispatch analysis       Latency, normalized per-function latency, queueing, running            Useful, but several scripts assume older/GPU-oriented schemas
                              averages and resource plots

Useful local entry points:

profiling README (iluvatar-faas/scripts/profiling/README.md:1)
profile lifecycle wrapper (iluvatar-faas/scripts/profiling/profile.sh:1)
experiment analyzer (iluvatar-faas/scripts/analysis/README.md:1)
Ansible perf-record worker (iluvatar-faas/src/Ilúvatar/ansible/worker_perf.yml:1)
FineSched BPF interface (iluvatar-faas/src/Ilúvatar/iluvatar_finesched/src/bpf/intf.h:91)

### Important FineSched finding

bpf_verbose: 0 only disables libbpf loader diagnostics. Inside the scheduler, debug is hard-coded to 1 (iluvatar-faas/src/Ilúvatar/iluvatar_finesched/src/bpf/intf.h:91), and info() always invokes bpf_printk.

There are many info() calls in task initialization, CPU selection, enqueueing, DSQ insertion, and exit paths. This tracing can itself impose overhead even when trace_pipe is not being saved. A FineSched build with BPF debug printing disabled should be an explicit comparison case.

### BPF-suite issues

All 17 local .bt programs currently fail parsing with the installed bpftrace 0.20.2 because their config blocks use syntax accepted by the newer v-020 setup. This should be a small mechanical repair.

After that:

DSQ-length tracing needs its fexit probe adapted to the symbol/probe name exposed by this kernel.
task-ip-variance cannot currently run: scx_bpf_task_ip is unavailable and task_ip_monitoring is disabled.
The remaining generic hardware and sched tracepoint profiles should be usable.
Running all 17 simultaneously is not advisable because the instrumentation could perturb scheduling.

## Recommended profiling plan

Establish a low-overhead baseline.

Run at least five alternating CFS/FineSched pairs, collecting:
cycles and instructions
IPC
cache references and misses
context switches
CPU migrations
task-clock
per-core utilization
effective CPU frequency and C-state residency

Use two smaller perf event groups if necessary to avoid counter multiplexing.

Isolate gzip and pyaes.

Create separate warm-only gzip and pyaes traces. Compare:
CFS on all 24 CPUs
CFS restricted to the same six-CPU home domain
FineSched on that domain

This separates domain confinement/SMT effects from SCHED_EXT overhead.

Repair and run targeted BPF profiles.

Start with only:
task CPU-time distribution
timeslice distribution
scheduler wait time
queue and wakeup delay
DSQ length

These will reveal whether FineSched adds preemption, shorter runs, scheduler delay, or hidden DSQ waiting.

Add FineSched-native state capture.

Sample the pinned gStats map once per second into the result directory:
domain utilization
averaged utilization
DSQ length
average frequency

Keep the existing domain-assignment log analysis.

Perform overhead ablations.

Compare FineSched with:
BPF printing enabled versus disabled
5 ms versus a larger timeslice
current SMT-containing groups versus physical-core-first groups
current domain confinement versus equivalent CFS cpusets

Use call-stack profiling only after counters identify the category.

If context switches rise, use perf sched/scheduler tracepoints. If cycles rise without extra instructions, inspect kernel/BPF call stacks. If IPC falls or cache misses rise, investigate SMT placement and cache locality.

## Validated power result

The profiled runs are:

- FineSched: `/home/prateeks/code/results/four-functions-finesched-bfs-perf-freq-20261109-032034`
- CFS: `/home/prateeks/code/results/four-functions-cfs-bfs-perf-freq-20261109-035439`

Both workers handled 650 requests successfully. From the first request received by the
worker to the final `Invocation complete`, FineSched took 214.418 seconds and CFS took
214.434 seconds. The 15.9 ms difference is negligible, so total energy can be compared
without a makespan correction.

The perf processes did not start at the same trace position: FineSched started about
33.4 seconds after load generation began and CFS about 10.8 seconds after it began.
Whole-file perf totals are therefore not comparable. Counter comparisons in this
document use the common 35--215 second trace interval. Hardware events were multiplexed
at roughly 50--62%, and `perf stat -a` includes the worker, containers, load generator,
kernel, and all other host activity.

Turbostat has 180 one-second samples for each scheduler over the common interval. Its
RAPL-derived package and core power readings show:

| Measurement | FineSched | CFS | CFS relative to FineSched |
|---|---:|---:|---:|
| Mean package power | 6.417 W | 8.245 W | +28.5% |
| Package energy over 180 s | 1,155.10 J | 1,484.15 J | +329.05 J |
| Mean core power | 2.118 W | 3.654 W | +72.5% |
| Core energy over 180 s | 381.28 J | 657.74 J | +276.46 J |

FineSched therefore used about 22.2% less package energy over the same trace interval.
The package-energy reduction is smaller than the core-energy reduction because package
power includes relatively fixed SoC/uncore/idle power.

## Existing C-state and power evidence

Aligned turbostat averages over the same 180 seconds are:

| Metric | FineSched | CFS |
|---|---:|---:|
| CPU busy | 4.595% | 5.315% |
| Average effective frequency | 109 MHz | 140 MHz |
| Frequency while busy | 2.338 GHz | 2.537 GHz |
| C1 residency | 0.427% | 0.715% |
| C2 residency | 46.910% | 42.485% |
| C3 residency | 48.777% | 52.188% |
| Interrupts per one-second sample | 31,120 | 32,450 |
| LLC references per second reported by turbostat | 55.9 | 78.3 |

FineSched has about one percentage point more combined C2+C3 residency, but CFS has
more C3 residency by itself. The current result is not explained simply by FineSched
entering the deepest state more often. The stronger signals are less busy time, lower
active frequency, fewer scheduler events, fewer migrations, fewer interrupts, and less
cache activity. Idle-episode duration and wakeup frequency must be measured to determine
whether FineSched creates longer uninterrupted sleep intervals.

## Perf and power analysis plan

### Stage 1: repeatable low-overhead baseline

Run at least five alternating FineSched/CFS pairs, randomizing which scheduler runs
first. Use identical traces, metadata, CPU count, warmed containers, and governor. Record
starting and ending package temperature and allow the machine to return to a defined
idle-temperature band between runs.

Start every profiler before the trace-start marker and stop it immediately after the
last invocation-complete marker. The runner should write `TRACE_START` and `TRACE_END`
to `trace_marker` so perf, turbostat, RAPL, kernel traces, and worker logs share an exact
analysis window.

Collect:

- turbostat at one-second intervals, including Busy%, Bzy_MHz, IPC, C-state residency,
  IRQ, CorWatt, and PkgWatt;
- start/end package RAPL readings, plus 100--250 ms samples for the power time series;
- system-wide perf counters split into small event groups to avoid heavy multiplexing;
- worker- or container-cgroup perf counters to distinguish workload execution from host
  background activity;
- per-function warm E2E, queue, worker, and code duration.

Report mean power, trace energy, idle-adjusted dynamic energy, energy per invocation,
energy-delay product, and latency distributions. Keep raw totals, sample duration, and
counter enabled-time percentage with every normalized rate.

### Stage 2: idle and frequency causality

This kernel exposes the required tracepoints:

- `power:cpu_idle`
- `power:cpu_idle_miss`
- `power:cpu_frequency`
- `power:cpu_frequency_limits`
- `power:pstate_sample`

For each logical CPU, physical core, and FineSched domain, derive:

- time in each idle state;
- number of entries and exits;
- idle-duration histogram and longest uninterrupted idle interval;
- unsuccessful idle-state predictions from `cpu_idle_miss`;
- frequency-residency histogram;
- frequency ramp latency after wakeup and decay after an invocation;
- simultaneous active-core count over time.

Correlate idle exits and frequency boosts with RAPL power changes. This distinguishes
energy saved through consolidation and long sleep intervals from energy saved merely by
running at lower frequency.

### Stage 3: scheduler causality

For a representative 30--60 second window, collect:

- `sched:sched_waking`, `sched:sched_wakeup`, and `sched:sched_wakeup_new`;
- `sched:sched_switch`;
- `sched:sched_migrate_task`;
- `sched:sched_stat_runtime` and `sched:sched_stat_wait`;
- `sched_ext:sched_ext_event`, `sched_ext:sched_ext_bypass_lb`, and scheduler errors.

Filter to the worker/container cgroups where possible. Full-system `sched_switch` tracing
for the complete experiment will be large and may perturb the result; prefer eBPF
histograms and counters, or a short targeted window.

Compute per task and function:

- wakeup-to-running delay;
- run-slice length and preemption count;
- runnable and blocked time;
- CPU/domain sequence;
- migration and work-steal count;
- home-domain versus non-home-domain execution;
- the wakeup responsible for each idle exit.

### Stage 4: FineSched-native instrumentation

Export compact counters from the existing pinned BPF maps rather than relying on
`bpf_printk`:

- per-domain utilization and averaged utilization;
- regular and priority DSQ lengths;
- enqueue, dispatch, local execution, and work-steal counts;
- home-domain overload and cross-domain movement counts;
- per-task CPU time, sleep frequency, and timeslice statistics;
- requested versus actual CPU and frequency target.

`bpf_verbose` controls libbpf loader output, not all scheduler logging. `info()` in the
BPF code currently calls `bpf_printk` unconditionally, while `debug` is compiled as 1.
Add an explicit runtime debug level or compile a no-print scheduler for measurement.
Keep `task_ip_monitoring` disabled during the initial experiments because it attaches a
hardware-cycle event to every CPU and can contend with or perturb PMU measurements.

### Stage 5: focused attribution and ablations

Run warm, single-function traces for gzip, pyaes, and BFS. Compare:

- unrestricted CFS;
- CFS restricted to the same FineSched home-domain cpuset;
- FineSched on that domain;
- FineSched with BPF prints disabled;
- alternate timeslices and physical-core-first domain layouts;
- the current frequency policy versus fixed or explicitly controlled frequency targets.

For package-energy attribution, combine package RAPL deltas with per-cgroup CPU runtime
and hardware counters. RAPL is package-wide and cannot directly assign joules to an
individual function while functions overlap; isolated function runs are the validation
case.

## RAPL implementation caution

Iluvatar's worker has an `EnergyLogger` controlled by `energy.rapl_freq_ms`, and Ansible's
`worker_perf.yml` can set `worker_rapl_log_freq_ms`. The current experiment configs have
RAPL disabled (`rapl_freq_ms: 0`) and span energy monitoring disabled.

The active `RaplMonitor` opens `/dev/cpu/*/msr`, reads the package-energy MSR once per
physical CPU, and sums the readings. This host has one AMD socket and 12 physical cores,
so those readings likely duplicate the same package counter and make absolute joules
incorrect. Access also requires root on this host. Before relying on `energy-rapl.log`,
change the monitor to read one package counter per socket, preferably through the
powercap `energy_uj` interface, and handle counter wraparound consistently. Until then,
turbostat's RAPL-derived package energy is the preferred source.
