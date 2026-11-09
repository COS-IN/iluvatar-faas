# Local experiment analysis

This directory ports the active single-experiment analysis path from
`v-020:/data2/ar/finescheduling/cpusched_experiments`. The original entry point
is `process_results.sh`; it filters `worker_worker1.log`, summarizes invocation
CSV files, computes load-generator timing and makespan, and optionally processes
bpftrace logs. Its remote configuration and plotting dependencies have been
removed here.

Run the local analyzer against a trace or benchmark result directory:

```bash
scripts/analysis/process_results.sh scripts/sebs/results/trace-<timestamp> \
  --worker-log /path/to/worker.log
```

The analyzer accepts either `worker.log` or `worker_worker1.log`, and either
`output-funcs-in.csv` or `benchmark-output.csv`. It writes filtered worker events,
per-function latency statistics, failed invocations, CPU status statistics,
load-generator timing, and `summary.json` under `<result>/analysis/`. It uses
only the Python standard library.

Scheduler profiling is in `../profiling/`. Start it before the load generator,
stop it afterward, then summarize time-series profiles:

```bash
scripts/profiling/profile.sh list
scripts/profiling/profile.sh start --output-dir /path/to/result \
  --profiles cpufrequency,dsq-length,task-cpu,task-queue
scripts/profiling/profile.sh stop --output-dir /path/to/result
scripts/profiling/analyze_bpftrace.py /path/to/result
```

The v-020 single-result dependencies represented here are
`postprocess/single_exp/load_gen_timing.py`, `makespan.py`, `avg_cpu_v2.py`,
`split_funcs_csv.py`, and `funcs_csv_per_fqdn_summary.py`. Plot-only scripts and
multi-experiment comparison scripts are not part of worker-log parsing and are
not copied into this local execution path.

Profiling requires root privileges and bpftrace. SCX kfunc-based profiles depend
on the corresponding symbols in the running kernel. In particular,
`task-ip-variance` requires `scx_bpf_task_ip`; it is optional when task-IP
monitoring is disabled.

## Paired CFS/FineSched comparison

`compare_cfs_finesched.py` consolidates the ad-hoc pair-analysis scripts used
for the four-function experiments. It parses:

- first request, final completion, makespan, and final-request drain time;
- success, cold/warm counts, and per-function warm latency and execution time;
- worker warnings/errors and FineSched function-to-domain assignments;
- interval-mode perf totals, rates, ratios, and multiplexing percentages;
- turbostat power, RAPL-derived energy, frequency, C-state residency, IRQs,
  LLC activity, and per-domain busy-core/frequency statistics.

With no result arguments, it finds the newest matching `*-cfs` and
`*-finesched` directories under `/home/prateeks/code/results`:

```bash
scripts/analysis/compare_cfs_finesched.py
```

Pass a pair explicitly when needed:

```bash
scripts/analysis/compare_cfs_finesched.py \
  /path/to/run-cfs /path/to/run-finesched
```

Use `--output-dir` to save both a readable report and all parsed measurements:

```bash
scripts/analysis/compare_cfs_finesched.py \
  --output-dir /path/to/comparison
```

The profiler must cover the full invocation interval. The comparator selects
only complete one-second perf/turbostat intervals between the first worker
`Handling invocation request` and final `Invocation complete`, avoiding
startup, shutdown, and partial boundary samples.
