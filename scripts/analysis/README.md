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

## CPU scheduler run analysis

`cpu_sched.py` consolidates the run analysis used for scheduler experiments. It
parses:

- first request, final completion, makespan, and final-request drain time;
- success, cold/warm counts, and per-function warm latency and execution time;
- worker warnings/errors and FineSched function-to-domain assignments;
- interval-mode perf totals, rates, ratios, and multiplexing percentages;
- turbostat power, RAPL-derived energy, frequency, C-state residency, IRQs,
  LLC activity, and per-domain busy-core/frequency statistics.

With no result arguments, it compares the newest matching `*-cfs` and
`*-finesched` directories under `/home/prateeks/code/results`:

```bash
scripts/analysis/cpu_sched.py
```

Pass one result directory to analyze it on its own:

```bash
scripts/analysis/cpu_sched.py /path/to/run
```

Pass two result directories to compare them:

```bash
scripts/analysis/cpu_sched.py \
  /path/to/run-cfs /path/to/run-finesched
```

Use `--output-dir` to save the readable report and parsed measurements. A
single-run analysis writes `analysis.md` and `analysis.json`; a comparison
writes `comparison.md` and `comparison.json`.

```bash
scripts/analysis/cpu_sched.py /path/to/run \
  --output-dir /path/to/comparison
```

The profiler must cover the full invocation interval. The comparator selects
only complete one-second perf/turbostat intervals between the first worker
`Handling invocation request` and final `Invocation complete`, avoiding
startup, shutdown, and partial boundary samples.

## FineSched domain metrics

`analyze_domain_metrics.py` extracts sampled per-domain ProcStat utilization,
PELT-like utilization, power, and assignment-time `used_cores` from a worker
JSON log. When an `output-*.csv` invocation file is beside the worker log, it
also joins each invocation to its exact sampled metrics by invocation ID and
plots e2e latency, function `exec_latency` (the CSV's `code_duration_sec`),
and worker duration. It writes per-domain metric correlations and latency
correlations (Pearson and Spearman), with separate all, warm, cold, and
per-function warm cohorts. The latency/utilization correlations use only
invocations with an exact utilization sample; no stale values are carried
forward. Time-series figures use one subplot per domain and connect samples
with lines. The latency/utilization scatter panels and exploratory models are
separated by function. Each log-linear model fits
`ln(latency_ms) = intercept + slope * utilization_percent` for warm successful
invocations; coefficients and R² are written to
`latency_utilization_models.csv`. The heatmap averages samples into five-second
bins by default; empty cells indicate no sample in that bin.

```bash
python3 scripts/analysis/analyze_domain_metrics.py \
  /path/to/result/worker1.log
```

Outputs are written to `<result>/domain-metrics-analysis/`. Use
`--invocations-csv` if the invocation CSV is elsewhere, `--bin-seconds` to
change the heatmap bin width, or `--output-dir` to choose a different
destination. Plotting requires Matplotlib and NumPy.

`compare_chbl_domain_runs.py` compares warm per-function e2e and function-code
latency percentiles across domain-count runs. It uses nearest-rank p50/p95/p99
and reports p95 change relative to the four-domain run. When each run also has
`profile-analysis/analysis.json`, it adds package/core energy and selected
system-wide perf and CPU activity counters to the report and plots:

```bash
python3 scripts/analysis/compare_chbl_domain_runs.py \
  --run 1=/path/to/chbl1 \
  --run 2=/path/to/chbl2 \
  --run 4=/path/to/chbl4 \
  --run 6=/path/to/chbl6 \
  --output-dir /path/to/comparison
```
