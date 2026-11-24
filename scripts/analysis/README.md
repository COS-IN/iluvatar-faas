# Local experiment analysis

## Invocation CSV provenance

The trace load generator (`iluvatar_load_gen trace`) writes the primary
`output-<trace-name>.csv` when it finishes. It omits
`output-full-<trace-name>.json` by default; pass `--keep-full-json` when running
the load generator if you need the full per-invocation records for the older
`load.analysis.log_parser` workflow.
The CSV is written from completed invocation results by
`src/Ilúvatar/iluvatar_load_gen/src/trace/trace_utils.rs::save_controller_results`;
it is not reconstructed from the worker log. The CSV contains per-invocation
success, function, cold/warm status, worker duration, function-code duration,
end-to-end duration, and invocation ID (`tid`).
`tracecmp.py` uses this CSV when the optional full JSON is absent, pairing rows
in trace submission order and adding worker-log context when available.

In this CSV, `worker_duration_us` is the worker-to-container invocation-call
duration. It does not include the entire worker request lifecycle, including
dispatcher wait or cold container creation. `analyze_results.py` uses worker
events keyed by `tid` to summarize full worker E2E (`Invocation complete`),
dispatcher queue wait (`Item starting to execute` insert/remove times), and
container startup where those events are available. Missing event data is
reported with a zero sample count, not interpreted as zero latency.

The Python runner then enriches the CSV from worker JSON logs:
`src/load/run/local.py::enrich_output_csv_with_finesched_logs` joins FineSched
assignment and sampled-metric fields onto invocation rows by `tid`. The runner
generates `summary.csv` and `failed_invocations.csv` from that CSV. Analysis
tools may read both the invocation CSV and worker logs, but the worker log is
not the source of the primary invocation rows. `load_gen.log` captures the
load generator's console output and is separate from the worker JSON logs.

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

`analyze_bursty_results.py` compares a non-bursty hzga-16 baseline with the full
bursty trace and its centered burst window. It reports per-function E2E and
execution means, and for FineSched it includes burst-window domain assignments,
coassigned functions, and correlations with sampled assignment-time loads:

```bash
python3 scripts/analysis/analyze_bursty_results.py \
  --baseline-cfs /path/to/hzga-16-cfs \
  --baseline-finesched /path/to/hzga-16-domains-4 \
  --bursty-cfs /path/to/bursty-cfs \
  --bursty-finesched /path/to/bursty-domains-4 \
  --output-dir /path/to/bursty-analysis
```

The burst duration defaults to 60 seconds and can be changed with
`--burst-seconds`. The script writes Markdown, CSV, and JSON summaries.

The profiler must cover the full invocation interval. The comparator selects
only complete one-second perf/turbostat intervals between the first worker
`Handling invocation request` and final `Invocation complete`, avoiding
startup, shutdown, and partial boundary samples.

## FineSched domain metrics

`extract_load_latency.py` builds a row-per-invocation feature table from the
load-generator CSV, worker JSON log, perf interval CSV, and turbostat output:

```bash
python3 scripts/analysis/extract_load_latency.py \
  /path/to/result-4cpu /path/to/result-8cpu /path/to/result-12cpu \
  --output /path/to/features-per-invocation.csv
```

It retains invocation identity/status and assignment-time context, and adds one
target column, `target_function_execution_ms`, computed from
`code_duration_sec * 1000`. This is function-code execution latency, not client
E2E or full worker-service time. E2E, worker duration, queue wait, CPU time,
and other post-assignment durations are deliberately excluded from the
predictor table to avoid target leakage. It also adds sampled FineSched metrics
and WC counters joined by exact `tid`. WC totals are kept;
per-CPU interval deltas, rates, miss/success ratios, and enqueue-histogram
bucket deltas are derived from consecutive samples on the same CPU. The first
sample and counter resets have no delta. The worker's effective CPU-domain map
is used to aggregate perf events and turbostat activity by domain as well as
system-wide.

Perf values are assigned from the one-second interval containing the worker
assignment timestamp. Turbostat values use the one-second block aligned from
`profiling-start.txt`, following `cpu_sched.py`'s timing convention. Both
joins are time-aligned estimates, not per-invocation measurements; their sample
age is included. Missing profiler files or unmatched samples stay empty. Keep
`wc_metrics_enabled=true` and a nonzero `core_metrics_debug_every` in the
worker config to collect WC/domain samples; `core_metrics_debug_every=1`
samples each assignment but increases log volume.

`analyze_feature_dataset.py` ranks numeric/categorical features against the
execution target using pairwise Pearson and Spearman correlations, univariate
OLS fits, and Benjamini-Hochberg adjusted p-values. It also fits a multivariate
Ridge regression using top training-set correlations and evaluates it on a
chronological 80/20 holdout. It reports all-successful and warm-only cohorts by
default. It requires NumPy and SciPy. Because perf/turbostat values repeat
across invocations within their one-second windows, pairwise p-values assume
independence that the data does not have; treat them as descriptive. The report
includes distinct feature-value counts to show this repeated-sample structure.

```bash
python3 scripts/analysis/analyze_feature_dataset.py \
  /path/to/features-per-invocation.csv
```

Outputs include `feature_correlations.csv`, `ridge_coefficients.csv`,
`model_summary.json`, and `analysis.md` under a sibling `*-regression-analysis/`
directory. The script requires `target_function_execution_ms`; it does not
accept E2E latency as a regression target.

`summarize_feature_distributions.py` reports per-column missingness, unique
counts, numeric percentiles/spread, and categorical frequency tables:

```bash
python3 scripts/analysis/summarize_feature_distributions.py \
  /path/to/features-per-invocation.csv
```

It writes `feature_distributions.csv`, `categorical_frequencies.csv`, and
`distribution_summary.md` under a sibling `*-distribution/` directory.

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

## HZGA trace comparison plots

`plot_hzga_trace_comparison.py` compares CFS and FineSched with 1, 2, 4, and 6
domains across the hzga-8, hzga-16, and hzga-32 experiment runs. It plots
weighted warm end-to-end latency, direct worker queue wait, function execution,
system CPU busy time, package energy per success, and package EDP. Latency
plots use linear axes. The script also writes a CSV and JSON summary.

```bash
python3 scripts/analysis/plot_hzga_trace_comparison.py \
  --output-dir /path/to/trace-comparison
```

The default input roots point to the completed runs under
`/home/prateeks/code/results/`. Use `--results-8-16` or `--results-32` to
select different result roots. Plotting requires Matplotlib.
