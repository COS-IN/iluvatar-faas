# Local SEBS load generation

This directory ports the SEBS load-generation path from
`v-020:/data2/ar/finescheduling/cpusched_experiments` to the local Iluvatar
checkout. It removes the remote SSH wrappers, `/data2` paths, and the fixed
v-021 worker address.

## v-020 source inventory

The SEBS workflow on v-020 consists of `load_gen.sh`, `benchmark.sh`,
`benchmark_exec_all.sh`, `trace_load_gen_wrapper.sh`,
`trace_exec_from_config.sh`, and `setup_sebs.sh`, plus
`benchmarking_files/generate_sebs_meta.py`,
`benchmarking_files/sebs_meta.csv`, and the generated trace directories under
`traces/`. The setup script delegates to SEBS helpers under
`src/load/functions/python3/standalone_testing/` in the v-020 Iluvatar source.

This port includes the active `traces/sebs_180_5.0` request stream (8,313
invocations). The other v-020 trace directories are alternate generated
workloads rather than additional load-generator implementations.

## Ported workflow

- `run_benchmark.sh` replaces `load_gen.sh` plus `benchmark.sh`.
- `run_trace.sh` replaces `trace_load_gen_wrapper.sh` and
  `trace_exec_from_config.sh`.
- `setup_services.sh` replaces `setup_sebs.sh` and the standalone MinIO,
  UDP-echo, and TCP-reply setup scripts.
- `generate_metadata.py` replaces
  `benchmarking_files/generate_sebs_meta.py` without requiring Fire or IPython.
- `metadata/sebs_meta.csv` and `traces/sebs_180_5.0/` are copied from v-020,
  with input paths rewritten to this directory.

## Usage

Build Iluvatar and start a local worker on port 8079, then run:

```bash
scripts/sebs/setup_services.sh start
scripts/sebs/run_benchmark.sh --cold-iters 1 --warm-iters 10
```

Use the generated benchmark JSON for trace mode:

```bash
scripts/sebs/run_trace.sh \
  --function-data scripts/sebs/results/benchmark-<timestamp>/worker_function_benchmarks.json
```

Set `HOST`, `PORT`, `BUILD`, `LOAD_GEN_BIN`, or `RESULTS_ROOT` to override local
defaults. `run_trace.sh` defaults to no prewarming because v-020's final value
of 100 prewarms is too large for a workstation; pass `--prewarms` explicitly.

## Benchmark data

The graph and simple microbenchmarks need no object-store input. Thumbnail,
video, compression, image-recognition, and DNA functions require the 154 MB
`benchmarks-data` tree used on v-020. Copy or download the SEBS source tree and
run:

```bash
SEBS_SOURCE_ROOT=/path/to/sebs-functions scripts/sebs/setup_services.sh restart
```

The Python package `minio` and the `socat` executable are required to populate
storage and run the network microbenchmarks, respectively. Stop auxiliary
services with `scripts/sebs/setup_services.sh stop`.
