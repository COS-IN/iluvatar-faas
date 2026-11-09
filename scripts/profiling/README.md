# Local scheduler profiling

This directory ports the optional profiling path from v-020. The original
workflow is driven by `capture_matrices.sh`, `capture_matrices_stop.sh`, the
profile toggles in `config.sh`, and the programs and wrappers under
`bpftracing_scripts/`. Those drivers execute everything through SSH; the local
`profile.sh` replaces them with PID-tracked local processes.

All 17 active v-020 `.bt` programs used for CPU counters, SCX dispatch queues,
task execution/queue/wakeup timing, scheduler distributions, wait time, and
task-IP variance are retained unchanged under `bpftrace/`. Editor swap files,
test output, obsolete command scratchpads, and the downloaded bpftrace binary
are intentionally excluded.

```bash
./profile.sh list
./profile.sh start --output-dir /path/to/result --profiles cpufrequency,dsq-length
./profile.sh status --output-dir /path/to/result
./profile.sh stop --output-dir /path/to/result
```

`analyze_bpftrace.py` summarizes the per-second comma-separated profiles by CPU
or scheduler entity. `analyze_histogram.py` converts the final dump from the
distribution and wait-time profiles to CSV.

The scripts use the system `bpftrace`; set `BPFTRACE_BIN` to another executable.
Starting and stopping probes uses `sudo`. Probe attachment remains dependent on
running-kernel symbols. `task-ip-variance` specifically requires the optional
`scx_bpf_task_ip` kfunc.
