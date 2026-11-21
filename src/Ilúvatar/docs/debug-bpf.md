# Debugging sched_ext BPF load failures

Use this guide when FineSched starts but `/sys/kernel/sched_ext/state` remains
`disabled`, the worker does not become ready, or the worker log only reports
`Failed to load BPF program` / `Permission denied (os error 13)`.

## Enable libbpf and verifier diagnostics

Set `fineloadbalancing.bpf_verbose` to `1` in the worker configuration:

```json
"fineloadbalancing": {
  "bpf_verbose": 1
}
```


## Find the verifier output

The worker's JSON log (usually `worker1.log`) records the load failure and the
panic, but may not contain libbpf's verifier output. The Ansible orchestration
log records an async job ID and its result file, for example:

```text
"ansible_job_id": "j752884155544.123819",
"results_file": "/root/.ansible_async/<ansible-job-id>"
```

Read the captured stdout from that result file:

```bash
sudo jq -r '.stdout' /root/.ansible_async/<ansible-job-id>
```

Search the output for `PROG LOAD LOG`, `BPF program load failed`, or the
verifier's first rejection message. A worker readiness failure such as
`Connection refused` is downstream: the scheduler load failure can panic its
loader thread before the worker opens its HTTP port.

## Example: FineSched CPU-domain verifier failure

In the 2026-11-16 01:47 run, `bpf_verbose` was `1` and the domain map was
created for CPUs `4-23`. The async output showed:

```text
prog 'finesched_select_cpu': BPF program load failed: -EACCES
R1 unbounded memory access
```

The rejected access was `cpu_to_domain_id[cpu]` in `cpu_has_domain()`. The code
checked the runtime CPU count, but the verifier could not prove that the array
index was below the fixed `MAX_CPUS` array size.  Keep an explicit,
verifier-visible bound on the exact index used for the array access (the
runtime CPU-set check is still needed to reject CPUs outside the configured
range).

After changing the index logic, rebuild and retry. A successful Rust/C build
does not prove the kernel verifier accepts the BPF program; inspect the next
load log because the verifier reports rejected programs individually.
