import sys, os

sys.path.append("../../../../load/run")
from run_trace import rust_build, run_live, RunTarget, BuildTarget, LOCALHOST_Q

ILU_HOME = "../../.."
CORES = 2
MEMORY = 4096
build_level = BuildTarget.RELEASE_SPANS
worker_log_dir = os.path.join(os.getcwd(), "temp_results")
results_dir = os.path.join(os.getcwd(), "results")
environment = "local"
benchmark = "../benchmark/worker_function_benchmarks.json"
os.makedirs(results_dir, exist_ok=True)
os.makedirs(worker_log_dir, exist_ok=True)

# build the solution
rust_build(ILU_HOME, None, build_level)

ansible_dir = os.path.join(ILU_HOME, "ansible")
kwargs = {
    "ilu_home": ILU_HOME,
    "ansible_hosts_addrs": "@"
    + os.path.join(ansible_dir, "group_vars/local_addresses.yml"),
    "ansible_dir": ansible_dir,
    "build_level": build_level,
    "cores": CORES,
    "memory": MEMORY,
    "snapshotter": "overlayfs",
    "worker_status_ms": 500,
    "worker_log_dir": worker_log_dir,
    "cpu_queue_policy": "fcfs",
    "target": RunTarget.WORKER,
    "prewarm": 1,
    "benchmark_file": benchmark,
    "worker_spanning": "NEW+CLOSE",
}
# run entire experiment
run_target = RunTarget.WORKER
run_live("./in.csv", "./meta.csv", results_dir, LOCALHOST_Q, **kwargs)
