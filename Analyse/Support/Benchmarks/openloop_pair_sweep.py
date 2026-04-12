import sys, os
import csv
import itertools
import time

# Adjust the path to include the iluvatar-faas src
ILU_HOME = "/extra/sid/Experiment_Setup/iluvatar-faas/src/Ilúvatar"
LOAD_SRC = "/extra/sid/Experiment_Setup/iluvatar-faas/src"
sys.path.append(LOAD_SRC)

from load.run.run_trace import (
    rust_build,
    run_live,
    RunTarget,
    BuildTarget,
    RunType,
    make_host_queue,
)
import subprocess

# Configuration
META_CSV = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/metadata-chosen-ecdf-docker.csv"
BENCHMARK_JSON = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/Benchmarks/all-gpu-driver-results/worker_function_benchmarks.json"
RESULTS_BASE = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/Benchmarks/openloop-sweep-results/pairs"
ANSIBLE_DIR = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/ansible"
HOST_VARS = os.path.join(ANSIBLE_DIR, "vars/host_addresses.yml")

# Host Queue for the specific remote node
HOSTS = [("d7525-10s10331", "d7525-10s10331.wisc.cloudlab.us")]
REMOTE_HOST_Q = make_host_queue(HOSTS)

# IAT Sweep (ms)
IAT_MS_LIST = [500, 1000, 1500, 2000]

def get_functions():
    functions = []
    with open(META_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['func_name']:
                functions.append(row['func_name'])
    return sorted(list(set(functions)))

def collect_hardware_info(results_base, hosts):
    """Collects lscpu and nvidia-smi info from the worker node."""
    os.makedirs(results_base, exist_ok=True)
    out_path = os.path.join(results_base, "hardware_info.txt")
    if os.path.exists(out_path):
        return

    # We use the first host in the list for info
    host_name, host_addr = hosts[0]
    print(f"--- Collecting hardware info from {host_name} ({host_addr}) ---")

    cmd = [
        "ansible", "all", "-i", f"{ANSIBLE_DIR}/environments/{host_name}/hosts.ini",
        "-e", f"@{HOST_VARS}", "-m", "shell", "-a", "lscpu && nvidia-smi"
    ]
    try:
        with open(out_path, "w") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    except Exception as e:
        print(f"Failed to collect hardware info: {e}")

def create_pair_trace(func1, func2, iat_ms, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        f.write("func_name,invoke_time_ms\n")
        # Interleave 100 invocations of each function
        all_invokes = []
        for i in range(100):
            invoke_time = (i + 1) * iat_ms
            all_invokes.append((func1, invoke_time))
            all_invokes.append((func2, invoke_time))

        # Sort by time then name
        all_invokes.sort(key=lambda x: (x[1], x[0]))

        for name, time in all_invokes:
            f.write(f"{name},{time}\n")

def run_experiment(func1, func2, iat_ms):
    pair_name = f"{func1}_and_{func2}"
    trace_dir = os.path.join(RESULTS_BASE, "traces", pair_name)
    trace_in = os.path.join(trace_dir, f"iat{iat_ms}ms.csv")
    create_pair_trace(func1, func2, iat_ms, trace_in)

    results_dir = os.path.join(RESULTS_BASE, pair_name, f"iat{iat_ms}ms")

    kwargs = {
        "ilu_home": ILU_HOME,
        "ansible_dir": ANSIBLE_DIR,
        "ansible_hosts_addrs": "@" + HOST_VARS,
        "build_level": BuildTarget.RELEASE,
        "benchmark_file": BENCHMARK_JSON,
        "worker_port": 8079,
        "worker_log_dir": "/tmp/iluvatar/logs/ansible",
        "force": False, # Set to True to re-run
        "cores": 96,
        "gpus": 1,
        "docker_avoid_pull": "true",
        "log_level": "info",
        "cpu_queue_policy": "fcfs",
        "gpu_queue_policy": "mqfq_select_out_len",
        "target": RunTarget.WORKER,
        "prewarm": 1,
        "concurrent_running_funcs": 8, # Increased for 2 functions
    }

    print(f"--- Starting Pair: {func1} & {func2} @ IAT={iat_ms}ms ---")
    try:
        run_live(
            trace_in,
            META_CSV,
            results_dir,
            REMOTE_HOST_Q,
            **kwargs,
        )
        print(f"--- Finished Pair: {func1} & {func2} @ IAT={iat_ms}ms ---")
    except Exception as e:
        print(f"--- FAILED Pair: {func1} & {func2} @ IAT={iat_ms}ms ---")
        print(e)

def main():
    functions = get_functions()
    print(f"Sweeping pairs: {len(functions) * (len(functions) - 1)} combinations")

    collect_hardware_info(RESULTS_BASE, HOSTS)

    for i, func1 in enumerate(functions):
        for j, func2 in enumerate(functions):
            if i == j:
                continue # Skip same function for now, or include if desired
            for iat in IAT_MS_LIST:
                run_experiment(func1, func2, iat)

if __name__ == "__main__":
    main()
