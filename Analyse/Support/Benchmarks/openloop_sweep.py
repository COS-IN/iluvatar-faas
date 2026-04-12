import sys, os
import csv
import multiprocessing as mp
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
RESULTS_BASE = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/Benchmarks/openloop-sweep-results"
ANSIBLE_DIR = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/ansible"
HOST_VARS = os.path.join(ANSIBLE_DIR, "vars/host_addresses.yml")

# Host Queue for the specific remote node
HOSTS = [("d7525-10s10327", "d7525-10s10327.wisc.cloudlab.us")]
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

def create_trace(func_name, iat_ms, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        f.write("func_name,invoke_time_ms\n")
        for i in range(100):
            invoke_time = (i + 1) * iat_ms
            f.write(f"{func_name},{invoke_time}\n")

def run_experiment(func_name, iat_ms):
    trace_dir = os.path.join(RESULTS_BASE, "traces", func_name)
    trace_in = os.path.join(trace_dir, f"iat{iat_ms}ms.csv")
    create_trace(func_name, iat_ms, trace_in)

    results_dir = os.path.join(RESULTS_BASE, func_name, f"iat{iat_ms}ms")

    kwargs = {
        "ilu_home": ILU_HOME,
        "ansible_dir": ANSIBLE_DIR,
        "ansible_hosts_addrs": "@" + HOST_VARS,
        "build_level": BuildTarget.RELEASE,
        "benchmark_file": BENCHMARK_JSON,
        "worker_port": 8079,
        "worker_log_dir": "/tmp/iluvatar/logs/ansible",
        "force": True,
        "cores": 96,
        "gpus": 1,
        "docker_avoid_pull": "true",
        "log_level": "info",
        "cpu_queue_policy": "fcfs",
        "gpu_queue_policy": "mqfq_select_out_len",
        "target": RunTarget.WORKER,
        "prewarm": 1,
        "concurrent_running_funcs": 4,
    }

    print(f"--- Starting: {func_name} @ IAT={iat_ms}ms ---")
    try:
        run_live(
            trace_in,
            META_CSV,
            results_dir,
            REMOTE_HOST_Q,
            **kwargs,
        )
        print(f"--- Finished: {func_name} @ IAT={iat_ms}ms ---")
    except Exception as e:
        print(f"--- FAILED: {func_name} @ IAT={iat_ms}ms ---")
        print(e)

def main():
    functions = get_functions()
    print(f"Sweeping functions: {functions}")

    collect_hardware_info(RESULTS_BASE, HOSTS)

    # Optional: Build the project first
    # rust_build(ILU_HOME, None, BuildTarget.RELEASE)

    for func in functions:
        for iat in IAT_MS_LIST:
            run_experiment(func, iat)

if __name__ == "__main__":
    main()
