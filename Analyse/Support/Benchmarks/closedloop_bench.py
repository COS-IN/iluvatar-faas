import sys, os
import subprocess
import json
import csv
import time

# Adjust the path to include the iluvatar-faas src
ILU_HOME = "/extra/sid/Experiment_Setup/iluvatar-faas/src/Ilúvatar"
LOAD_SRC = "/extra/sid/Experiment_Setup/iluvatar-faas/src"
sys.path.append(LOAD_SRC)

from load.run.run_trace import (
    RunTarget,
    BuildTarget,
    make_host_queue,
)

# Specific remote node
REMOTE_HOST_NAME = "d7525-10s10327"
# REMOTE_HOST_NAME = "v-gpu1"
REMOTE_HOST_ADDR = "d7525-10s10327.wisc.cloudlab.us"
# REMOTE_HOST_ADDR = "v-gpu1.victor.futuresystems.org"
REMOTE_HOST_Q = make_host_queue([(REMOTE_HOST_NAME, REMOTE_HOST_ADDR)])

# Configuration
ANSIBLE_DIR = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/ansible"
HOST_VARS = os.path.join(ANSIBLE_DIR, "vars/host_addresses.yml")
RESULTS_DIR = f"/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/Benchmarks/closedloop-results/{REMOTE_HOST_NAME}"
METADATA_CSV = "/extra/sid/Experiment_Setup/iluvatar-faas/Analyse/Support/metadata-chosen-ecdf-docker.csv"


def collect_hardware_info(results_base, host_name, host_addr):
    """Collects lscpu and nvidia-smi info from the worker node."""
    os.makedirs(results_base, exist_ok=True)
    out_path = os.path.join(results_base, "hardware_info.txt")
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

def prepare_benchmark_metadata(source_csv, output_csv):
    """Transforms the open-loop metadata CSV into the format expected by the benchmark subcommand."""
    print(f"--- Transforming metadata for benchmark: {source_csv} -> {output_csv} ---")
    with open(source_csv, 'r') as f_in, open(output_csv, 'w', newline='') as f_out:
        reader = csv.DictReader(f_in)
        # Target header matches ToBenchmarkFunction struct
        fieldnames = ['name', 'image_name', 'compute', 'isolation', 'memory', 'args', 'server']
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow({
                'name': row['func_name'],
                'image_name': row['image_name'],
                'compute': row['compute'],
                'isolation': row['isolation'].upper(), # Ensure DOCKER etc.
                'memory': row['mem_mb'],
                'args': row['args'],
                'server': 'HTTP' # Default for current images
            })

def run_closed_loop_bench():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    log_file = os.path.join(RESULTS_DIR, "orchestration.log")
    
    # 1. Collect Hardware Info
    collect_hardware_info(RESULTS_DIR, REMOTE_HOST_NAME, REMOTE_HOST_ADDR)
    
    # 2. Prepare Metadata
    temp_metadata = os.path.join(RESULTS_DIR, "closedloop_metadata.csv")
    prepare_benchmark_metadata(METADATA_CSV, temp_metadata)
    
    # 3. Worker Environment Configuration (Matching benchmark.sh)
    worker_env = {
        "ILUVATAR_WORKER__container_resources__memory_mb": "102400",
        "ILUVATAR_WORKER__container_resources__memory_buffer_mb": "4000",
        "ILUVATAR_WORKER__container_resources__cpu_resource__count": "96",
        "ILUVATAR_WORKER__container_resources__gpu_resource__count": "1",
        "ILUVATAR_WORKER__status__report_freq_ms": "1000",
        "ILUVATAR_WORKER__invocation__queue_policies__CPU": "fcfs",
        "ILUVATAR_WORKER__invocation__queue_policies__GPU": "fcfs",
        "ILUVATAR_WORKER__energy__rapl_freq_ms": "0",
        "ILUVATAR_WORKER__energy__ipmi_freq_ms": "0",
        "ILUVATAR_WORKER__logging__level": "info",
        "ILUVATAR_WORKER__logging__directory": "/tmp/iluvatar/logs/ansible"
    }
    worker_env_json = json.dumps({"worker_environment": worker_env})
    
    common_args = [
        "-i", f"{ANSIBLE_DIR}/environments/{REMOTE_HOST_NAME}/hosts.ini",
        os.path.join(ILU_HOME, "ansible/worker.yml"),
        "-e", f"@{HOST_VARS}",
        "-e", "target=release",
        "-e", "worker_log_dir=/tmp/iluvatar/logs/ansible"
    ]

    print(f"--- Cleaning and Deploying Worker on {REMOTE_HOST_ADDR} ---")
    with open(log_file, "a") as f:
        # Clean
        subprocess.run(["ansible-playbook"] + common_args + ["-e", "mode=clean"], stdout=f, stderr=subprocess.STDOUT)
        # Deploy
        subprocess.run(["ansible-playbook"] + common_args + ["-e", "mode=deploy", "-e", "influx_enabled=false", "-e", worker_env_json], stdout=f, stderr=subprocess.STDOUT)

    # 3. Run iluvatar_load_gen benchmark
    load_gen_bin = os.path.join(ILU_HOME, "target/x86_64-unknown-linux-gnu/release/iluvatar_load_gen")
    bench_cmd = [
        load_gen_bin, "benchmark",
        "--out-folder", RESULTS_DIR,
        "--port", "8070", # Match benchmark.sh
        "--host", REMOTE_HOST_ADDR,
        "--cold-iters", "4",
        "--warm-iters", "3",
        "--target", "worker",
        "--function-file", temp_metadata
    ]
    
    print(f"--- Starting Closed-Loop Benchmark ---")
    with open(log_file, "a") as f:
        subprocess.run(bench_cmd, stdout=f, stderr=subprocess.STDOUT)
    
    # 5. Format JSON outputs for readability
    print("--- Formatting JSON results ---")
    for filename in os.listdir(RESULTS_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(RESULTS_DIR, filename)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                with open(filepath, 'w') as f:
                    json.dump(data, f, indent=4)
            except Exception as e:
                print(f"Failed to format {filename}: {e}")

    print(f"--- Closed-Loop Benchmark Finished. Results in {RESULTS_DIR} ---")

if __name__ == "__main__":
    run_closed_loop_bench()
