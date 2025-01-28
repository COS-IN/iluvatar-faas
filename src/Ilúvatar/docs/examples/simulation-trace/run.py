import sys, os

sys.path.append("../../../../load/run")
from run_trace import rust_build, run_sim, RunTarget, BuildTarget

ILU_HOME = "../../.."
CORES = 4
MEMORY = 20480
build_level = BuildTarget.RELEASE
worker_log_dir = os.path.join(os.getcwd(), "temp_results")
results_dir = os.path.join(os.getcwd(), "results")
environment = "local"
benchmark = "../benchmark/worker_function_benchmarks.json"
os.makedirs(results_dir, exist_ok=True)
os.makedirs(worker_log_dir, exist_ok=True)

# build the solution
rust_build(ILU_HOME, None, build_level)

kwargs = {
    "ilu_home": ILU_HOME,
    "build_level": build_level,
    "cores": CORES,
    "memory": MEMORY,
    "worker_status_ms": 500,
    "worker_log_dir": worker_log_dir,
    "cpu_queue_policy": "fcfs",
    "target": RunTarget.CONTROLLER,
    "controller_thread_sleep_ms": 6000,
    "controller_load_metric": "running",
    "num_workers": 2,
    "prewarm": 1,
    "benchmark_file": benchmark,
    "force": True,
    "worker_port": 5000,
}
# run entire experiment
run_sim(
    "./four-functions.csv",
    "./four-functions-metadata.csv",
    results_dir,
    **kwargs,
)
