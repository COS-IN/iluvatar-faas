import sys, os

sys.path.append("../../../../load/run")
from run_trace import rust_build, run_sim, RunTarget, BuildTarget, run_cmd
from multiprocessing import Pool

ILU_HOME = "../../.."
build_level = BuildTarget.RELEASE
build_level = BuildTarget.DEBUG
benchmark = "../benchmark/worker_function_benchmarks.json"

# build the solution
rust_build(ILU_HOME, None, build_level)


def run_experiment(cpu_queue, cores):
    worker_log_dir = os.path.join(os.getcwd(), "temp_results", cpu_queue, str(cores))
    results_dir = os.path.join(os.getcwd(), "results", cpu_queue, str(cores))
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(worker_log_dir, exist_ok=True)

    kwargs = {
        "ilu_home": ILU_HOME,
        "build_level": build_level,
        "cores": cores,
        "memory": 10240,
        "worker_status_ms": 500,
        "worker_log_dir": worker_log_dir,
        "cpu_queue_policy": cpu_queue,
        "target": RunTarget.WORKER,
        "prewarm": 1,
        "benchmark_file": benchmark,
    }
    # run entire experiment
    run_sim("./trace/trace.csv", "./trace/metadata-trace.csv", results_dir, **kwargs)
    log_file = os.path.join(results_dir, "orchestration.log")
    run_cmd(
        [
            "python3",
            "plot_status.py",
            "-l",
            results_dir,
            "-t",
            "trace",
            "--out",
            results_dir,
        ],
        log_file,
    )


experiments = []
for cores in [0, 18, 20]:
    experiments.append(("fcfs", cores))
for cores in [18, 20]:
    experiments.append(("minheap", cores))

with Pool() as p:
    p.starmap(run_experiment, experiments)
