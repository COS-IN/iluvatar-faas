import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from load.run.run_trace import rust_build, run_sim, RunTarget, BuildTarget

CORES = 5
MEMORY = 20480
# We have to run with all function spans enabled to capture per-worker information
build_level = BuildTarget.DEBUG_SPANS
results_dir = os.path.join(os.getcwd(), "results")
os.makedirs(results_dir, exist_ok=True)
benchmark = "../benchmark/worker_function_benchmarks.json"

# build the solution
rust_build(ILU_HOME, None, build_level)

kwargs = {
    "ilu_home": ILU_HOME,
    "build_level": build_level,
    "cores": CORES,
    "memory": MEMORY,
    "worker_status_ms": 3000,
    "worker_log_dir": results_dir,
    "controller_log_dir": results_dir,
    "cpu_queue_policy": "fcfs",
    "target": RunTarget.CONTROLLER,
    "controller_thread_sleep_ms": 2000,
    "controller_log_level": "info",
    "controller_load_metric": "running",
    "controller_bounded_ceil": 4,
    "controller_popular_pct": 0.2,
    "controller_algorithm": "CHRLU",
    "num_workers": 4,
    "prewarm": 1,
    "benchmark_file": benchmark,
    "force": False,
    # Disable stdout for simulation load gen because it will get us ALL logs to stdout, which we don't want.
    # They are still logged to files.
    "load_log_stdout": False,
}
# run entire experiment
input_csv = "./four-functions.csv"
meta_csv = "./four-functions-metadata.csv"

run_sim(
    input_csv,
    meta_csv,
    results_dir,
    **kwargs,
)


## plot some results
from load.analysis import LogParser
from load.run.run_trace import RunTarget, RunType
import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt

mpl.rcParams.update({"font.size": 14})
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42


parser = LogParser(results_dir, os.path.join(results_dir, input_csv), os.path.join(results_dir, meta_csv), benchmark, RunType.SIM)
parser.parse_logs()

# Plot per-worker, per-func warm hit rates
for worker in parser.worker_parsers:
    worker_name = worker.results_log.split('.')[0]
    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)

    labels = []
    for i, (func, df) in enumerate(worker.invokes_df.groupby("function_name")):
        ax.bar(i, height=len(df[~df["was_cold"]]) / len(df))
        labels.append(func)

    ax.set_xticks(list(range(len(labels))))
    ax.set_xticklabels(labels, rotation=90)
    ax.set_ylabel("Warm Hit %")
    ax.set_xlabel("Function Name")
    plt.savefig(os.path.join(results_dir, f"{worker_name}_warm_hits.png"), bbox_inches="tight")
    plt.close(fig)

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

for worker in parser.worker_parsers:
    ax.plot(worker.status_df["norm_time"].dt.total_seconds(), worker.status_df["num_running"], label=worker.results_log.split('/')[-1].split('.')[0])

ax.legend()
ax.set_ylabel("# Running")
ax.set_xlabel("Time (sec)")
plt.savefig(os.path.join(results_dir, f"cluster_running.png"), bbox_inches="tight")
plt.close(fig)

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

for worker in parser.worker_parsers:
    ax.plot(worker.status_df["norm_time"].dt.total_seconds(), worker.status_df["cpu_queue_len"], label=worker.results_log.split('/')[-1].split('.')[0])

ax.legend()
ax.set_ylabel("# Running")
ax.set_xlabel("Time (sec)")
plt.savefig(os.path.join(results_dir, f"cluster_queue.png"), bbox_inches="tight")
plt.close(fig)