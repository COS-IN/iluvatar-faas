import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from load.run.run_trace import rust_build, run_sim, RunTarget, BuildTarget

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
    "num_workers": 1,
    "prewarm": 1,
    "benchmark_file": benchmark,
    "force": True,
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
from load.analysis import WorkerLogParser
from load.run.run_trace import RunTarget, RunType
import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt

mpl.rcParams.update({"font.size": 14})
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42


parser = WorkerLogParser(
    results_dir, input_csv, meta_csv, benchmark, RunType.SIM, RunTarget.CONTROLLER
)
parser.parse_logs()

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

labels = []
for i, (func, df) in enumerate(parser.invokes_df.groupby("function_name")):
    ax.bar(i, height=len(df[~df["was_cold"]]) / len(df))
    labels.append(func)


ax.set_xticks(list(range(len(labels))))
ax.set_xticklabels(labels)
ax.set_ylabel("Warm Hit %")
ax.set_xlabel("Function Name")
plt.savefig(os.path.join(results_dir, "warm_hits.png"), bbox_inches="tight")
plt.close(fig)
