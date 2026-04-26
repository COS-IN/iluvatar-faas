import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from load.run.run_trace import rust_build, run_live, RunTarget, BuildTarget, make_host_queue

CORES = 48
MEMORY = 1024*100
build_level = BuildTarget.RELEASE
results_dir = os.path.join(os.getcwd(), "results")
os.makedirs(results_dir, exist_ok=True)
benchmark = "./worker_function_benchmarks.json"

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
    "gpus": 1,
    "running": 2,
    "worker_status_ms": 500,
    # "worker_log_dir": results_dir,
    # "controller_log_dir": results_dir,
    "target": RunTarget.CONTROLLER,
    "controller_thread_sleep_ms": 500,
    "controller_algorithm": "CHRLU",
    "prewarm": 0,
    "benchmark_file": benchmark,
    "force": False,
}
# run entire experiment
input_csv = os.path.join(os.getcwd(), "zipf-2/chosen-ecdf.csv")
meta_csv = os.path.join(os.getcwd(), "zipf-2/metadata-chosen-ecdf.csv")

SSH_Q = make_host_queue([("gpu_1_3", "v-gpu3.victor.futuresystems.org")])
run_live(
    input_csv,
    meta_csv,
    results_dir,
    SSH_Q,
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


parser = LogParser(results_dir, input_csv, meta_csv, benchmark, RunType.SIM)
parser.parse_logs()

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)
# Plot per-worker, per-func warm hit rates
for worker, color in zip(parser.worker_parsers, ["red", "green", "cyan", "gold"]):
    worker_name = worker.results_log.split('/')[-1].split('.')[0]

    # ax.plot(worker.status_df["norm_time"], worker.status_df["cpu_load_avg"], color=color, linestyle='--', label="CPU")
    ax.plot(worker.status_df["norm_time"], worker.status_df["gpu_load_avg"], color=color, linestyle='-', label=f"{worker_name} GPU")

# ax.set_xticks(list(range(len(labels))))
# ax.set_xticklabels(labels)
ax.legend()
ax.set_ylabel("Device Load Avg")
ax.set_xlabel("Time")
plt.savefig(os.path.join(results_dir, f"load_avgs.png"), bbox_inches="tight")
plt.close(fig)
