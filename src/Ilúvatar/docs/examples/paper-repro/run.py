import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from load.run.run_trace import rust_build_native, run_sim, RunTarget, BuildTarget
from multiprocessing import Pool

# We have to run with all function spans enabled to capture worker information to separate log file
build_level = BuildTarget.RELEASE_SPANS
benchmark = "../benchmark/worker_function_benchmarks.json"

# build the solution
rust_build_native(ILU_HOME, None, build_level)

input_csv = os.path.join(os.getcwd(),"trace/trace.csv")
meta_csv = os.path.join(os.getcwd(),"trace/metadata-trace.csv")
results_dir = os.path.join(os.getcwd(), "results")


def run_experiment(cpu_queue, cores):
    exp_results_dir = os.path.join(os.getcwd(), results_dir, cpu_queue, str(cores))
    os.makedirs(results_dir, exist_ok=True)

    kwargs = {
        "ilu_home": ILU_HOME,
        "build_level": build_level,
        "cores": cores,
        "memory": 10240,
        "worker_status_ms": 500,
        "worker_log_dir": exp_results_dir,
        "cpu_queue_policy": cpu_queue,
        "target": RunTarget.WORKER,
        "prewarm": 1,
        "benchmark_file": benchmark,
        "load_log_stdout": False,
    }
    # run entire experiment
    run_sim(input_csv, meta_csv, exp_results_dir, **kwargs)
    log_file = os.path.join(exp_results_dir, "orchestration.log")
    # run_cmd(
    #     [
    #         "python3",
    #         "plot_status.py",
    #         "-l",
    #         results_dir,
    #         "-t",
    #         "trace",
    #         "--out",
    #         results_dir,
    #     ],
    #     log_file,
    # )


experiments = []
for cores in [0, 10, 12, 15, 16, 20]:
    experiments.append(("fcfs", cores))
for cores in [10, 12, 15, 16, 20]:
    experiments.append(("minheap", cores))

with Pool() as p:
    p.starmap(run_experiment, experiments)

## plot some results
from load.analysis import WorkerLogParser, parse_data, BaseParser
from load.analysis.log_parser import *
from load.run.run_trace import RunTarget, RunType
from collections import defaultdict
import pandas as pd
import numpy as np
import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


@WorkerLogParser.register_parser
class ReuseDistanceParser(BaseParser):
    """
    An example of injecting a custom parser, computing reuse distances of invocations
    """

    def __init__(self, main_parser: WorkerLogParser):
        super().__init__(main_parser)
        self.reuse_distances = defaultdict(list)
        self.last_seen = {}

    def calc_reuse(self, log):
        tid = log["fields"]["tid"]
        invoke = self.main_parser.invokes_df[self.main_parser.invokes_df.index == tid]
        if len(invoke) == 0:
            # invocation had an error somewhere
            return
        fname = invoke.iloc[0]["function_name"]
        if fname in self.last_seen:
            distance = self.last_seen[fname]
            for k in self.last_seen.keys():
                self.last_seen[k] += 1
            self.last_seen[fname] = 0
            self.reuse_distances[fname].append(distance)
        else:
            self.last_seen[fname] = 0

    def log_completed(self):
        reuse_data = []
        for name, data in self.reuse_distances.items():
            reuse_data.append((name, np.mean(data)))
        df = pd.DataFrame.from_records(
            reuse_data, columns=["func_name", "avg_reuse_dis"], index="func_name"
        )
        self.main_parser.metadata_df = self.main_parser.metadata_df.join(df)

    parser_map = {
        "Item starting to execute": calc_reuse,
    }


mpl.rcParams.update({"font.size": 14})
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42

folder_structure = ["cpu_queue", "cpu_cores"]


def filter_fn(found_results):
    ret = []
    for pth, run_data in found_results:
        # filter some set of results we do or don't want
        if int(run_data["cpu_cores"]) != 16:
            ret.append((pth, run_data))
    return ret


parsed_data = parse_data(
    results_dir,
    input_csv,
    meta_csv,
    benchmark,
    folder_structure,
    RunType.SIM,
    RunTarget.WORKER,
    filter_fn=filter_fn,
)


def plot_overhead():
    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)

    pt = 0
    labels = []

    fcfs_parsed_data = filter(lambda x: x.run_data["cpu_queue"] == "fcfs", parsed_data)
    minheap_parsed_data = filter(
        lambda x: x.run_data["cpu_queue"] == "minheap", parsed_data
    )

    fcfs_parsed_data = sorted(
        fcfs_parsed_data, key=lambda x: int(x.run_data["cpu_cores"])
    )
    minheap_parsed_data = sorted(
        minheap_parsed_data, key=lambda x: int(x.run_data["cpu_cores"])
    )

    for parser in fcfs_parsed_data:
        df = parser.invokes_df
        ax.bar(pt, height=df["e2e_sec"].mean(), yerr=df["e2e_sec"].std(), color="red")
        labels.append(parser.run_data["cpu_cores"])
        pt += 1

    for parser in minheap_parsed_data:
        df = parser.invokes_df
        ax.bar(pt, height=df["e2e_sec"].mean(), yerr=df["e2e_sec"].std(), color="blue")
        labels.append(parser.run_data["cpu_cores"])
        pt += 1

    ax.set_xticks(list(range(len(labels))))
    ax.set_xticklabels(labels)
    ax.legend(
        handles=[Patch(color="red"), Patch(color="blue")], labels=["FCFS", "MinHeap"]
    )
    ax.set_ylabel("Platform Overhead (sec.)")
    ax.set_xlabel("CPU Cores experiment")
    plt.savefig(os.path.join(results_dir, "e2e_time.png"), bbox_inches="tight")
    plt.close(fig)


def plot_reuse():
    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)

    pt = 0
    labels = []

    fcfs_parsed_data = filter(lambda x: x.run_data["cpu_queue"] == "fcfs", parsed_data)
    minheap_parsed_data = filter(
        lambda x: x.run_data["cpu_queue"] == "minheap", parsed_data
    )

    fcfs_parsed_data = sorted(
        fcfs_parsed_data, key=lambda x: int(x.run_data["cpu_cores"])
    )
    minheap_parsed_data = sorted(
        minheap_parsed_data, key=lambda x: int(x.run_data["cpu_cores"])
    )

    for parser in fcfs_parsed_data:
        box = ax.boxplot(parser.metadata_df["avg_reuse_dis"], positions=[pt])
        for item in ["boxes", "whiskers", "fliers", "caps"]:
            plt.setp(box[item], color="red")

        labels.append(parser.run_data["cpu_cores"])
        pt += 1

    for parser in minheap_parsed_data:
        box = ax.boxplot(parser.metadata_df["avg_reuse_dis"], positions=[pt])
        for item in ["boxes", "whiskers", "fliers", "caps"]:
            plt.setp(box[item], color="blue")
        labels.append(parser.run_data["cpu_cores"])
        pt += 1

    ax.set_xticks(list(range(len(labels))))
    ax.set_xticklabels(labels)
    ax.legend(
        handles=[Patch(color="red"), Patch(color="blue")], labels=["FCFS", "MinHeap"]
    )
    ax.set_ylabel("Resuse Distance")
    ax.set_xlabel("CPU Cores experiment")
    plt.savefig(os.path.join(results_dir, "reuse.png"), bbox_inches="tight")
    plt.close(fig)


plot_overhead()
plot_reuse()
