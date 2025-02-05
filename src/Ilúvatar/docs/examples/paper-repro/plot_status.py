import warnings

warnings.filterwarnings("error", category=FutureWarning)
import os
import argparse
import matplotlib as mpl
import matplotlib.patches as mpatches

mpl.rcParams.update({"font.size": 14})
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42
mpl.use("Agg")
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()
from Logs_to_df import Logs_to_df
import json
import numpy as np
import pandas as pd

from summarize_trace import AnalyzeTrace

"""
Plot all the energy readings and some load metrics alongside each other
"""

argparser = argparse.ArgumentParser()
argparser.add_argument(
    "--logs-folder",
    "-l",
    help="The folder worker logs are stored in",
    required=True,
    type=str,
)
argparser.add_argument(
    "--trace", "-t", help="Trace experiment was un with", required=True, type=str
)
argparser.add_argument(
    "--format", help="The graph format", required=False, type=str, default="png"
)
argparser.add_argument("--out", help="output folder", required=True, type=str)
argparser.add_argument(
    "--benchmark",
    help="benchmark file",
    required=False,
    type=str,
    default="../benchmark/worker_function_benchmarks.json",
)
args = argparser.parse_args()
os.makedirs(args.out, exist_ok=True)


def get_function_map(log_file):
    function_code_map = {}
    with open(log_file, "r") as f:
        for line in f.readlines():
            ljson = json.loads(line)
            if ljson["fields"]["message"] == "Function mapped to benchmark code":
                func_name = ljson["fields"]["function"]
                mapping = ljson["fields"]["chosen_code"]
                function_code_map[func_name] = mapping
    return function_code_map


with open(args.benchmark) as f:
    benchmark = json.load(f)["data"]

trace = pd.read_csv(os.path.join(args.trace, "trace.csv"))
metadata = pd.read_csv(os.path.join(args.trace, "metadata-trace.csv"))
fn_map = get_function_map(os.path.join(args.logs_folder, "load_gen.log"))

analyzer = AnalyzeTrace()
little_sum = 0.0
for func_name, group in trace.groupby("func_name"):
    iats = analyzer.gen_iat(group["invoke_time_ms"])
    iats = analyzer.trim_iats(iats)
    mean_iat_sec = np.mean(iats)
    mapping = fn_map[func_name]
    row = metadata[metadata["func_name"] == func_name]
    bench_data = benchmark[mapping]
    avg_warm_time = (
        np.mean(bench_data["resource_data"]["cpu"]["warm_results_sec"])
        + np.mean(bench_data["resource_data"]["cpu"]["warm_over_results_us"])
        / 1_000_000.0
    )
    avg_cold_time = (
        np.mean(bench_data["resource_data"]["cpu"]["cold_results_sec"])
        + np.mean(bench_data["resource_data"]["cpu"]["cold_over_results_us"])
        / 1_000_000.0
    )
    exp_run_time = (avg_warm_time * 0.99) + (avg_cold_time * 0.01)

    little = analyzer.count_littles_law(mean_iat_sec, exp_run_time)
    little_sum += little

ldf = Logs_to_df(args.logs_folder)
ldf.process_all_logs()

function_data = {}

fig, ax = plt.subplots()
labels = []
fig.set_size_inches(5, 3)

xs = ldf.status_df["minute"].to_numpy() - ldf.first_invoke
ax.stackplot(
    xs,
    ldf.status_df["num_running_funcs"].to_numpy(),
    ldf.status_df["queue_len"].to_numpy(),
    colors=["tab:cyan", "tab:green"],
)
ax.hlines(
    little_sum,
    min(xs),
    max(xs),
    colors=["red"],
    linestyles=["--"],
    label=["Little's Law Running"],
)
labels = ["Running", "Enqueued", "Little's Law Ideal"]

ax.legend(labels=labels, loc="best")
ax.set_xlim(0, max(xs))
save_fname = os.path.join(args.out, "paper-status.{}".format(args.format))

ax.set_xlabel("Elapsed time (min)")
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)
