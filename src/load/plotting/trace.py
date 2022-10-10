
import os, sys
from collections import defaultdict
import argparse
from turtle import position
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.patches as mpatches
mpl.rcParams.update({'font.size': 14})
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.use('Agg')
import matplotlib.pyplot as plt

"""
Plot the worker overhead for each function in a trace, using violing plots and quantiles
"""

parser = argparse.ArgumentParser(description='')
parser.add_argument("--file", required=True, type=str)
parser.add_argument("--outpath", required=True, type=str)
parser.add_argument("--type", required=True, type=str, help="'worker' or 'controller'", choices=['worker', 'controller'])
args = parser.parse_args()

bench_type = args.type.title()

warm_overhead_ms_data = defaultdict(list)
cold_overhead_ms_data = defaultdict(list)

with open(args.file) as f:
  f.readline()

  for i, line in enumerate(f.readlines()):
    success,function_name,was_cold,worker_duration_ms,code_duration_sec,e2e_duration_ms = line.split(",")
    function_name, *_ = function_name.split("-")
    if was_cold == "true":
      was_cold = True
    elif was_cold == "false":
      was_cold = False
    else:
      raise Exception(f"Illegal was_cold value: '{was_cold}'")
    code_duration_ms = float(code_duration_sec) * 1000
    overhead_ms = int(worker_duration_ms) - code_duration_ms
    if was_cold:
      cold_overhead_ms_data[function_name].append(overhead_ms)
    else:
      if overhead_ms > 100:
        print(i, overhead_ms)
        continue
      warm_overhead_ms_data[function_name].append(overhead_ms)

cold_cnt = sum([len(x) for x in cold_overhead_ms_data.values()])
warm_cnt = sum([len(x) for x in warm_overhead_ms_data.values()])

################################################################

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

data = []
labels = []
quantlies = [0.5, 0.8, 0.9, 0.99, 0.999]
for l, d in sorted(warm_overhead_ms_data.items(), key=lambda x: x[0]):
  ax.plot(quantlies, np.quantile(d, quantlies), label=l)
  data.append(d)
  labels.append(l)

save_fname = os.path.join(args.outpath, "{}_warm_overhead_qs.png".format(args.type))

ax.set_title("{} Per-function Warm-Start Overhead Quantiles".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Quantiles")
# ax.legend()
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

positions = [i for i in range(len(labels))]

ax.violinplot(data, positions=positions)
save_fname = os.path.join(args.outpath, "{}_warm_overhead.png".format(args.type))

ax.set_title("{} Per-function Warm-Start Overhead".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Function")
ax.set_xticks(positions)
ax.set_xticklabels(labels, rotation = 90)

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

# FYI:
# If there are prewarmed containers, "cold" starts will have very low latency
# So these graphs may not be accurate
# "Outliers" are just actually true cold starts
# TODO: parse worker log to check if invoked on a pre-warmed container?

data = []
labels = []
quantlies = [0.5, 0.8, 0.9, 0.99, 0.999]
for l, d in sorted(cold_overhead_ms_data.items(), key=lambda x: x[0]):
  ax.plot(quantlies, np.quantile(d, quantlies), label=l)
  data.append(d)
  labels.append(l)

save_fname = os.path.join(args.outpath, "{}_cold_overhead_qs.png".format(args.type))

ax.set_title("{} Per-function Cold-Start Overhead Quantiles".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Quantiles")
# ax.legend()
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

positions = [i for i in range(len(labels))]
ax.violinplot(data, positions=positions)
save_fname = os.path.join(args.outpath, "{}_cold_overhead.png".format(args.type))

ax.set_title("{} Per-function Cold-Start Overhead".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Function")

ax.set_xticks(positions)
ax.set_xticklabels(labels, rotation = 90)

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)
