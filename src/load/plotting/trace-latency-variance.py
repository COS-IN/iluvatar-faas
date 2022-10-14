
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
Plot the latency variance for each function in a trace
"""

parser = argparse.ArgumentParser(description='')
parser.add_argument("--file", required=True, type=str)
parser.add_argument("--outpath", required=True, type=str)
parser.add_argument("--type", required=True, type=str, help="'worker' or 'controller'", choices=['worker', 'controller'])
args = parser.parse_args()

bench_type = args.type.title()

warm_ms_data = defaultdict(list)
cold_ms_data = defaultdict(list)

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
    if was_cold:
      cold_ms_data[function_name].append(int(e2e_duration_ms))
    else:
      warm_ms_data[function_name].append(int(e2e_duration_ms))

cold_cnt = sum([len(x) for x in cold_ms_data.values()])
warm_cnt = sum([len(x) for x in warm_ms_data.values()])

################################################################

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

data = []
labels = []
for l, d in sorted(warm_ms_data.items(), key=lambda x: x[0]):
  print(np.std(d), np.var(d))
  exit(0)
  # ax.plot(quantlies, np.quantile(d, quantlies), label=l)
  data.append(d)
  labels.append(l)

save_fname = os.path.join(args.outpath, "{}_warm_variance.png".format(args.type))

ax.set_title("{} Per-function Cold-Start Overhead Quantiles".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Quantiles")
ax.legend()
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
for l, d in sorted(cold_ms_data.items(), key=lambda x: x[0]):
  ax.plot(quantlies, np.quantile(d, quantlies), label=l)
  data.append(d)
  labels.append(l)

save_fname = os.path.join(args.outpath, "{}_cold_variance.png".format(args.type))

ax.set_title("{} Per-function Cold-Start Overhead Quantiles".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Quantiles")
# ax.legend()
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)
