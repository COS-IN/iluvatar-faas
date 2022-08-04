
import os, sys
from collections import defaultdict
import argparse
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.patches as mpatches
import json
mpl.rcParams.update({'font.size': 14})
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.use('Agg')
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description='')
parser.add_argument("--file", required=True, type=str)
parser.add_argument("--outpath", required=True, type=str)
parser.add_argument("--type", required=True, type=str, help="'worker' or 'controller'", choices=['worker', 'controller'])
args = parser.parse_args()

bench_type = args.type.title()

with open(args.file , 'r') as f:
  data = json.load(f)

warm_overhead_ms_data = []
cold_overhead_ms_data = []
labels = []

for key, data in sorted(data["data"].items(), key=lambda x: x[0]):
  print(key)
  labels.append(key)
  cold_overhead_ms_data.append(data["cold_over_results"])
  warm_overhead_ms_data.append(data["warm_over_results"])

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

ax.boxplot(warm_overhead_ms_data, labels=labels)
save_fname = os.path.join(args.outpath, "{}_warm_overhead_boxplot.png".format(args.type))

ax.set_title("{} Per-function Warm-Start Overhead".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Function")
ax.set_xticklabels(labels, rotation = 90)

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

ax.boxplot(cold_overhead_ms_data, labels=labels)
save_fname = os.path.join(args.outpath, "{}_cold_overhead_boxplot.png".format(args.type))

ax.set_title("{} Per-function Cold-Start Overhead".format(bench_type))
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Function")
ax.set_xticklabels(labels, rotation = 90)

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)