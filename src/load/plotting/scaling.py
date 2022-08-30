
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
parser.add_argument("--path", required=True, type=str)
parser.add_argument("--out", required=False, type=str)
args = parser.parse_args()

if args.out is None:
  out_dir = args.path
else :
  out_dir = args.out

registration_data = defaultdict(list)
latency_data = defaultdict(list)
overhead_ms_data = defaultdict(list)
overhead_pct_data = defaultdict(list)

runtimes = 0
invocation_cnt = 0

for file in os.listdir(args.path):
  if "json" in file:
    info = file[:-len(".json")].split("-")
    if len(info) != 2 :
      continue
    path = os.path.join(args.path, file)
    num_threads, iteration = file[:-len(".json")].split("-")
    num_threads = int(num_threads)
    iteration = int(iteration)
    with open(path, 'r') as f:
      data = json.load(f)
    for entry in data:
      thread_id = entry["thread_id"]
      registration = entry["registration"]
      errors = entry["errors"]
      invocations = entry["data"]
      
      registration_data[num_threads].append(registration["duration_ms"])
      for invoke in invocations:
        duration_ms = invoke["duration_ms"]
        latency_data[num_threads].append(duration_ms)
        
        runtime_ms = invoke["json"]["body"]["latency"] * 1000
        runtimes += runtime_ms
        invocation_cnt += 1
        overhead_ms = duration_ms - runtime_ms
        overhead_pct = overhead_ms / duration_ms

        overhead_ms_data[num_threads].append(overhead_ms)
        overhead_pct_data[num_threads].append(overhead_pct)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

data = sorted(overhead_ms_data.items(), key=lambda x: x[0])
xs = [x for x,_ in data]
ys = [np.mean(y) for _,y in data]
ax.plot(xs, ys, label="Ilúvatar")
save_fname = os.path.join(out_dir, "overheads.png")

ax.set_title("Average platform overhead per-invocation as clients increase")
ax.set_ylabel("Platform overhead (ms)")
ax.set_xlabel("Num Client Threads")

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

data = sorted(overhead_pct_data.items(), key=lambda x: x[0])
xs = [x for x,_ in data]
ys = [np.mean(y) for _,y in data]
ax.plot(xs, ys, label="Ilúvatar")
save_fname = os.path.join(out_dir, "overheads_pct.png")

ax.set_title("Average percent of invocation time caused by platform overhead as clients increase")
ax.set_ylabel("Platform overhead")
ax.set_xlabel("Num Client Threads")

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

data = sorted(latency_data.items(), key=lambda x: x[0])
xs = [x for x,_ in data]
ys = [np.mean(y) for _,y in data]
ax.plot(xs, ys, label="Ilúvatar")
save_fname = os.path.join(out_dir, "latency.png")

ax.set_title("Average invocation latency as clients increase")
ax.set_ylabel("Invocation Latency (ms)")
ax.set_xlabel("Num Client Threads")

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

data = sorted(latency_data.items(), key=lambda x: x[0])
xs = [x for x,_ in data]
ys = [len(y) for _,y in data]
ax.plot(xs, ys, label="Ilúvatar")
save_fname = os.path.join(out_dir, "throughput.png")

ax.set_title("Total throughput as clients increase")
ax.set_ylabel("Invocations")
ax.set_xlabel("Num Client Threads")

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

data = sorted(latency_data.items(), key=lambda x: x[0])
xs = [x for x,_ in data]
ys = [len(y)/x for x,y in data]
ax.plot(xs, ys, label="Ilúvatar")
save_fname = os.path.join(out_dir, "avg_throughput.png")

ax.set_title("Per-client throughput as clients increase")
ax.set_ylabel("Invocations per thread")
ax.set_xlabel("Num Client Threads")

plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)