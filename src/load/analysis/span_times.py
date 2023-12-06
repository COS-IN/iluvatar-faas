import argparse
import json
from collections import defaultdict
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.patches as mpatches
mpl.rcParams.update({'font.size': 14})
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.use('Agg')
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta, timezone

"""
Calculates and prints the number of each log message in the given log file
And their contribution to the file size
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--log", '-l', help="The worker log file", required=True, type=str)
argparser.add_argument("--csv", '-c', help="The csv output file from the run", required=True, type=str)
argparser.add_argument("--output", '-o', help="The csv output file from the run", required=False, type=str, default="plots")

args = argparser.parse_args()

result_df = pd.read_csv(args.csv)
result_df["code_duration_ms"] = result_df["code_duration_sec"] * 1000.0
if "e2e_duration_us" in result_df.columns:
  result_df["e2e_duration_ms"] = result_df["e2e_duration_us"]/1000.0 

result_df["overhead_ms"] = result_df["e2e_duration_ms"] - result_df["code_duration_ms"]
result_df = result_df[~result_df["was_cold"]]
tids = set(result_df["tid"])

print(result_df.describe())

accumulated_times_ms = defaultdict(lambda: defaultdict(float))
span_enter = defaultdict(lambda: defaultdict(float))
times = defaultdict(list)
by_tid = defaultdict(list)
invocation_tids = set()
targets = set()

def span_name(log) -> str:
  return f"{log['target']}::{log['span']['name']}"

def convert_time_to_ms(time: str):
  for i, c in enumerate(time[::-1]):
    if c.isnumeric():
      break
  unit = time[-i:]
  time = time[:-i]
  # print(i, unit, time)
  if unit == "µs":
    return float(time) / 1000.0
  elif unit == "ms":
    return float(time)
  elif unit == "s":
    return float(time) * 1000.0
  else:
    raise Exception(f"unknown unit of time: {unit}")

def span_time_ms(log, both: bool = False):
  try:
    work = log["fields"]["time.busy"]
    wait = log["fields"]["time.idle"]
  except Exception as e:
    print(e)
    print(log)
    raise e
  work = convert_time_to_ms(work)
  wait = convert_time_to_ms(wait)
  if both:
    return work + wait
  return work

def get_tid(log):
  try:
    if "span" in log:
      return log["span"]["tid"]
    elif "fields" in log:
      return log["fields"]["tid"]
    else:
      raise Exception("unknown format")
  except Exception as e:
    print(e)
    print(log)
    exit(1)

def parse_date(string):
  # 2022-10-28 09:20:07.68160776
  return pd.to_datetime(string, format="%Y-%m-%d %H:%M:%S.%f")

def short_span_name(log):
  if type(log) is dict:
    full_name = span_name(log)
  elif type(log) is str:
    full_name = log
  splits = full_name.split("::")
  return "::".join(splits[-2:])

with open(args.log, 'r') as f:
  while True:
    line = f.readline()
    if line == "":
      break
    try:
      log = json.loads(line)
    except Exception as e:
      print(e)
      print(line)
      exit(1)
    tid = get_tid(log)
    if tid not in tids:
      continue

    if log["fields"]["message"] == "enter":
      span_enter[tid][span_name(log)] = parse_date(log["timestamp"])

    if log["fields"]["message"] == "exit":
      end_t = parse_date(log["timestamp"])
      duration = end_t - span_enter[tid][span_name(log)]
      accumulated_times_ms[tid][span_name(log)] += duration.total_seconds() * 1000.0

    if log["fields"]["message"] == "new":
      targets.add(span_name(log))
      if span_name(log) == "iluvatar_worker_library::worker_api::iluvatar_worker::invoke":
        invocation_tids.add(tid)
    if log["fields"]["message"] == "close":
      # if "iluvatar_worker_library::services::invocation::invoker::invoke" == span_name(log):
      #   print(log)
      #   exit()
      # if get_tid(log) in invocation_tids:
      by_tid[tid].append(log)
      # span_time(log)
      times[span_name(log)].append(span_time_ms(log))

def plot_time_error(df, by_tid):
  errors = []
  for tid, logs in by_tid.items():
    time = 0.0
    for log in logs:
      time += span_time_ms(log)
    entry = df[df["tid"]==tid]
    errors.append(int(entry["overhead_ms"]) - time)

  fig, ax = plt.subplots()
  plt.tight_layout()

  ax.boxplot(errors)

  ax.set_ylabel("Time (ms)")

  save_fname = os.path.join(args.output, "span-error.png")
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

def plot_avg_span_time(times):
  labels = []
  data = []
  for span, ts in times.items():
    labels.append(short_span_name(span))
    data.append(np.mean(ts))

  fig, ax = plt.subplots()
  plt.tight_layout()
  pts = np.arange(len(data))

  ax.barh(pts, data)

  ax.set_xlabel("Time (ms)")
  ax.set_yticks(pts)
  ax.set_yticklabels(labels)

  save_fname = os.path.join(args.output, "span-avg-times.png")
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

def plot_span_diff(by_tid, accumulated_times_ms):
  span_errors = defaultdict(list)
  for tid in by_tid.keys():
    for span in by_tid[tid]:
      e2e_time_ms = span_time_ms(span)
      acc_time_ms = accumulated_times_ms[tid][span_name(span)]
      error = e2e_time_ms - acc_time_ms
      span_errors[short_span_name(span)].append(error)

  labels = []
  data = []
  for span, ts in span_errors.items():
    labels.append(short_span_name(span))
    data.append(np.mean(ts))

  fig, ax = plt.subplots()
  plt.tight_layout()
  pts = np.arange(len(data))

  ax.barh(pts, data)

  ax.set_xlabel("Time (ms)")
  ax.set_yticks(pts)
  ax.set_yticklabels(labels)

  save_fname = os.path.join(args.output, "span-diff.png")
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

plot_time_error(result_df, by_tid)
plot_avg_span_time(times)
plot_span_diff(by_tid, accumulated_times_ms)
