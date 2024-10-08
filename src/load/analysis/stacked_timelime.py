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

accumulated_times_ms = defaultdict(lambda: defaultdict(float))
span_enter = defaultdict(lambda: defaultdict(float))
times = defaultdict(list)
by_tid_entry = defaultdict(list)
by_tid_exit = defaultdict(list)
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

def span_function(log):
  if type(log) is dict:
    return log['span']['name']
  elif type(log) is str:
    return log.split("::")[-1]

def is_child(parent_log, check_log):
  parent_fn = span_function(parent_log)
  if short_span_name(parent_log) == "iluvatar_worker::invoke" and short_span_name(check_log) == "invoker::enqueue_new_invocation":
    # hack because the API and invoker class have a shared function name
    # so that name will be in `enqueue`'s parent spans, but not associated
    return False

  # hack because the span is not being correctly sent through the invoker queue
  if short_span_name(parent_log) == "invoker::invoke" and short_span_name(check_log) == "invoker::invocation_worker_thread":
    # they are "parent/child" if crossing the invoke queue boundary
    return get_tid(parent_log) == get_tid(check_log)

  # hack because the span is not being correctly sent through the invoker queue
  if short_span_name(parent_log) == "invoker::invoke" and short_span_name(check_log) == "invoker::spawn_tokio_worker":
    # they are "parent/child" if crossing the invoke queue boundary
    return get_tid(parent_log) == get_tid(check_log)

  if "spans" in check_log:
    if len(check_log["spans"]) <= 0:
      return False
    else:
      newest_parent = check_log["spans"][-1]
      return newest_parent["name"] == parent_fn
  return False

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
      by_tid_entry[tid].append(log)
    if log["fields"]["message"] == "close":
      times[span_name(log)].append(span_time_ms(log))
      by_tid_exit[tid].append(log)

def find_matching_span(target_span, exit_list):
  for span in exit_list:
    if short_span_name(target_span) == short_span_name(span):
      return span
  else:
    raise Exception(f"Unable to find exit span for {short_span_name(target_span) }")

def get_span_timeline(span, sorted_start_spans, exit_spans, depth=1):
  timeline = []  
  start_t = parse_date(span["timestamp"])
  exit_t = parse_date(find_matching_span(span, exit_spans)["timestamp"])
  timeline.append( (short_span_name(span), start_t, exit_t, depth) )
  # print(f"start {short_span_name(span)}", start_t)

  if len(sorted_start_spans) >= 0:
    for i in range(len(sorted_start_spans)):
      if is_child(span, sorted_start_spans[i]):
        new_curr = sorted_start_spans[i]
        timeline += get_span_timeline(new_curr, sorted_start_spans[1:], exit_spans, depth=depth+1)

  # print(f"exit {short_span_name(span)}", exit_t)
  return sorted(timeline, key=lambda x: parse_date(x[1]))

def plot_invocation_timeline(tid):
  # print(by_tid_entry[tid])
  reodreded = sorted(by_tid_entry[tid], key=lambda x: parse_date(x["timestamp"]))
  timeline = get_span_timeline(reodreded[0], reodreded[1:], by_tid_exit[tid])
  # print(timeline)
  target = result_df[result_df["tid"]==tid]
  print(int(target["overhead_ms"]), int(target["overhead_ms"]))

  first_t = timeline[0][1]
  data = []
  labels = []
  left = []
  heights = []
  invoke_start = 0
  invoke_end = 0
  for name, start_t, end_t, depth in timeline:
    if invoke_start == 0:
      if name == "ContainerdContainer::invoke":
        invoke_start = left[-1]
        invoke_end = data[-1]# + invoke_start
        continue
      # if name == "ContainerLock::invoke":
      #   continue

    left.append( (start_t - first_t).total_seconds() * 1000.0 )
    data.append( (end_t - start_t).total_seconds() * 1000.0 )
    labels.append(name)
    heights.append(depth)

  # print(invoke_start, invoke_end)
  left = np.array(left)
  data = np.array(data)
  # print(labels)
  # print(left)
  # print(data)

  for i, name in enumerate(labels):
    if name in ["ContainerLock::invoke", "invoker::invoke_internal", "invoker::invocation_worker_thread", "invoker::invoke", "iluvatar_worker::invoke"]:
      data[i] = data[i] - invoke_end
    if name in ["containermanager::return_container"]:
      left[i] = left[i] - invoke_end

  # print(left)
  # print(data)

  fig, ax = plt.subplots()
  plt.tight_layout()
  pts = [0 for _ in range(len(data))]

  colors = ["tab:gray","tab:blue","tab:red", "gold","tab:green","tab:cyan", "tab:olive","tab:purple","tab:orange", "tab:pink", "tab:brown"]
  ax.barh(pts, data, left=left, color=colors, height=heights)
  ax.set_xlabel("Time (ms)")
  bottom, top = ax.get_ylim()
  ax.set_ylim(0, top)
  ax.set_yticks([])
  ax.set_yticklabels([])

  legends = []
  for i in range(len(labels)):
    if labels[i] == "ContainerLock::invoke":
      continue
    patch = mpatches.Patch(color=colors[i], label=labels[i])
    legends.append(patch)
  ax.legend(handles=legends, bbox_to_anchor=(0,1))

  save_fname = os.path.join(args.output, f"{tid[:5]}-timeline.png")
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

for i in range(5):
  k = list(by_tid_entry.keys())[i]
  plot_invocation_timeline(k)
  # break