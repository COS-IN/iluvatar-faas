from span import *
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
Plot a timeline of function invocations
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--log", '-l', help="The worker log file", required=True, type=str)
argparser.add_argument("--csv", '-c', help="The csv output file from the run", required=True, type=str)
argparser.add_argument("--output", '-o', help="Output folder to put graphs in", required=False, type=str, default="plots")
argparser.add_argument("--format", help="Output format for figures", required=False, type=str, default="png")
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

with open(args.log, 'r') as f:
  while True:
    line = f.readline()
    if line == "":
      break
    if "DEBUG" in line:
      continue
    try:
      log = json.loads(line)
    except Exception as e:
      print("read ERROR:", e)
      print(line)
      exit(1)
    tid = get_tid(log)
    if tid is None or tid not in tids:
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
  for i, span in enumerate(exit_list):
    if short_span_name(target_span) == short_span_name(span):
      return exit_list.pop(i)
  else:
    tid = get_tid(log)
    n = short_span_name(target_span)
    raise Exception(f"Unable to find exit span for {tid} {n} {log}")

seen_once=False
def get_span_timeline(span, sorted_start_spans, exit_spans, depth=1):
  timeline = []
  start_t = parse_date(span["timestamp"])
  exit_t = parse_date(find_matching_span(span, exit_spans)["timestamp"])
  short_name = short_span_name(span)
  timeline.append( (short_name, start_t, exit_t, depth) )

  if len(sorted_start_spans) >= 0:
    seen_add_container = False
    removed = 0
    for i in range(len(sorted_start_spans)):
      if is_child(span, sorted_start_spans[i-removed], exit_t):
        new_curr = sorted_start_spans[i-removed]
        if short_span_name(new_curr) == "container_pool::add_container":
          global seen_once
          # if seen_once and not seen_add_container:
          #   print(short_span_name(span), "skipping")
          #   continue
          # sorted_start_spans.pop(i-removed)
          removed += 1
          if seen_add_container:
            print(short_span_name(span), "skipping")
            seen_once = True
            continue
          seen_add_container = True
        timeline += get_span_timeline(new_curr, sorted_start_spans[1:], exit_spans, depth=depth+1)
  return sorted(timeline, key=lambda x: parse_date(x[1]))

def plot_invocation_timeline(tid, title):
  reodreded = sorted(by_tid_entry[tid], key=lambda x: parse_date(x["timestamp"]))
  timeline = get_span_timeline(reodreded[0], reodreded[1:], by_tid_exit[tid])
  target = result_df[result_df["tid"]==tid]

  first_t = timeline[0][1]
  data = []
  labels = []
  left = []
  heights = []
  invoke_start = None
  for name, start_t, end_t, depth in timeline:
    left.append( (start_t - first_t).total_seconds() * 1000.0 )
    data.append( (end_t - start_t).total_seconds() * 1000.0 )
    labels.append(name)
    heights.append(depth)
    if invoke_start is None and name == "containerdstructs::call_container":
      invoke_start = left[-1]
      invoke_end = invoke_start + float(target["code_duration_ms"])

  left = np.array(left)
  data = np.array(data)
  seen_add_container = False
  seen_add_to_pool = False
  for i, name in enumerate(labels):
    # these functions cross the invocation boundary, so we should remove the invoke time from their duration
    if name in ["ContainerLock::invoke", "cpu_q_invoke::invoke_on_container", "queueing_dispatcher::sync_invocation", 
                "invoker_trait::invocation_worker_thread", "containerdstructs::call_container", "iluvatar_worker::invoke", 
                "ContainerdContainer::invoke"]:
      data[i] = data[i] - float(target["code_duration_ms"])

      if data[i] < 0:
        raise Exception(f"function '{name}' had a duration of '{data[i]}'")
    # these functions start and finish after the invocation, so we should remove the invoke time from their start
    if name in ["containermanager::return_container", "containerdstructs::download_text", "container_pool::add_container", 
                "container_pool::remove_container", "containermanager::add_container_to_pool"]:
      if name == "container_pool::add_container":
        if seen_add_container:
          left[i] = left[i] - float(target["code_duration_ms"])
        seen_add_container = True
      elif name == "containermanager::add_container_to_pool":
        if seen_add_to_pool:
          left[i] = left[i] - float(target["code_duration_ms"])
        seen_add_to_pool = True
      elif name == "container_pool::remove_container":
        left[i] = left[i] - float(target["code_duration_ms"])
      else:
        left[i] = left[i] - float(target["code_duration_ms"])


  for i, name in enumerate(labels):
    if name == "container_pool::add_container":
      print(left[i], data[i])
  fig, ax = plt.subplots(figsize=(12, 6))
  pts = [0 for _ in range(len(data))]
  colors = ["tab:gray","tab:blue","tab:red", "gold","tab:green","tab:cyan", "tab:olive","tab:purple",
              "tab:orange", "tab:pink", "tab:brown", "tan", "indigo", "magenta", "lightblue", "maroon", 
              "purple", "blue", "violet", "crimson", "salmon", "peru", "lime"]
  ax.barh(pts, data, left=left, color=colors, height=heights)
  ax.set_xlabel("Time (ms)")
  bottom, top = ax.get_ylim()
  ax.set_ylim(0, top)
  ax.set_title(title)
  ax.set_yticks([])
  ax.set_yticklabels([])

  legends = []
  for i in range(len(labels)):
    patch = mpatches.Patch(color=colors[i], label=labels[i])
    legends.append(patch)
  ax.legend(handles=legends, bbox_to_anchor=(0,1))

  save_fname = os.path.join(args.output, f"{tid[:8]}-timeline.{args.format}")
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

for index, row in result_df.sample(1, random_state=0).iterrows():
  ms = row["overhead_ms"]
  tid = row["tid"]
  title = f"{tid} Invocation Timelime"
  plot_invocation_timeline(tid, title)
