"""
Script to read a trace and find the (rough theoretical) maximum resource usages of a trace
Analyzing it at various levels of warm-hit-percentages
"""
import argparse
from collections import defaultdict
from heapq import heappush, heappop
import numpy as np
import pandas as pd
from multiprocessing import pool
from tabulate import tabulate

def gen_fin_t(warm_pct, func_dict, curr_t):
  if np.random.rand() <= warm_pct:
    fin_t = curr_t + func_dict["warm_dur_ms"]
  else:
    fin_t = curr_t + func_dict["cold_dur_ms"]
  return fin_t

def run_trace_csv(trace_csv, warm_pct, metadata_csv):
  metadata = defaultdict(dict)
  with open(metadata_csv, 'r') as f:
    f.readline()
    for line in f.readlines():
      func_name,cold_dur_ms,warm_dur_ms,mem_mb,mean_iat = line.split(',')
      metadata[func_name]["func_name"] = func_name
      metadata[func_name]["cold_dur_ms"] = int(cold_dur_ms)
      metadata[func_name]["warm_dur_ms"] = int(warm_dur_ms)
      metadata[func_name]["mem_mb"] = int(mem_mb)
      
  df = pd.read_csv(trace_csv)
  trace = list(map(lambda x: x[1], df.iterrows()))
  return run_trace(trace, warm_pct, metadata)

def run_trace(trace, warm_pct, metadata):
  running = []
  running_mem = 0
  history = {}

  curr_t = 0
  for func_name, invoke_time_ms in trace:
    curr_t = invoke_time_ms
    running_mem += metadata[func_name]["mem_mb"]
    fin_t = gen_fin_t(warm_pct, metadata[func_name], curr_t)
    while len(running) > 0:
      popped = heappop(running)
      if popped[0] >= curr_t:
        heappush(running, popped)
        break
      else:
        running_mem -= metadata[popped[1]]["mem_mb"]
    heappush(running, (fin_t, func_name))
    history[curr_t] = (len(running), running_mem)
  usage_df = pd.DataFrame.from_dict(history, columns=["running", "memory"], orient='index')
  # print(usage_df)
  # exit()
  max_mem = usage_df["memory"].max()
  max_running = usage_df["running"].max()
  mean_running = usage_df["running"].mean()
  running_75th = usage_df["running"].quantile(0.75)
  running_90th = usage_df["running"].quantile(0.9)

  return warm_pct, max_mem, max_running, mean_running, running_75th, running_90th

if __name__ == '__main__':
  argparser = argparse.ArgumentParser()
  argparser.add_argument("--trace-csv", '-t', required=True, type=str)
  argparser.add_argument("--metadata-csv", '-m', required=True, type=str)
  args = argparser.parse_args()

  with pool.Pool() as p:
    handles = []
    for pct in [0, 0.25, 0.5, 0.75, 0.80, 0.85, 0.9, 0.95, 0.99, 0.999, 1]:
      h = p.apply_async(run_trace_csv, (args.trace_csv, pct, args.metadata_csv))
      handles.append(h)
    results = [h.get() for h in handles]
    results = sorted(results, key=lambda x: x[0])
    header="warm_pct, max_mem, max_running, mean_running, running_75th, running_90th"
    print(header)
    header=header.split(", ")
    items=[]
    for warm_pct, max_mem, max_running, mean_running, running_75th, running_90th in results:
      print("{}, {}, {}, {}, {}, {}".format(warm_pct, max_mem, max_running, mean_running, running_75th, running_90th))
      items.append( [warm_pct, max_mem, max_running, mean_running, running_75th, running_90th] )
    
    with open("stats_analyzed.txt",'w') as f:
        print(tabulate( items, headers=header ),file=f)

