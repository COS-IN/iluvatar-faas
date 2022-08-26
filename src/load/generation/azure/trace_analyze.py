"""
Script to read a trace and find the (rough theoretical) maximum resource usages of a trace
Analyzing it at various levels of warm-hit-percentages
"""
import argparse
from collections import defaultdict
from heapq import heappush, heappop
import numpy as np
from multiprocessing import pool

argparser = argparse.ArgumentParser()
argparser.add_argument("--trace-csv", '-t', required=True, type=str)
argparser.add_argument("--metadata-csv", '-m', required=True, type=str)
args = argparser.parse_args()

metadata = defaultdict(dict)
with open(args.metadata_csv, 'r') as f:
  f.readline()
  for line in f.readlines():
    func_name,cold_dur_ms,warm_dur_ms,mem_mb,function_id = line.split(',')
    function_id = function_id.strip("\n")
    metadata[function_id]["func_name"] = func_name
    metadata[function_id]["cold_dur_ms"] = int(cold_dur_ms)
    metadata[function_id]["warm_dur_ms"] = int(warm_dur_ms)
    metadata[function_id]["mem_mb"] = int(mem_mb)

def gen_fin_t(warm_pct, func_dict, curr_t):
  if np.random.rand() <= warm_pct:
    fin_t = curr_t + func_dict["warm_dur_ms"]
  else:
    fin_t = curr_t + func_dict["cold_dur_ms"]
  return fin_t

def run_trace(trace_csv, warm_pct, metadata):
  running = []
  running_mem = 0
  max_mem = 0
  max_running = 0

  curr_t = 0
  with open(trace_csv, 'r') as f:
    f.readline()
    for line in f.readlines():
      function_id, invoke_time_ms = line.split(',')
      curr_t = int(invoke_time_ms.strip("\n"))
      running_mem += metadata[function_id]["mem_mb"]
      fin_t = gen_fin_t(warm_pct, metadata[function_id], curr_t)
      while len(running) > 0:
        popped = heappop(running)
        if popped[0] >= curr_t:
          heappush(running, popped)
          break
        else:
          running_mem -= metadata[popped[1]]["mem_mb"]
      heappush(running, (fin_t, function_id))
      if running_mem > max_mem:
        max_mem = running_mem
      if len(running) > max_running:
        max_running = len(running)
  return warm_pct, max_mem, max_running

if __name__ == '__main__':
  with pool.Pool() as p:
    handles = []
    for pct in [0, 0.25, 0.5, 0.75, 0.80, 0.85, 0.9, 0.95, 0.99, 0.999, 1]:
      h = p.apply_async(run_trace, (args.trace_csv, pct, metadata))
      handles.append(h)
    results = [h.get() for h in handles]
    results = sorted(results, key=lambda x: x[0])
    print("warm_pct, max_mem, max_running")
    for warm_pct, max_mem, max_running in results:
      print("{}, {}, {}".format(warm_pct, max_mem, max_running))
