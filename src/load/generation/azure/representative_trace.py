from dataset import join_day_one, trace_row
import os
import pandas as pd
import pickle
from math import ceil
import argparse

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o')
argparser.add_argument("--data-path", '-d')
argparser.add_argument("--num-funcs", '-n', type=int)
argparser.add_argument("--force", '-f', action='store_true')
args = argparser.parse_args()

dataset = join_day_one(args.data_path)

quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]

per_qant = args.num_funcs // (len(quantiles)-1)
qts = dataset["total_invocations"].quantile(quantiles)
trace = []
function_metadata = []
metadata_save_pth = os.path.join(args.out_folder, "metadata-{}.csv".format(args.num_funcs))

if not os.path.exists(metadata_save_pth) or args.force:
  function_id = 0
  for i in range(4):
    low = qts.iloc[i]
    high = qts.iloc[i+1]
    choose_from = dataset[dataset["total_invocations"].between(low, high)]
    chosen = choose_from.sample(per_qant)

    for index, row in chosen.iterrows():
      traced_row, (func_name, cold_dur, warm_dur, mem) = trace_row(index, row, function_id)
      trace += traced_row
      function_metadata.append((func_name, cold_dur, warm_dur, mem, function_id))
      function_id += 1

  out_trace = sorted(trace, key=lambda x:x[1]) #(func_name, time_ms)
  print(args.num_funcs, len(out_trace))

  trace_save_pth = os.path.join(args.out_folder, "{}.csv".format(args.num_funcs))
  with open(trace_save_pth, "w") as f:
    f.write("{},{}\n".format("function_id", "invoke_time_ms"))
    for function_id, time_ms in out_trace:
      f.write("{},{}\n".format(function_id, time_ms))

  with open(metadata_save_pth, "w") as f:
    f.write("{},{},{},{},{}\n".format("func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb", "function_id"))
    for (func_name, cold_dur, warm_dur, mem, function_id) in function_metadata:
      f.write("{},{},{},{},{}\n".format(func_name, cold_dur, warm_dur, mem, function_id))

  print("done", trace_save_pth)