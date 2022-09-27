from dataset import join_day_one, iat_trace_row
from trace_analyze import run_trace_csv
import os
import argparse
from contextlib import suppress

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o', help="The folder to store the output csv files into", required=True)
argparser.add_argument("--data-path", '-d', help="The folder where the azure dataset has been downloaded to", required=True)
argparser.add_argument("--num-funcs", '-n', type=int, help="Number of functions to sample for the trace", required=True)
argparser.add_argument("--force", '-f', action='store_true', help="Overwrite an existing trace that has the same number of functions")
argparser.add_argument("--duration", type=int, help="The length in minutes of the trace", default=60)
args = argparser.parse_args()

dataset = join_day_one(args.data_path, args.force, iats=True)
# dataset = dataset[dataset["IAT_std"] != 0.0]
dataset = dataset[dataset["dur_iat_ratio"] < 4]

quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]

per_qant = args.num_funcs // (len(quantiles)-1)
qts = dataset["IAT_mean"].quantile(quantiles)
trace = []
function_metadata = []
metadata_save_pth = os.path.join(args.out_folder, "metadata-{}.csv".format(args.num_funcs))
with suppress(FileExistsError):
    os.makedirs(args.out_folder)

if not os.path.exists(metadata_save_pth) or args.force:
  function_id = 0
  for i in range(4):
    low = qts.iloc[i]
    high = qts.iloc[i+1]
    choose_from = dataset[dataset["IAT_mean"].between(low, high)]
    chosen = choose_from.sample(per_qant)

    for index, row in chosen.iterrows():
      traced_row, (func_name, cold_dur, warm_dur, mem) = iat_trace_row(index, row, function_id, args.duration)
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
  print("warm_pct, max_mem, max_running, mean_running, running_75th, running_90th")
  print(*run_trace_csv(trace_save_pth, 0.80, metadata_save_pth))
