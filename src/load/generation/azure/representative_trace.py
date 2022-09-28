from dataset import join_day_one, real_trace_row
import os
import argparse
from contextlib import suppress

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o', help="The folder to store the output csv files into", required=True)
argparser.add_argument("--data-path", '-d', help="The folder where the azure dataset has been downloaded to", required=True)
argparser.add_argument("--num-funcs", '-n', type=int, help="Number of functions to sample for the trace", required=True)
argparser.add_argument("--force", '-f', action='store_true', help="Overwrite an existing trace that has the same number of functions")
argparser.add_argument("--min-start", '-s', type=int, help="The minute to start the trace at", default=60)
argparser.add_argument("--min-end", '-e', type=int, help="The minute to end the trace at", default=120)
args = argparser.parse_args()

dataset = join_day_one(args.data_path, args.force)

quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]

per_qant = args.num_funcs // (len(quantiles)-1)
qts = dataset["total_invocations"].quantile(quantiles)
trace = []
function_metadata = []
metadata_save_pth = os.path.join(args.out_folder, "metadata-{}.csv".format(args.num_funcs))
with suppress(FileExistsError):
    os.makedirs(args.out_folder)

if not os.path.exists(metadata_save_pth) or args.force:
  func_name = 0
  for i in range(4):
    low = qts.iloc[i]
    high = qts.iloc[i+1]
    choose_from = dataset[dataset["total_invocations"].between(low, high)]
    chosen = choose_from.sample(per_qant)

    for index, row in chosen.iterrows():
      traced_row, (func_name, cold_dur, warm_dur, mem) = real_trace_row(index, row, func_name, args.min_start, args.min_end)
      trace += traced_row
      function_metadata.append((func_name, cold_dur, warm_dur, mem, func_name))
      func_name += 1

  out_trace = sorted(trace, key=lambda x:x[1]) #(func_name, time_ms)
  print(args.num_funcs, len(out_trace))

  trace_save_pth = os.path.join(args.out_folder, "{}.csv".format(args.num_funcs))
  with open(trace_save_pth, "w") as f:
    f.write("{},{}\n".format("func_name", "invoke_time_ms"))
    for func_name, time_ms in out_trace:
      f.write("{},{}\n".format(func_name, time_ms))

  with open(metadata_save_pth, "w") as f:
    f.write("{},{},{},{},{}\n".format("func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb", "func_name"))
    for (func_name, cold_dur, warm_dur, mem, func_name) in function_metadata:
      f.write("{},{},{},{},{}\n".format(func_name, cold_dur, warm_dur, mem, func_name))

  print("done", trace_save_pth)
