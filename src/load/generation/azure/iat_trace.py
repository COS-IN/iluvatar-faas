from dataset import join_day_one, iat_trace_row, write_trace
from trace_analyze import run_trace_csv
import os
import argparse
from contextlib import suppress
from multiprocessing import Pool

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o', help="The folder to store the output csv files into", required=True)
argparser.add_argument("--data-path", '-d', help="The folder where the azure dataset has been downloaded to", required=True)
argparser.add_argument("--num-funcs", '-n', type=int, help="Number of functions to sample for the trace", required=False)
argparser.add_argument("--force", '-f', action='store_true', help="Overwrite an existing trace that has the same number of functions")
argparser.add_argument("--scale", type=float, help="Scale function IATs by this number. Number < 1 means smaller IATs and higher invocation count", default=1.0)
argparser.add_argument("--fname", action="extend", nargs="+", type=str, help="Function names to take from trace instead of random sampling")
argparser.add_argument("--duration", type=int, help="The length in minutes of the trace", default=60)
args = argparser.parse_args()

if args.num_funcs is not None:
  num_funcs = args.num_funcs
if args.fname is not None:
  num_funcs = len(args.fname)
if (args.num_funcs is None and args.fname is None) or (args.num_funcs is not None and args.fname is not None):
  print("Must supply either a number of functions `--num-funcs`, or funcitons names `--fname`")
  exit(1)

dataset = join_day_one(args.data_path, args.force, iats=True)
# dataset = dataset[dataset["IAT_std"] != 0.0]
dataset = dataset[dataset["dur_iat_ratio"] < 4]

quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]

per_qant = num_funcs // (len(quantiles)-1)
qts = dataset["IAT_mean"].quantile(quantiles)
trace = []
function_metadata = []
metadata_save_pth = os.path.join(args.out_folder, "metadata-{}.csv".format(num_funcs))
with suppress(FileExistsError):
    os.makedirs(args.out_folder)

if not os.path.exists(metadata_save_pth) or args.force:
  if args.num_funcs is not None:
    for i in range(4):
      low = qts.iloc[i]
      high = qts.iloc[i+1]
      choose_from = dataset[dataset["IAT_mean"].between(low, high)]
      chosen = choose_from.sample(per_qant)

      for index, row in chosen.iterrows():
        traced_row, (func_name, cold_dur, warm_dur, mem, mean_iat) = iat_trace_row(index, row, args.duration, args.scale)
        trace += traced_row
        function_metadata.append((func_name, cold_dur, warm_dur, mem, mean_iat))
  else:
    for fname in args.fname:
      index, row = list(dataset[dataset["HashFunction"].str.startswith(fname)].iterrows())[0]
      traced_row, (func_name, cold_dur, warm_dur, mem, mean_iat) = iat_trace_row(index, row, args.duration, args.scale)
      trace += traced_row
      function_metadata.append((func_name, cold_dur, warm_dur, mem, mean_iat))

  # print(function_metadata)
  # exit()
  out_trace = sorted(trace, key=lambda x:x[1]) #(func_name, time_ms)
  trace_save_pth = os.path.join(args.out_folder, "{}.csv".format(num_funcs))

  write_trace(out_trace, function_metadata, trace_save_pth, metadata_save_pth)

  print("done", trace_save_pth)
  print("warm_pct, max_mem, max_running, mean_running, running_75th, running_90th")
  print(*run_trace_csv(trace_save_pth, 0.80, metadata_save_pth))
  print(*run_trace_csv(trace_save_pth, 0.90, metadata_save_pth))
  print(*run_trace_csv(trace_save_pth, 0.99, metadata_save_pth))
  print(*run_trace_csv(trace_save_pth, 0.999, metadata_save_pth))
