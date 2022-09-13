import numpy as np
import argparse, os
from math import ceil

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o', help="The folder to store the output csv files into", required=True)
args = argparser.parse_args()

# Function durations should be proportional to iat. So f1 is small/short fn, f4 is largest one, etc.
# everything is seconds
def per_fn(name, iat, n):
  """
  name: function name
  iat: inter-arrival-time in seconds
  n: number of invocations
  """
  iats = np.random.exponential(scale=iat, size=n)
  times = np.cumsum(iats)
  # convert to milliseconds
  out = [(ceil(time * 1000), name) for time in times]
  return out

f1 = per_fn(1, 5, 300)
f2 = per_fn(2, 10, 200)
f3 = per_fn(3, 20, 100)
f4 = per_fn(4, 30, 50)

allf = f1 + f2 + f3 + f4

allf.sort()
function_metadata = []
# hello
function_metadata.append(('f1', 400, 10, 512, 1))
# chameleon
function_metadata.append(('f2', 400, 40, 512, 2))
# pyaes
function_metadata.append(('f3', 700, 300, 512, 3))
# cnn image classification
function_metadata.append(('f4', 4000, 1000, 512, 4))

trace_save_pth = os.path.join(args.out_folder, "four-functions.csv")
with open(trace_save_pth, "w") as f:
  f.write("{},{}\n".format("function_id", "invoke_time_ms"))
  for time_ms, function_id in allf:
    f.write("{},{}\n".format(function_id, time_ms))

metadata_save_pth = os.path.join(args.out_folder, "four-functions-metadata.csv")
with open(metadata_save_pth, "w") as f:
  f.write("{},{},{},{},{}\n".format("func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb", "function_id"))
  for (func_name, cold_dur, warm_dur, mem, function_id) in function_metadata:
    f.write("{},{},{},{},{}\n".format(func_name, cold_dur, warm_dur, mem, function_id))