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

f1 = per_fn("f1", 1, 300)
f2 = per_fn("f2", 3, 200)
f3 = per_fn("f3", 8, 100)
f4 = per_fn("f4", 15, 50)

allf = f1 + f2 + f3 + f4

allf.sort()
function_metadata = []
# hello
function_metadata.append(('f1', 400, 10, 512))
# chameleon
function_metadata.append(('f2', 400, 40, 512))
# pyaes
function_metadata.append(('f3', 700, 300, 512))
# cnn image classification
function_metadata.append(('f4', 4000, 1000, 512))

trace_save_pth = os.path.join(args.out_folder, "four-functions.csv")
with open(trace_save_pth, "w") as f:
  f.write("{},{}\n".format("func_name", "invoke_time_ms"))
  for time_ms, func_name in allf:
    f.write("{},{}\n".format(func_name, time_ms))

metadata_save_pth = os.path.join(args.out_folder, "four-functions-metadata.csv")
with open(metadata_save_pth, "w") as f:
  f.write("{},{},{},{}\n".format("func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb"))
  for (func_name, cold_dur, warm_dur, mem) in function_metadata:
    f.write("{},{},{},{}\n".format(func_name, cold_dur, warm_dur, mem))
