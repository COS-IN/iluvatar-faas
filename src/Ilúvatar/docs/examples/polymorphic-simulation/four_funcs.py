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

f1 = per_fn("cupy-1", 1, 300)
f2 = per_fn("cupy-2", 3, 300)
f3 = per_fn("onnx-roberta-1", 8, 300)
f4 = per_fn("onnx-roberta-2", 15, 300)

allf = f1 + f2 + f3 + f4

allf.sort()
function_metadata = []
function_metadata.append(('cupy-1', 3000, 1200, 2048))
function_metadata.append(('cupy-2', 3000, 1200, 2048))
function_metadata.append(('onnx-roberta-1', 1000, 300, 1024))
function_metadata.append(('onnx-roberta-2', 1000, 300, 2048))

trace_save_pth = os.path.join(args.out_folder, "four-functions.csv")
with open(trace_save_pth, "w") as f:
  f.write("{},{}\n".format("func_name", "invoke_time_ms"))
  for time_ms, func_name in allf:
    f.write("{},{}\n".format(func_name, time_ms))

metadata_save_pth = os.path.join(args.out_folder, "four-functions-metadata.csv")
COMPUTE="CPU|GPU"
COMPUTE="GPU"
ISOLATION="CONTAINERD|DOCKER"
with open(metadata_save_pth, "w") as f:
  f.write("{},{},{},{},{},{}\n".format("func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb", "compute", "isolation"))
  for (func_name, cold_dur, warm_dur, mem) in function_metadata:
    f.write("{},{},{},{},{},{}\n".format(func_name, cold_dur, warm_dur, mem, COMPUTE, ISOLATION))
