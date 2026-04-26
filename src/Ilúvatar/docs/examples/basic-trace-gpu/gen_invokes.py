import numpy as np
import argparse, os
from math import ceil

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o', help="The folder to store the output csv files into", required=True)
args = argparser.parse_args()

# Function durations should be proportional to iat. So f1 is small/short fn, f4 is largest one, etc.
# everything is seconds
def per_fn(name, iat, n, start):
  """
  name: function name
  iat: inter-arrival-time in seconds
  n: number of invocations
  """
  iats = np.random.exponential(scale=iat, size=n)
  times = start+np.cumsum(iats)
  # convert to milliseconds
  out = [(ceil(time * 1000), name) for time in times]
  return out

function_metadata = []
allf = []

start = 0
NUM_FUNCS=32
for i in range(NUM_FUNCS):
    name = f"cupy-{i}"
    allf += per_fn(name, 4, 3, start / 1000)
    start = allf[-1][0]
    f_args="size=8000;iters=50"
    function_metadata.append(f"{name},5000,1000,2048,docker.io/alfuerst/cupy-iluvatar-gpu-unix:latest,GPU,DOCKER,3072,,UnixSocket")

for i in range(NUM_FUNCS):
    name = f"cupy-{i}"
    allf += per_fn(name, 4, 3, start / 1000)
    start = allf[-1][0]

for i in range(NUM_FUNCS):
    name = f"cupy-{i}"
    allf += per_fn(name, 4, 3, start / 1000)
    start = allf[-1][0]

allf.sort()

trace_save_pth = os.path.join(args.out_folder, "in.csv")
with open(trace_save_pth, "w") as f:
  f.write("{},{}\n".format("func_name", "invoke_time_ms"))
  for time_ms, func_name in allf:
    f.write("{},{}\n".format(func_name, time_ms))

metadata_save_pth = os.path.join(args.out_folder, "meta.csv")
with open(metadata_save_pth, "w") as f:
  f.write("func_name,cold_dur_ms,warm_dur_ms,mem_mb,image_name,compute,isolation,memory,args,server\n")
  for meta_str in function_metadata:
    f.write("{}\n".format(meta_str))
