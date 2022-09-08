import numpy as np
import matplotlib.pyplot as plt
import argparse, os
from math import ceil

argparser = argparse.ArgumentParser()
argparser.add_argument("--out_folder", '-o', help="The folder to store the output csv files and plot into.", required=True)
argparser.add_argument("--duration", '-d', help="Total duration for which trace is required in mins.", required=True)
argparser.add_argument("--invoc", '-i', help="Invocations per second", required=True)
args = argparser.parse_args()

# Function durations should be proportional to iat. So f1 is small/short fn, f4 is largest one, etc.
# everything is seconds
def per_fn(name, iat, n):
  """
  name: function name
  iat: inter-arrival-time in milli seconds
  n: number of invocations
  """
  iats = np.random.exponential(scale=float(iat), size=int(n))
  times = np.cumsum(iats)
  # convert to milliseconds
  out = [(ceil(time), name) for time in times]
  return out

total_duration = int(args.duration)*60*1000
invocations_per_sec = int(args.invoc)
total_funcs = 4

iat_base = (1 / invocations_per_sec)
iats = [int(iat_base*i*1000) for i in range(1,5)]

funcs = [per_fn(i+1, iats[i], total_duration/iats[i]) for i in range(0,len(iats))]

allf = []

get_maxidx = lambda alist, m: [ n for n,i in enumerate(alist) if i > m ][0]

for i in range(0,len(funcs)):
  times, name = zip(*funcs[i])
  times = [times[i]/1000 for i in range(len(times))]
  y = np.ones(len(times))*(1+i)
  section = get_maxidx( times, 5 )
  print(section)
  #print(times)
  plt.scatter( times[0:section], y[0:section], label="f-"+str(i+1), marker=".")
  allf += funcs[i]

plt.ylabel("invocation of a function each index separate function")
plt.xlabel("Time in seconds")
plt.legend()
plt.savefig( os.path.join(args.out_folder, "funcs.jpg") )

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

with open(os.path.join(args.out_folder, "stats.log"), "w") as f:
    dur = allf[-1][0]/(1000*60)
    ips = int( len(allf) / (allf[-1][0]/1000) ) 
    stats = f"""
            Total Invocations: {len(allf)}\n
            Total duration: {dur} mins\n
            Average Invocation per second: {ips}\n"""
    f.write(stats)
