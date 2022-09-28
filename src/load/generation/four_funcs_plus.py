import numpy as np
import matplotlib.pyplot as plt
import argparse, os
from math import ceil

argparser = argparse.ArgumentParser()
argparser.add_argument("--out_folder", '-o', help="The folder to store the output csv files and plot into.", required=True)
argparser.add_argument("--duration", '-d', help="Total duration for which trace is required in mins.", required=True)
argparser.add_argument("--invoc", '-i', help="Invocations per second", required=True)
argparser.add_argument("--type", '-t', help="type of trace required", choices=["random-exp", "even", "singular"], required=True, type=str, default="singular")
args = argparser.parse_args()

# Function durations should be proportional to iat. So f1 is small/short fn, f4 is largest one, etc.
# everything is seconds
def per_fn(name, iat, n, base=0):
  """
  name: function name
  iat: inter-arrival-time in milli seconds
  n: number of invocations
  """
  if args.type == "random-exp":
    iats = np.random.exponential(scale=float(iat), size=int(n))
  elif args.type == "even" or args.type == "singular":
      iats = [ float(iat) for i in range(int(n)) ]
        
  times = np.cumsum(iats)
  # convert to milliseconds
  out = [(ceil(time+base), name) for time in times]
  return out

total_duration = int(args.duration)*60*1000
invocations_per_sec = int(args.invoc)
total_funcs = 4

iat_base = (1 / invocations_per_sec)
iats = [int(iat_base*i*1000) for i in range(1,5)]
iats[0] = (iat_base*1*1000)/1.2 # hack to increase the number of invocations for first function 

funcs = []

if args.type == "random-exp" or args.type == "even":
    funcs = [per_fn(i+1, iats[i], total_duration/iats[i]) for i in range(0,len(iats))]
elif args.type == "singular":
    timeperiod = total_duration/(4*3*2) # 4 unique functions, 3 iterations, 2 portions(invocation,noinvocation)
    
    # repeat for four unique functions
    for fi in range(4):
        start_base = timeperiod*(fi*2*3)
        
        tfuncs = []

        # repeat for three cycles
        for ci in range(3):
            cstart_base = start_base + timeperiod*ci*2
            # generate function invocation for given time period
            # add base time to each invocation time 
            tfuncs.extend( per_fn( fi+1, iats[fi], timeperiod/iats[fi], cstart_base ) )

            # no function invocation for another time period 
        funcs.append( tfuncs )

    # print( funcs )
    
else:
    raise Exception("Trace type not recognized")

allf = []

get_maxidx = lambda alist, m: [ n for n,i in enumerate(alist) if i > m ][0]

for i in range(0,len(funcs)):
  times, name = zip(*funcs[i])
  times = [times[i]/1000 for i in range(len(times))]
  y = np.ones(len(times))*(1+i)
  section = get_maxidx( times, 5 )
  # print(section)
  # print(times)
  plt.scatter( times[0:section], y[0:section], label="f-"+str(i+1), marker=".")
  allf += funcs[i]

plt.ylabel("invocation of a function each index separate function")
plt.xlabel("Time in seconds")
plt.legend()
plt.savefig( os.path.join(args.out_folder, "funcs_first_five_seconds.jpg") )

plt.close()

for i in range(0,len(funcs)):
  times, name = zip(*funcs[i])
  y = np.ones(len(times))*(1+i)
  plt.scatter( times, y, label="f-"+str(i+1), marker=".")

plt.ylabel("invocation of a function each index separate function")
plt.xlabel("Time in seconds")
plt.legend()
plt.savefig( os.path.join(args.out_folder, "funcs_all.jpg") )

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
  f.write("{},{}\n".format("func_name", "invoke_time_ms"))
  for time_ms, func_name in allf:
    f.write("{},{}\n".format(func_name, time_ms))

metadata_save_pth = os.path.join(args.out_folder, "four-functions-metadata.csv")
with open(metadata_save_pth, "w") as f:
  f.write("{},{},{},{},{}\n".format("func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb"))
  for (func_name, cold_dur, warm_dur, mem) in function_metadata:
    f.write("{},{},{},{},{}\n".format(func_name, cold_dur, warm_dur, mem))

with open(os.path.join(args.out_folder, "stats.log"), "w") as f:
    dur = allf[-1][0]/(1000*60)
    ips = int( len(allf) / (allf[-1][0]/1000) ) 
    ipsf = [ len(funcs[i])/(allf[-1][0]/1000) for i in range(0,4) ]
    stats = f"""
            Total Invocations: {len(allf)}\n
            Total duration: {dur} mins\n
            Average Invocation per second: {ips}\n"""
    stats += "Average Invocation per second for functions:\n"
    for i in range(0,4):
        stats += f"  Func-{i+1} - {ipsf[i]}\n"
    f.write(stats)
