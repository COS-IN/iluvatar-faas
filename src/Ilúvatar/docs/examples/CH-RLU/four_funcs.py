import numpy as np
import argparse, os
from math import ceil
import random
import string

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

names = {}
def func_name(f):
    if f in names:
        return names[f]
    letters = string.ascii_lowercase
    name = "".join(random.choices(letters,k=20))
    name = f"{f}-{name}"
    names[f] = name
    return names[f]

all_invokes = []
function_metadata = []
def random_func():
    global all_invokes
    name = f"f{len(function_metadata)}"
    name = func_name(name)
    r = random.randint(0,3)
    if r == 0:
        # hello
        all_invokes += per_fn(name, 3, 200)
        function_metadata.append((name, 400, 10, 512))
    elif r == 1:
        # chameleon
        function_metadata.append((name, 400, 40, 512))
        all_invokes += per_fn(name, 3, 200)
    elif r == 2:
        # pyaes
        function_metadata.append((name, 700, 300, 512))
        all_invokes += per_fn(name, 8, 100)
    elif r == 3:
        # cnn image classification
        function_metadata.append((name, 4000, 1000, 512))
        all_invokes += per_fn(name, 15, 50)


for _ in range(50):
    random_func()
all_invokes = sorted(all_invokes, key=lambda x: x[0])

trace_save_pth = os.path.join(args.out_folder, "four-functions.csv")
with open(trace_save_pth, "w") as f:
    f.write("{},{}\n".format("func_name", "invoke_time_ms"))
    for time_ms, func_name in all_invokes:
        f.write("{},{}\n".format(func_name, time_ms))

metadata_save_pth = os.path.join(args.out_folder, "four-functions-metadata.csv")
with open(metadata_save_pth, "w") as f:
    f.write("{},{},{},{}\n".format("func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb"))
    for (func_name, cold_dur, warm_dur, mem) in function_metadata:
        f.write("{},{},{},{}\n".format(func_name, cold_dur, warm_dur, mem))
