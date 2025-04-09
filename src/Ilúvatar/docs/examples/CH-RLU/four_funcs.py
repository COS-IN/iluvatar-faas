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
def per_fn(name, iat_ms):
    """
    name: function name
    iat: inter-arrival-time in milliseconds
    """
    iats = np.random.exponential(scale=iat_ms, size=1000)
    times = np.cumsum(iats)
    MAX_TIME = 10*60*1000
    while times[-1] <= MAX_TIME:
        iats = np.random.exponential(scale=iat_ms, size=1000) + iats[-1]
        addtl_times = np.cumsum(iats)
        times += addtl_times
    times = list(filter(lambda x: x <= MAX_TIME, times))
    # convert to milliseconds
    out = [(ceil(time), name) for time in times]
    return out

def func_name(f):
    letters = string.ascii_lowercase
    name = "".join(random.choices(letters,k=20))
    name = f"{f}-{name}"
    return name

all_invokes = []
function_metadata = []
def random_func(scale=1.0):
    global all_invokes
    r = random.randint(0,3)
    if r == 0:
        # hello
        name = func_name("hello")
        all_invokes += per_fn(name, 300*scale)
        function_metadata.append((name, 400, 10, 512))
    elif r == 1:
        # chameleon
        name = func_name("chameleon")
        function_metadata.append((name, 400, 40, 512))
        all_invokes += per_fn(name, 120*scale)
    elif r == 2:
        # pyaes
        name = func_name("pyaes")
        function_metadata.append((name, 700, 300, 512))
        all_invokes += per_fn(name, 200*scale)
    elif r == 3:
        # cnn image classification
        name = func_name("cnn_image_classification")
        function_metadata.append((name, 4000, 1000, 512))
        all_invokes += per_fn(name, 250*scale)


for _ in range(40):
    random_func()
for _ in range(2):
        random_func(0.5)
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
