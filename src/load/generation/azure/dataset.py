import os
import os.path
import pandas as pd
import numpy as np
from math import ceil
import argparse
import multiprocessing as mp

buckets = [str(i) for i in range(1, 1441)]

def compute_row_iat(row) -> float:
  if len(row) == 2:
    index, row = row
  iats = []
  last_t = -1
  for min, count in enumerate(row[buckets]):
    time_ms = min * 1000
    if count == 0:
      continue
    elif count == 1:
      if last_t == -1:
        last_t = time_ms
        continue

      diff = time_ms - last_t
      iats.append(diff)
      last_t = time_ms
    else:
      if last_t == -1:
        last_t = time_ms

      sep = 1000.0 / float(count)
      # print(sep)
      for i in range(count):
        diff = (time_ms + i*sep) - last_t
        if diff == 0:
          continue
        iats.append(diff)
        last_t = (time_ms + i*sep)
      # print(iats)
      # print(min, count)
      # print(np.mean(iats))
      # exit(0)
      # continue
  if len(iats) < 1:
    print(iats, sum(row[buckets]))
    exit(1)
  return np.mean(iats), np.std(iats), len(iats)

def join_day_one(datapath: str, force: bool):
  # TODO: use all the files in the dataset
  durations_file = "function_durations_percentiles.anon.d01.csv"
  invocations_file = "invocations_per_function_md.anon.d01.csv"
  mem_fnames_file = "app_memory_percentiles.anon.d01.csv"
  outfile = os.path.join(datapath,"joined_d01_trace.csv")

  if not os.path.exists(outfile) or force:
    file = os.path.join(datapath, durations_file)
    durations = pd.read_csv(file)
    durations.index = durations["HashFunction"]
    durations = durations.drop_duplicates("HashFunction")

    group_by_app = durations.groupby("HashApp").size()

    file = os.path.join(datapath, invocations_file)
    invocations = pd.read_csv(file)
    invocations = invocations.dropna()
    invocations.index = invocations["HashFunction"]
    sums = invocations[buckets].sum(axis=1)
    invocations["total_invocations"] = sums
    invocations = invocations[sums > 1] # action must be invoked at least twice
    invocations = invocations.drop_duplicates("HashFunction")

    p = mp.Pool()
    iat_data = p.map(compute_row_iat, invocations.iterrows())
    invocations["IAT_mean"] = list(map(lambda x: x[0], iat_data))
    invocations["IAT_std"] = list(map(lambda x: x[1], iat_data))
    invocations["IAT_cnt"] = list(map(lambda x: x[2], iat_data))

    joined = invocations.join(durations, how="inner", lsuffix='', rsuffix='_durs')

    file = os.path.join(datapath, mem_fnames_file)
    memory = pd.read_csv(file)
    memory = memory.drop_duplicates("HashApp")
    memory.index = memory["HashApp"]

    # memory is tabulated per _application_, but invocations a per-function
    # distribute the memory evenly between all functions in an application
    new_mem = memory.apply(lambda x: divive_by_func_num(x, group_by_app), axis=1, raw=False, result_type='expand')
    memory["divvied"] = new_mem

    joined = joined.join(memory, how="inner", on="HashApp", lsuffix='', rsuffix='_mems')

    # prevent 0 duration invocations, don't know why they're in the dataset
    joined = joined[joined["Maximum"]>0]
    joined = joined[joined["percentile_Average_25"]>0]

    joined["dur_iat_ratio"] = joined["percentile_Average_25"] / joined["IAT_mean"]
    joined.to_csv(outfile)

    return joined
  else:
    df = pd.read_csv(outfile)
    # print(df["dur_iat_ratio"].describe(percentiles=[0.8, 0.9, 0.95, 0.99]))
    return df

def iat_trace_row(func_name, row, function_id, duration_min:int):
  """
  Create invocations for the function using the function's IAT
  """
  secs_p_min = 60
  milis_p_sec = 1000
  trace = list()
  cold_dur = int(row["Maximum"])
  warm_dur = int(row["percentile_Average_25"])
  mean = float(row["IAT_mean"])
  std = float(row["IAT_std"])
  mem = int(row["divvied"])
  rng = np.random.default_rng(None)
  time = 0
  end_ms = duration_min * secs_p_min * milis_p_sec
  while time < end_ms:
    sample = rng.normal(loc=mean, scale=std)
    while sample < 0:
      sample = rng.normal(loc=mean, scale=std)
    time += sample
    trace.append( (function_id, time) )

  # print(function_id, mean, std, len(trace))
  return trace, (func_name, cold_dur, warm_dur, mem)


def real_trace_row(func_name, row, function_id, min_start=0, min_end=1440):
  """
  Create invocations for the function using the exact invocation times of the function from the trace
  """
  secs_p_min = 60
  milis_p_sec = 1000
  trace = list()
  cold_dur = int(row["Maximum"])
  warm_dur = int(row["percentile_Average_25"])
  mem = int(row["divvied"])
  for minute, invocs in enumerate(row[buckets[min_start:min_end]]):
    start = minute * secs_p_min * milis_p_sec
    if invocs == 0:
      continue
    elif invocs == 1:
      # if only one invocation, start randomly within that minute
      # avoid "thundering heard" of invocations at start of minute
      start_ms = np.random.randint(0, (secs_p_min * milis_p_sec)-1)
      trace.append((function_id, start+start_ms))
    else:
      every = (secs_p_min*milis_p_sec) / invocs
      trace += [(function_id, int(start + i*every)) for i in range(invocs)]

  return trace, (func_name, cold_dur, warm_dur, mem)

def divive_by_func_num(row, grouped_by_app):
    return ceil(row["AverageAllocatedMb"] / grouped_by_app[row["HashApp"]])

if __name__ == '__main__':
  argparser = argparse.ArgumentParser()
  argparser.add_argument("--out-folder", '-o', required=True)
  argparser.add_argument("--data-path", '-d', required=True)
  args = argparser.parse_args()
  store = args.out_folder

  joined = join_day_one(args.data_path)
  # for idx, row in joined.iterrows():
    # trace, data = iat_trace_row(idx, row, 0, 10)
    # print(data)
    # print(trace)
    # break