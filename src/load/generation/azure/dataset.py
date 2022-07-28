import os
import pandas as pd
import random
from math import ceil
import argparse

buckets = [str(i) for i in range(1, 1441)]

def join_day_one(datapath: str):
  # TODO: use all the files in the dataset
  durations_file = "function_durations_percentiles.anon.d01.csv"
  invocations_file = "invocations_per_function_md.anon.d01.csv"
  mem_fnames_file = "app_memory_percentiles.anon.d01.csv"

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
  return joined

def trace_row(func_name, row, function_id):
  secs_p_min = 60
  milis_p_sec = 1000
  trace = list()
  cold_dur = int(row["Maximum"])
  warm_dur = int(row["percentile_Average_25"])
  mem = int(row["divvied"])
  for minute, invocs in enumerate(row[buckets]):
    start = minute * secs_p_min * milis_p_sec
    if invocs == 0:
      continue
    elif invocs == 1:
      # if only one invocation, start randomly within that minute
      # avoid "thundering heard" of invocations at start of minute
      start_ms = random.randint(0, (secs_p_min * milis_p_sec)-1)
      trace.append((function_id, start+start_ms))
    else:
      every = (secs_p_min*milis_p_sec) / invocs
      trace += [(function_id, int(start + i*every)) for i in range(invocs)]

  return trace, (func_name, cold_dur, warm_dur, mem)

def divive_by_func_num(row, grouped_by_app):
    return ceil(row["AverageAllocatedMb"] / grouped_by_app[row["HashApp"]])

if __name__ == '__main__':
  argparser = argparse.ArgumentParser()
  argparser.add_argument("--out-folder", '-o')
  argparser.add_argument("--data-path", '-d')
  args = argparser.parse_args()
  store = args.out_folder

  joined = join_day_one(args.data_path)
