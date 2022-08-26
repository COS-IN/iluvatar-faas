import argparse
import os
import pandas as pd
from dateutil.parser import isoparse
import json
from datetime import datetime

argparser = argparse.ArgumentParser()
argparser.add_argument("--logs-folder", '-l', help="The folder worker logs are stored in", required=True, type=str)
args = argparser.parse_args()

energy_log = os.path.join(args.logs_folder, "energy-function.log")
# TODO: put perf log information in here too
perf_log = os.path.join(args.logs_folder, "energy-perf.log")
worker_log = os.path.join(args.logs_folder, "worker.log")

def parse_energy_log_time(time):
  """
  The timestamps created by the Ilúvatar energy log can have too-precise fraction seconds
  Clean them up with this helper
  """
  day, hr, tz = time.split(" ")
  full, part_sec = hr.split('.')
  diff  = len(part_sec) - 6
  if diff > 0:
    part_sec = part_sec[:-diff]
  done = "{} {}.{}{}".format(day, full, part_sec, tz)
  return datetime.fromisoformat(done)

energy_df = pd.read_csv(energy_log, usecols=["timestamp","rapl_uj","ipmi"], parse_dates=[1])
energy_df["timestamp"] = energy_df["timestamp"].apply(lambda x: parse_energy_log_time(x))
energy_df["running"] = [[] for _ in range(len(energy_df))]

INVOKE_TARGET = "iluvatar_worker_library::services::containers::containerd::containerdstructs"
INVOKE_NAME = "ContainerdContainer::invoke"

def timestamp(log):
  return isoparse(log["timestamp"])

def invoke_start(log):
  fqdn = log["span"]["fqdn"]
  tid = log["span"]["tid"]
  t = timestamp(log)
  running[tid] = (t, fqdn)

def invoke_end(log):
  tid = log["span"]["tid"]
  stop_t = timestamp(log)
  start_t, fqdn = running[tid]
  duration = stop_t - start_t
  return duration, start_t, stop_t

def insert_to_df(df, start_t, end_t, fqdn):
  match = df[df["timestamp"].between(start_t, end_t)]
  for _, item in match.iterrows():
    item[3].append(fqdn)

running = {}

with open(worker_log, 'r') as f:
  while True:
    line = f.readline()
    if line == "":
      break
    try:
      log = json.loads(line)
    except Exception as e:
      print(e)
      print(line)
      exit(1)
    if log["fields"]["message"] == "new" and log["target"] == INVOKE_TARGET and log["span"]["name"] == INVOKE_NAME:
      # invocation starting
      invoke_start(log)
    if log["fields"]["message"] == "close" and log["target"] == INVOKE_TARGET and log["span"]["name"] == INVOKE_NAME:
      # invocation concluded
      fqdn = log["span"]["fqdn"]
      duration, start_t, stop_t = invoke_end(log)
      insert_to_df(energy_df, start_t, stop_t, fqdn)

print(energy_df)