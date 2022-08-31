import argparse
import os
from string import whitespace
import pandas as pd
import numpy as np
from dateutil.parser import isoparse
import json
from datetime import datetime, timedelta, timezone

argparser = argparse.ArgumentParser()
argparser.add_argument("--logs-folder", '-l', help="The folder worker logs are stored in", required=True, type=str)
argparser.add_argument("--energy-freq-ms", '-q', help="The frequency at which energy readings were recorded, in milliseconds", required=True, type=int)
args = argparser.parse_args()

energy_log = os.path.join(args.logs_folder, "energy-function.log")
perf_log = os.path.join(args.logs_folder, "energy-perf.log")
worker_log = os.path.join(args.logs_folder, "worker.log")

def round_date(time: datetime) -> datetime:
  """
    Rounds date time to the nearest "poll-dur-ms" from args
  """
  to_remove = time.microsecond % (args.energy_freq_ms * 1000)
  whole = time.microsecond / (args.energy_freq_ms * 1000)
  whole = round(whole) * (args.energy_freq_ms * 1000)
  new_date = time - timedelta(microseconds=to_remove) + timedelta(microseconds=whole)
  return datetime.combine(new_date.date(), new_date.time(), timezone.utc)

def load_perf_log(path: str, energy_df: pd.DataFrame) -> pd.DataFrame:
  with open(path, 'r') as f:
    first_line = f.readline()
    start_date = first_line[len("# started on "):].strip(whitespace)
    # 'Mon Aug 29 15:04:17 2022'
    start_date = datetime.strptime(start_date, "%a %b %d %H:%M:%S %Y")
  energy_timestamp = energy_df["timestamp"][5]
  hour_diff = energy_timestamp.hour - start_date.hour

  # https://www.man7.org/linux/man-pages/man1/perf-stat.1.html#top_of_page
  # CSV FORMAT
  cols = ["timestamp", "perf_watts", "unit", "event_name", "counter_runtime", "pct_time_counter_running"]
  df = pd.read_csv(path, skiprows=2, names=cols, usecols=[i for i in range(len(cols))])
  def update_time(x) -> datetime:
    if np.isnan(x):
      raise Exception("Got a nan value instead of a real time!")
    new_date = start_date + timedelta(seconds=x)
    # perf does not send datetime with timezone
    # must manually correct by computing difference from UTC in energy log
    new_date = new_date + timedelta(hours=hour_diff)
    return datetime.combine(new_date.date(), new_date.time(), timezone.utc)
  df["timestamp"] = df["timestamp"].apply(lambda x: round_date(update_time(x)))
  df.set_index(pd.DatetimeIndex(df["timestamp"]))
  return df

def load_energy_log(path) -> pd.DataFrame:
  with open(path, 'r') as f:
    first_line = f.readline().strip(whitespace)
  cols = first_line.split(",")
  energy_df = pd.read_csv(path, usecols=cols, parse_dates=[1])
  energy_df["timestamp"] = energy_df["timestamp"].apply(lambda x: round_date(parse_energy_log_time(x)))
  energy_df["running"] = [[] for _ in range(len(energy_df))]
  energy_df.set_index(pd.DatetimeIndex(energy_df["timestamp"]))
  return energy_df

def parse_energy_log_time(time) -> datetime:
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

def inject_running_into_df(df: pd.DataFrame) -> pd.DataFrame:
  INVOKE_TARGET = "iluvatar_worker_library::services::containers::containerd::containerdstructs"
  INVOKE_NAME = "ContainerdContainer::invoke"  
  running = {}
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
    match = df[df["timestamp_l"].between(start_t, end_t)]
    for item in match.itertuples():
      item.running.append(fqdn)

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
        insert_to_df(df, start_t, stop_t, fqdn)
  return df

energy_df = load_energy_log(energy_log)
perf_df = load_perf_log(perf_log, energy_df)
full_df = energy_df.join(perf_df, lsuffix="_l", rsuffix="_r")
full_df = inject_running_into_df(full_df)

print(full_df)
outpath = os.path.join(args.logs_folder, "combined-energy-output.csv")
full_df.to_csv(outpath)
