import argparse
import os
from string import whitespace
import pandas as pd
import numpy as np
from dateutil.parser import isoparse
import json
from datetime import datetime, timedelta, timezone

with open("/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj", "r") as f:
  max_rapl_uj = int(f.read())

argparser = argparse.ArgumentParser()
argparser.add_argument("--logs-folder", '-l', help="The folder worker logs are stored in", required=True, type=str)
argparser.add_argument("--energy-freq-ms", '-q', help="The frequency at which energy readings were recorded, in milliseconds", required=True, type=int)
argparser.add_argument("--max-rapl", '-m', help="File that contains man energy range for rapl in uj", default="none", type=str)
args = argparser.parse_args()

energy_log = os.path.join(args.logs_folder, "energy-function.log")
perf_log = os.path.join(args.logs_folder, "energy-perf.log")


if args.max_rapl == "none":
    with open("/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj", "r") as f:
      max_rapl_uj = int(f.read())
else:
    with open(args.max_rapl, "r") as f:
      max_rapl_uj = int(f.read())

def round_date(time: datetime) -> datetime:
  """
    Rounds date time up to the nearest "poll-dur-ms" from args
  """
  to_remove = time.microsecond % (args.energy_freq_ms * 1000)
  whole = time.microsecond / (args.energy_freq_ms * 1000)
  whole = round(whole) * (args.energy_freq_ms * 1000)
  new_date = time - timedelta(microseconds=to_remove) + timedelta(microseconds=whole)
  return datetime.combine(new_date.date(), new_date.time(), timezone.utc)

def load_perf_log(path: str, energy_df: pd.DataFrame) -> pd.DataFrame:
  """
    Load a perf log into a dataframe
    The multiple reported metrics are each put into their own column
  """

  try:
    f = open(path, 'r')
    first_line = f.readline()
    start_date = first_line[len("# started on "):].strip(whitespace)
    # 'Mon Aug 29 15:04:17 2022'
    start_date = datetime.strptime(start_date, "%a %b %d %H:%M:%S %Y")
  except FileNotFoundError as fnf:
      return

  energy_timestamp = energy_df["timestamp"][5]
  hour_diff = energy_timestamp.hour - start_date.hour

  # https://www.man7.org/linux/man-pages/man1/perf-stat.1.html#top_of_page
  # CSV FORMAT
  cols = ["timestamp", "perf_stat", "unit", "event_name", "counter_runtime", "pct_time_counter_running"]
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

  df_energy_pkg = df[df["event_name"] == "power/energy-pkg/"].copy()
  df_energy_ram = df[df["event_name"] == "power/energy-ram/"].copy()
  df_instructions = df[df["event_name"] == "inst_retired.any"].copy()

  df_energy_pkg.index = pd.DatetimeIndex(df_energy_pkg["timestamp"])
  df_energy_pkg.rename( columns = {"perf_stat":"energy_pkg"}, inplace=True )

  df_energy_ram.index = pd.DatetimeIndex(df_energy_ram["timestamp"])
  df_energy_ram.rename( columns = {"perf_stat":"energy_ram"}, inplace=True )

  df_instructions.index = pd.DatetimeIndex(df_instructions["timestamp"])
  df_instructions.rename( columns = {"perf_stat":"retired_instructions"}, inplace=True )

  df = df_energy_pkg.join(df_energy_ram["energy_ram"])
  df = df.join(df_instructions["retired_instructions"])

  df.set_index(pd.DatetimeIndex(df["timestamp"]), inplace=True)
  return df

def load_energy_log(path) -> pd.DataFrame:
  with open(path, 'r') as f:
    first_line = f.readline().strip(whitespace)
  cols = first_line.split(",")
  energy_df = pd.read_csv(path, usecols=cols, parse_dates=[1])
  energy_df["timestamp"] = energy_df["timestamp"].apply(lambda x: round_date(parse_energy_log_time(x)))
  # cumulative_running is every invocation that occured since the last timestep
  energy_df["cumulative_running"] = [[] for _ in range(len(energy_df))]
  # exact_running is those invocations that were ongoing when the energy sampling occured
  energy_df["exact_running"] = [[] for _ in range(len(energy_df))]
  energy_df.set_index(pd.DatetimeIndex(energy_df["timestamp"]), inplace=True)

  rapl_data = []
  for i in range(1, len(energy_df["rapl_uj"])):
    left = int(energy_df["rapl_uj"][i-1])
    right = int(energy_df["rapl_uj"][i])
    if right < left:
      uj = right + (max_rapl_uj - left)
    else:
      uj = right - left
    rapl_data.append(uj)

  rapl_data.append(rapl_data[-1])
  energy_df["rapl_uj_diff"] = pd.Series(rapl_data, index=energy_df.index)

  return energy_df

def parse_energy_log_time(time) -> datetime:
  """
  The timestamps created by the Ilúvatar energy log can have too-precise fraction seconds
  Clean them up with this helper
  """
  if type(time) is not str:
    raise Exception("Didnt get a string, got type({}) '{}'".format(type(time), time))
  day, hr, tz = time.split(" ")
  full, part_sec = hr.split('.')
  diff  = len(part_sec) - 6
  if diff > 0:
    # too many significant digits
    part_sec = part_sec[:-diff]
  elif diff < 0:
    # too few significant digits
    part_sec += "0"*abs(diff)
  done = "{} {}.{}{}".format(day, full, part_sec, tz)
  try:
    return datetime.fromisoformat(done)
  except Exception as e:
    print(e)
    print(time)
    print(done)
    exit(1)

def inject_running_into_df(df: pd.DataFrame) -> pd.DataFrame:
  INVOKE_TARGET = "iluvatar_worker_library::services::containers::containerd::containerdstructs"
  INVOKE_NAME = "ContainerdContainer::invoke"  
  running = {}
  cpu_data = []
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
    for item in match.itertuples():
      item.exact_running.append(fqdn)

    micro_t = args.energy_freq_ms * 1000
    rounded_start_t = round_date(start_t)
    rounded_end_t = round_date(end_t)
    while rounded_start_t <= rounded_end_t:
      match = df[df["timestamp"] == rounded_start_t]
      if len(match) > 1:
        raise Exception("Got multiple matches on a single timestamp!!", rounded_start_t)
      for item in match.itertuples():
        item.cumulative_running.append(fqdn)
      rounded_start_t += timedelta(microseconds=micro_t)

  worker_log = os.path.join(args.logs_folder, "worker.log")
  if not os.path.exists(worker_log):
    worker_log = os.path.join(args.logs_folder, "worker_worker1.log")

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
      if log["fields"]["message"] == "current load status":
        log_t = round_date(timestamp(log))
        data = {}
        data["timestamp"] = log_t
        json_log = json.loads(log["fields"]["status"])
        data["load_avg_1minute"] = json_log["load_avg_1minute"]
        data["cpu_pct"] = int(json_log["cpu_sy"]) + int(json_log["cpu_us"]) + int(json_log["cpu_wa"])

        data["hw_cpu_hz_mean"] = np.mean(json_log["hardware_cpu_freqs"])
        data["hw_cpu_hz_max"] = np.min(json_log["hardware_cpu_freqs"])
        data["hw_cpu_hz_min"] = np.max(json_log["hardware_cpu_freqs"])
        data["hw_cpu_hz_std"] = np.std(json_log["hardware_cpu_freqs"])

        data["kern_cpu_hz_mean"] = np.mean(json_log["kernel_cpu_freqs"])
        data["kern_cpu_hz_max"] = np.min(json_log["kernel_cpu_freqs"])
        data["kern_cpu_hz_min"] = np.max(json_log["kernel_cpu_freqs"])
        data["kern_cpu_hz_std"] = np.std(json_log["kernel_cpu_freqs"])
        cpu_data.append(data)

  cpu_df = pd.DataFrame.from_records(cpu_data, index="timestamp")
  cpu_df = cpu_df.resample("1s").mean().interpolate()
  df = df.join(cpu_df, lsuffix="", rsuffix="_cpu")

  return df

energy_df = load_energy_log(energy_log)
perf_df = load_perf_log(perf_log, energy_df)
if perf_df is not None:
  full_df = energy_df.join(perf_df, lsuffix="", rsuffix="_perf")
else:
  full_df = energy_df

full_df = inject_running_into_df(full_df)
full_df.fillna(method='ffill', inplace=True)
full_df.fillna(method='bfill', inplace=True)
full_df.fillna(value=0, inplace=True)

# print(full_df)
outpath = os.path.join(args.logs_folder, "combined-energy-output.csv")
outpath_pkl = os.path.join(args.logs_folder, "combined-energy-output.pickle")
full_df.to_csv(outpath)
full_df.to_pickle( outpath_pkl )
