import argparse
import os
from string import whitespace
import pandas as pd
import numpy as np
from dateutil.parser import isoparse
import json
from datetime import datetime, timedelta, timezone

"""
Sanitize and make consistent the various log files output by the worker or energy monitor
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--logs-folder", '-l', help="The folder worker logs are stored in", required=True, type=str)
argparser.add_argument("--max-rapl", '-m', help="File that contains max energy range for rapl in uj", required=False, type=str)
args = argparser.parse_args()

energy_log = os.path.join(args.logs_folder, "energy-function.log")

if args.max_rapl is None:
  with open("/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj", "r") as f:
    max_rapl_uj = int(f.read())
else:
  with open(args.max_rapl, "r") as f:
    max_rapl_uj = int(f.read())

def prepare_perf_log(args):
  """
    Load a perf log into a dataframe
    The multiple reported metrics are each put into their own column
  """
  perf_log = os.path.join(args.logs_folder, "energy-perf.log")

  try:
    f = open(perf_log, 'r')
    first_line = f.readline()
    start_date = first_line[len("# started on "):].strip(whitespace)
    # 'Mon Aug 29 15:04:17 2022'
    start_date = datetime.strptime(start_date, "%a %b %d %H:%M:%S %Y")
  except FileNotFoundError as fnf:
      return

  # https://www.man7.org/linux/man-pages/man1/perf-stat.1.html#top_of_page
  # CSV FORMAT
  cols = ["timestamp", "perf_stat", "unit", "event_name", "counter_runtime", "pct_time_counter_running"]
  df = pd.read_csv(perf_log, skiprows=2, names=cols, usecols=[i for i in range(len(cols))])
  def time_to_ns(x) -> int:
    if np.isnan(x):
      raise Exception("Got a nan value instead of a real time!")
    new_date = start_date + timedelta(seconds=x)
    new_date = datetime.combine(new_date.date(), new_date.time(), None)
    # 2022-09-14 11:54:00.793313159
    return new_date.strftime("%Y-%m-%d %H:%M:%S.%f")
  df["timestamp"] = df["timestamp"].apply(lambda x: time_to_ns(x))
  
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

  df.to_csv(os.path.join(args.logs_folder, "perf.csv"), index=False)

def prepare_rapl_log(args):
  path = os.path.join(args.logs_folder, "energy-rapl.log")
  try:
      rapl_df = pd.read_csv(path)
  except FileNotFoundError:
      print(f"Warning: RAPL logs not available: {path}")
      return
  rapl_data = []
  for i in range(1, len(rapl_df["rapl_uj"])):
    left = int(rapl_df["rapl_uj"][i-1])
    right = int(rapl_df["rapl_uj"][i])
    if right < left:
      uj = right + (max_rapl_uj - left)
    else:
      uj = right - left
    rapl_data.append(uj)

  rapl_data.append(rapl_data[-1])
  rapl_df["rapl_uj_diff"] = pd.Series(rapl_data, index=rapl_df.index)

  rapl_df.to_csv(os.path.join(args.logs_folder, "rapl.csv"), index=False)

def prepare_ipmi_log(args):
  pass

def prepare_energy_log(df: pd.DataFrame) -> pd.DataFrame:
  cpu_data = []

  worker_log = os.path.join(args.logs_folder, "worker.log")
  if not os.path.exists(worker_log):
    worker_log = os.path.join(args.logs_folder, "worker_worker1.log")
  if not os.path.exists(worker_log):
    return

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
      if log["fields"]["message"] == "current load status":
        data = {}
        data["timestamp"] = log["timestamp"]
        json_log = json.loads(log["fields"]["status"])
        data["load_avg_1minute"] = json_log["load_avg_1minute"]
        if json_log["cpu_sy"] == None:
            # in a very rare case some entry might be none
            continue
        data["cpu_pct"] = int(json_log["cpu_sy"]) + int(json_log["cpu_us"]) + int(json_log["cpu_wa"])

        if "hardware_cpu_freqs" in json_log:
          data["hw_cpu_hz_mean"] = np.mean(json_log["hardware_cpu_freqs"])
          data["hw_cpu_hz_max"] = np.min(json_log["hardware_cpu_freqs"])
          data["hw_cpu_hz_min"] = np.max(json_log["hardware_cpu_freqs"])
          data["hw_cpu_hz_std"] = np.std(json_log["hardware_cpu_freqs"])
          for cpu in range(len(json_log["hardware_cpu_freqs"])):
            data["hw_cpu{}_hz".format(cpu)] = json_log["hardware_cpu_freqs"][cpu]

        if "kernel_cpu_freqs" in json_log:
          data["kern_cpu_hz_mean"] = np.mean(json_log["kernel_cpu_freqs"])
          data["kern_cpu_hz_max"] = np.min(json_log["kernel_cpu_freqs"])
          data["kern_cpu_hz_min"] = np.max(json_log["kernel_cpu_freqs"])
          data["kern_cpu_hz_std"] = np.std(json_log["kernel_cpu_freqs"])
          for cpu in range(len(json_log["kernel_cpu_freqs"])):
            data["kern_cpu{}_hz".format(cpu)] = json_log["kernel_cpu_freqs"][cpu]

        cpu_data.append(data)

  cpu_df = pd.DataFrame.from_records(cpu_data)
  cpu_df.to_csv(os.path.join(args.logs_folder, "cpu.csv"), index=False)

prepare_perf_log(args)
prepare_rapl_log(args)
prepare_ipmi_log(args)
prepare_energy_log(args)
