import argparse
import os
from string import whitespace
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

argparser = argparse.ArgumentParser()
argparser.add_argument("--logs-folder", '-l', help="The folder worker logs are stored in", required=True, type=str)
argparser.add_argument("--energy-freq-ms", '-q', help="The frequency at which energy readings were recorded, in milliseconds", required=True, type=int)
args = argparser.parse_args()

energy_log = os.path.join(args.logs_folder, "energy-function.log")
perf_log = os.path.join(args.logs_folder, "energy-perf.log")
bash_log = os.path.join(args.logs_folder, "bash-times.log")

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
  with open(path, 'r') as f:
    first_line = f.readline()
    start_date = first_line[len("# started on "):].strip(whitespace)
    # 'Mon Aug 29 15:04:17 2022'
    start_date = datetime.strptime(start_date, "%a %b %d %H:%M:%S %Y")
  energy_timestamp = energy_df["timestamp"][5]
  hour_diff = energy_timestamp.hour - start_date.hour

  # https://www.man7.org/linux/man-pages/man1/perf-stat.1.html#top_of_page
  # CSV FORMAT
  cols = ["timestamp", "perf_stat", "unit", "event_name", "counter_runtime", "pct_time_counter_running"]
  df = pd.read_csv(path, skiprows=2, names=cols, usecols=[i for i in range(len(cols))])
  def update_time(x) -> datetime:
    if type(x) is not float and x != float('nan'):
      raise Exception("Didnt get a float, got type({}) '{}'".format(type(x), x))
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
  energy_df.set_index(pd.DatetimeIndex(energy_df["timestamp"]))
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
    part_sec = part_sec[:-diff]
  done = "{} {}.{}{}".format(day, full, part_sec, tz)
  return datetime.fromisoformat(done)

def load_bash_df() -> pd.DataFrame:
  data = []
  with open(bash_log, 'r') as f:
    first_line = f.readline()
    _, load_type, _ = first_line.split(", ")
    while True:
      line = f.readline()
      if line == "":
        break

      action, type_amnt, bash_datetime = line.split(", ")
      type_amnt = int(type_amnt)
      # Fri 02 Sep 2022 04:16:30 PM UTC
      parsed_time = datetime.strptime(bash_datetime.strip(whitespace), "%a %d %b %Y %I:%M:%S %p %Z")
      parsed_time = datetime.combine(parsed_time.date(), parsed_time.time(), timezone.utc)

      if action == "start":
        data.append((parsed_time, type_amnt))
      if action == "stop":
        last_time, last_type_amt = data[-1]
        step = timedelta(seconds=1)
        running_t = last_time + step
        while parsed_time > running_t:
          data.append((running_t, last_type_amt))
          running_t += step
        data.append((parsed_time, type_amnt))
  df =  pd.DataFrame.from_records(data, index="timestamp", columns=["timestamp", load_type])
  return df, load_type

energy_df = load_energy_log(energy_log)
perf_df = load_perf_log(perf_log, energy_df)
full_df = energy_df.join(perf_df, lsuffix="_l", rsuffix="_r")
full_df.index = full_df["timestamp_l"]
bash_df, load_type = load_bash_df()
full_df = full_df.join(bash_df)
full_df[load_type].replace(np.NaN, 0.0, inplace=True)

outpath = os.path.join(args.logs_folder, "combined-energy-output.csv")
full_df.to_csv(outpath)
