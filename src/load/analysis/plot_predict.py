import argparse
import os
from string import whitespace
import pandas as pd
import numpy as np
from dateutil.parser import isoparse
import json
from datetime import datetime, timedelta, timezone
import matplotlib as mpl
import matplotlib.patches as mpatches
mpl.rcParams.update({'font.size': 14})
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.use('Agg')
import matplotlib.pyplot as plt
import json
from dateutil.parser import isoparse
from dateutil import tz

"""
Sanitize and make consistent the various log files output by the worker or energy monitor
"""
def timestamp_to_pddate( time ):
  try:
      return pd.to_datetime( time )
  except:
      ptime = pd.to_datetime( time, format="%Y-%m-%d %H:%M:%S:%f+" )
      ptime = ptime.replace(tzinfo=tz.tzutc())
      local = ptime.astimezone(tz.tzlocal())
      local = local.replace(tzinfo=None)
      return local

argparser = argparse.ArgumentParser()
argparser.add_argument("--log-file", '-l', help="log file location", required=True, type=str)
argparser.add_argument("--power-cap", '-p', help="power cap level", required=True, type=float)
argparser.add_argument("--rapl-file", '-r', help="Rapl file location", required=False, type=str)
argparser.add_argument("--ipmi-file", '-i', help="IPMI file location", required=False, type=str)
args = argparser.parse_args()

if not ((args.ipmi_file is None) ^ (args.rapl_file is None)):
  print("Must have either IPMI or RAPL file!")
  exit(1)

data = []
for line in open(args.log_file).readlines():
    parsed = json.loads(line)
    if "fields" in parsed and "message" in parsed["fields"]:
        if "power cap check" in parsed["fields"]["message"]:
            parts = parsed["fields"]["message"].split(' ')
            power = expec_func_usage = None
            timestamp = timestamp_to_pddate(parsed["timestamp"])
            for p in parts:
                if p.startswith("j("):
                    expec_func_usage = float(p[len("j("):-1])
                if p.startswith("j_predicted(p("):
                    power = float(p[len("j_predicted(p("):-1])

            if power is not None and expec_func_usage is not None:
                data.append((timestamp, power, expec_func_usage))

cols = ["timestamp", "energy", "predicted"]
df = pd.DataFrame.from_records(data, columns=cols)
# print(df.describe())

if args.ipmi_file is not None:
  cols = ["timestamp", "ipmi"]
  df2 = pd.read_csv(args.ipmi_file, skiprows=1, names=cols)
  df2["energy"] = df2["ipmi"]
  df2["timestamp"] = df2["timestamp"].apply(timestamp_to_pddate)
elif args.rapl_file is None:
  cols = ["timestamp", "rapl_uj", "rapl_uj_diff"]
  df2 = pd.read_csv(args.rapl_file, skiprows=1, names=cols)
  df2 = df2[df2["rapl_uj_diff"] < 1000000]
  df2["energy"] = df2["rapl_uj_diff"] / 1_000_000.0

print(df2["timestamp"])
print(df["timestamp"])
df2 = df2[df2["timestamp"] < df["timestamp"].max()]
df2 = df2[df2["timestamp"] > df["timestamp"].min()]

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

ax.plot(df["timestamp"], df["predicted"], color='blue', label="predicted")
ax.plot(df["timestamp"], df["energy"], color='green', label="power reading (j)")
ax.plot(df["timestamp"], df["energy"] + df["predicted"], color='red', label="j_predicted + j")
ax.plot(df2["timestamp"], df2["energy"], color='orange', label="System Energy")

ax.axhline(args.power_cap, color='black', label="power cap")

# ax.set_title("Energy Cap")
ax.set_ylabel("Joules")
# ax.set_xlabel("Time (sec)")
ax.legend(loc=(1,0))
save_fname = os.path.join(os.path.dirname(args.log_file), "predict.png")
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)
