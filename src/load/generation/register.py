import argparse
from collections import defaultdict
import json
import subprocess
import os
from time import time
import numpy as np
import pickle

argparser = argparse.ArgumentParser()
argparser.add_argument("--clipth", '-c', default="/home/alex/repos/efaas/src/Ilúvatar/target/debug/iluvatar_worker_cli", required=False)
argparser.add_argument("--worker", '-w', default="local", required=False)

args = argparser.parse_args()

actions = os.listdir("../functions/python3/functions")

def register(args, version, dir):
  proc_args = [args.clipth, "--worker", args.worker, "--config", "/home/alex/repos/efaas/src/Ilúvatar/worker_cli/src/worker_cli.json", "register", "--name", dir, "--version", version, "--memory", "512", "--cpu", "1", "--image", image]
  cli = subprocess.run(args=proc_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  if cli.returncode != 0:
    print(cli.stderr)
    cli.check_returncode()

def invoke(args, version, dir):
  proc_args = [args.clipth, "--worker", args.worker, "--config", "/home/alex/repos/efaas/src/Ilúvatar/worker_cli/src/worker_cli.json", "invoke", "--name", dir, "--version", version]
  start = time()
  cli = subprocess.run(args=proc_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  end = time()
  duration = end - start
  if cli.returncode != 0:
    print(cli.stderr)
    cli.check_returncode()
  return cli, duration


colds = defaultdict(list)
warms = defaultdict(list)
colds_over = defaultdict(list)
warms_over = defaultdict(list)

for dir in actions:
  # if dir in ["cnn_image_classification", "video_processing", "model_training", "image_processing", "json_dumps_loads"]:
  #   continue
  print(dir)
  image = "docker.io/alfuerst/{}-iluvatar-action:latest".format(dir)
  for i in range(10):
    version = "0.0.{}".format(i)
    register(args, version, dir)
    for _ in range(3):
      cli, duration = invoke(args, version, dir)
      try:
        output = json.loads(cli.stdout)
      except:
        print(cli.stdout)
        continue
      if "Error" in output:
        print(output["Error"])
        continue
      else:
        if "body" in output and "latency" in output["body"]:
          lat = output["body"]["latency"]
          overhead = duration - lat 
          if "cold" in output["body"]:
            if bool(output["body"]["cold"]):
              colds[dir].append(duration)
              colds_over[dir].append(overhead)
            else:
              warms[dir].append(duration)
              warms_over[dir].append(overhead)
        else:
          print(output)

pctl = 0.3
# function name, mean warn time, mean cold time, mean warm system overhead, mean cold system overhead
print("Name, Warm, Cold, WarmOverhead, ColdOverhead")
for k in set(colds.keys()).union(set(warms.keys())):
  warm_m = 0
  cold_m = 0
  warm_mo = 0
  cold_mo = 0
  if k in colds:
    cold_m = np.quantile(colds[k], pctl)
    cold_mo = np.quantile(colds_over[k], pctl)
  if k in warms:
    warm_m = np.quantile(warms[k], pctl)
    warm_mo = np.quantile(warms_over[k], pctl)
  print(k, warm_m, cold_m, warm_mo, cold_mo)

data = (colds, colds_over, warms, warms_over)
with open("data.pckl", "w+b") as f:
  pickle.dump(data, f)