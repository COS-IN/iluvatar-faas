import os
import os.path
import argparse
import json
import multiprocessing as mp
import subprocess
import numpy as np
from time import time

"""
Prewarm a bunch of containers to test worker container creation
"""

config = os.path.join(os.environ["HOME"], ".config/Ilúvatar/worker_cli.json")
argparser = argparse.ArgumentParser()
argparser.add_argument("--threads", '-t', required=True, help="number of concurrent clients to reach to", type=int)
argparser.add_argument("--worker", '-w', required=True, help="worker name", type=str)
argparser.add_argument("--cli", '-c', required=False, help="cli location", default="../../../Ilúvatar/target/release/iluvatar_worker_cli", type=str)
argparser.add_argument("--config", required=False, help="config location", default=config, type=str)
argparser.add_argument("--count", required=False, help="config location", default=1, type=int)

args = argparser.parse_args()

def register_fn(proc_id):
  start = time()
  subp_args = [args.cli, "--config", args.config, "--worker", args.worker, "register", \
    "--cpu", "1", "--memory", "128", "--name", str(proc_id), "--parallel-invokes", "1", "--image", \
    "docker.io/alfuerst/cnn_image_classification-iluvatar-action:latest"]
  proc = subprocess.run(subp_args, capture_output=True)
  # if proc.returncode != 0:
  out = json.loads(proc.stdout)
  if "Error" in out:
    raise Exception(out['Error'])
  return time() - start

def prewarm(proc_id, count):
  start = time()
  times = []
  for _ in range(count):
    prewarm_s = time()
    subp_args = [args.cli, "--config", args.config, "--worker", args.worker, "prewarm", "--name", str(proc_id), "--memory", "512"]
    proc = subprocess.run(subp_args)
    if proc.stdout is not None:
      out = json.loads(proc.stdout)
      if "Error" in out:
        return out['Error']
    prewarm_t = time() - prewarm_s
    times.append(prewarm_t)
  return times

func_id = 1
def gen_func_id():
  global func_id
  ret = func_id
  func_id += 1
  return ret

for t in range(args.threads, args.threads+1):
  pool = mp.Pool(t)
  ids = [gen_func_id() for _ in range(t)]

  # print("register")
  registers = pool.map(register_fn, ids)
  # print(registers)

  print("prewarm ", t)
  prewarm_args = [(i, args.count) for i in ids]
  res = pool.starmap(prewarm, prewarm_args)
  for arr in res:
    print(np.min(arr), np.mean(arr), np.max(arr))
