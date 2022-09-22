import os.path
import argparse
import json

"""
Find Container IDs for container locks that were acquired but never released
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--log", '-l', required=True)
args = argparser.parse_args()

taken_containers = {}

with open(args.log, 'r') as f:
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
    if log["fields"]["message"] == "Container acquired":
      cid = log["fields"]["container_id"]
      if cid in taken_containers:
        print("!! Container {} was locked twice !!".format(cid))
      size = log["fields"]["size"]
      i = log["fields"]["i"]
      print("container was {} / {}".format(i, size))
      taken_containers[cid] = (log["fields"]["tid"], size, i)
    if log["fields"]["message"] == "Dropping container lock":
      if log["fields"]["container_id"] not in taken_containers :
        continue
      del taken_containers[log["fields"]["container_id"]]

  print(len(taken_containers), taken_containers)