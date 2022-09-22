import argparse
import json

"""
Find the number of invocation requests that never completed
They likely timed out waiting on something
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--log", '-l', required=True)
args = argparser.parse_args()

open_tids = {}
completed = set()
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
    if log["fields"]["message"] == "Handling invocation request":
      open_tids[log["fields"]["tid"]] = True
    if log["fields"]["message"] == "Invocation complete":
      tid = log["fields"]["tid"]
      if tid not in open_tids:
        print(f"tid {tid} was completed without being started")
      del open_tids[tid]
      completed.add(tid)

print("Hanging invocations:", len(open_tids))
print("Completed invocations:", len(completed))