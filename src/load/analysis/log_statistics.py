import argparse
import json
from collections import defaultdict

"""
Calculates and prints the number of each log message in the given log file
And their contribution to the file size
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--file", '-f', help="The worker log file", required=True, type=str)
args = argparser.parse_args()

counts = defaultdict(int)
sums = defaultdict(int)

with open(args.file, 'r') as f:
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
    counts[log["fields"]["message"]] += 1
    sums[log["fields"]["message"]] += len(line)

total_size = sum(sums.values())
for k,v in sorted(counts.items(), key=lambda x: x[1], reverse=True):
  print(k, v, "{:.1f}%".format((float(sums[k])/float(total_size))*100))
