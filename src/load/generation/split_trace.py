import os
import random
import argparse
import multiprocessing as mp

"""
Split a trace into per-function individual traces
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o', help="The folder to store the output files and subfolders into", required=True)
argparser.add_argument("--trace-path", '-t', help="The original trace", required=True)
argparser.add_argument("--metadata-path", '-m', help="The original metadata", required=True)
args = argparser.parse_args()

with open(args.trace_path, 'r') as trace_f:
  trace_header = trace_f.readline()
  trace = trace_f.readlines()

with open(args.metadata_path, 'r') as meta:
  meta_header = meta.readline()
  metadata = meta.readlines()

for m in metadata:
  func_name = m.split(",")[0]
  directory = os.path.join(args.out_folder, func_name)
  os.makedirs(directory, exist_ok=True)
  out_pth = os.path.join(directory, "trace.csv")
  func_trace = [trace_header]
  for t in trace:
    if t.split(",")[0] == func_name:
      # print(m, t)
      func_trace.append(t)
  with open(out_pth, 'w') as out:
    out.writelines(func_trace)

  meta_out_pth = os.path.join(directory, "metadata.csv")
  with open(meta_out_pth, 'w') as meta_out:
    meta_out.writelines([meta_header, m])
