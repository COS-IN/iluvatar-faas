import os.path
import argparse

"""
Make a trace one-quarter the intensity
Removes 3/4 of invocations
"""


argparser = argparse.ArgumentParser()
argparser.add_argument("--file", '-f', required=True)
args = argparser.parse_args()

path, fname = os.path.split(args.file)
new_fname = fname.split(".")[0] + "-shrunk.csv"
shrunk_f = os.path.join(path, new_fname)

with open(args.file, 'r') as infile:

  with open(shrunk_f, 'w') as outfile:
    outfile.write(infile.readline())
    for i, line in enumerate(infile.readlines()):
      if i % 4 == 0:
        outfile.write(line)
