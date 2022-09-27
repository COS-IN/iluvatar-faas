
import os, sys
from collections import defaultdict
import argparse
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.patches as mpatches
import json
mpl.rcParams.update({'font.size': 14})
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.use('Agg')
import matplotlib.pyplot as plt
import multiprocessing as mp
from dataset import join_day_one, ecdf, interpolate_ecdf

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o', help="The folder to store the output csv files into", required=True)
argparser.add_argument("--data-path", '-d', help="The folder where the azure dataset has been downloaded to", required=True)
argparser.add_argument("--num-funcs", '-n', type=int, help="Number of functions to sample for the trace", required=True)
args = argparser.parse_args()

df = join_day_one(args.data_path, False, True)

def plot_ecdf(row):
  name, row = row
  name = row["HashFunction"]
  xs, ys = ecdf(row)
  if len(xs) < 10 or np.std(xs) > 0:
    return

  fig, ax = plt.subplots()
  # interp_xs, interp_ys = interpolate_ecdf(xs, ys)
  interp_xs, interp_ys = xs, ys

  ax.plot(interp_xs, interp_ys, marker="<", markerfacecolor='none')
  ax.set_xscale('log')
  plt.savefig(f"{args.out_folder}/{name}.png", bbox_inches="tight")
  plt.close(fig)

with mp.Pool() as pool:
  print("starting plotting")
  pool.map(plot_ecdf, df.iterrows())

