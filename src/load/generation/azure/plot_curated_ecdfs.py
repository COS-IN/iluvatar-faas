
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
from dataset import join_day_one, ecdf

"""
Plot the ECDFs of a few specially curated functions
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--data-path", '-d', help="The folder where the azure dataset has been downloaded to", required=True)
args = argparser.parse_args()

df = join_day_one(args.data_path, force=False, debug=True)

functions = ["0a0fb019e32b06095378171cc091223d3456c87badd55156f93a668e919cfca7",
"1d6068092b0b3d0ff9e214242ac2678bd147313b03b1ae641eca88286c8ebbf3",
"3ef5de7d89a7336bcbf7273c387b4094cafa203cd618440d8ca2adeac8e21c75",
"9cfbc77f8131488e0261499dde8c0bb67a6c8460b28230918bd06ba63695afdc",
"45ce273e28e1910770ead3770647a92b058206cc91c5574ee10afa1cc158086d",
"33429bc85e5fc1a07053b627f7055c0d37300da4db44aaa857299627ac3e9b5c",
"a9783835cbd68a2e234cf861799cf840df9ce4055cca238f04978125c187a0cc",
"ce5f8cab397532fec35076960d07b43868222da16d5c3e67fef1d2b7e11b26c6",
"db8aebcc4103b79b973d3cdb82c0eb8eedbdad9c08773beb51d09fa40479e8b9",
"e81ba93f8513b173c53d51521c4569f0c5e160c2dc9ed7fd2cefddcb81dc4d76",
"771db0dde8fb0d2121086b7c686f564444a6ed2a9aec805b9a4849a7d5cd425f",
"f282dab639947c11df7328899c9c0dabbf532af1e22e97eca695a532554813f0",
"d74c3a3cb196fe5568444924a2f27abe1383c4eafa12a5f801e883779f899782",
"bc03a348453e61b1e27057ded9672d3e3903d8c0eb64b927cb4580e36ac3a962"]

def plot_ecdf(row):
  name, row = row
  name = row["HashFunction"]
  if name not in functions:
    return
  interp_xs, interp_ys, iats = ecdf(row)
  if len(interp_xs) < 10 or np.std(iats) == 0:
    return

  fig, ax = plt.subplots()

  ax.plot(interp_xs, interp_ys, marker="<", markerfacecolor='none')
  ax.set_xscale('log')
  out = os.path.join("curated_ecdfs", f"{name}.png")
  plt.savefig(out, bbox_inches="tight")
  plt.close(fig)

with mp.Pool() as pool:
  print("starting plotting")
  pool.map(plot_ecdf, df.iterrows())
