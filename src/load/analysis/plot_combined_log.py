import argparse
import os.path
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.patches as mpatches
mpl.rcParams.update({'font.size': 14})
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.use('Agg')
import matplotlib.pyplot as plt

"""
Plot some basic information and associations from a combined worker-energy log pickle file
"""

argparser = argparse.ArgumentParser()
argparser.add_argument("--log-file", '-l', help="The combined log pickle file", required=True, type=str)
args = argparser.parse_args()

df = pd.read_pickle(args.log_file)

folder = os.path.dirname(args.log_file)

def plot_cpu_speed(metric: str):
  if metric not in df.columns:
    return
  fig, ax = plt.subplots()
  plt.tight_layout()
  fig.set_size_inches(5, 3)

  # sampled_df = df[["kern_cpu_hz_mean","kern_cpu_hz_std"]].rolling(10).mean()
  # sampled_df = df.iloc[::5, :]
  sampled_df = df

  ax.plot(range(len(df)), df[metric], label=metric, color='blue')
  ax2 = ax.twinx()
  ax2.errorbar(range(len(sampled_df)), sampled_df["kern_cpu_hz_mean"]/ 1000000, label="kern_cpu_hz_mean", color='red', yerr=sampled_df["kern_cpu_hz_std"]/ 1000000)
  # ax2.errorbar(df.index, df ["kern_cpu_hz_mean"]/ 1000000, label="kern_cpu_hz_mean", color='red')

  ax.set_title("{} reported vs CPU speed".format(metric))
  ax2.set_ylabel("mean GHz")
  ax2.yaxis.label.set_color('red')
  ax.yaxis.label.set_color('blue')
  ax.set_ylabel("Wattage")
  ax.set_xlabel("Time (sec)")
  save_fname = os.path.join(folder, "proc-speed-vs-{}.png".format(metric))
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

def plot_cpu_pct(metric: str):
  if metric not in df.columns:
    return
  fig, ax = plt.subplots()
  plt.tight_layout()
  fig.set_size_inches(5, 3)

  # sampled_df = df[["kern_cpu_hz_mean","kern_cpu_hz_std"]].rolling(10).mean()
  # sampled_df = df.iloc[::5, :]
  sampled_df = df

  ax.plot(range(len(df)), df[metric], label=metric, color='blue')
  ax2 = ax.twinx()
  ax2.plot(range(len(sampled_df)), sampled_df["cpu_pct"], label="cpu_pct", color='red')

  ax.set_title("{} reported vs CPU %".format(metric))
  ax2.set_ylabel("CPU %")
  ax2.yaxis.label.set_color('red')
  ax.yaxis.label.set_color('blue')
  ax.set_ylabel("Wattage")
  ax.set_xlabel("Time (sec)")
  save_fname = os.path.join(folder, "cpu-pct-vs-{}.png".format(metric))
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

def plot_poly_fits():
  metrics = ["energy_pkg", "energy_ram", "ipmi", "rapl_uj_diff"]
  associations = ['cumulative_running', 'exact_running', 'retired_instructions', 'load_avg_1minute', 'cpu_pct', 'hw_cpu_hz_mean',
                   'hw_cpu_hz_max', 'kern_cpu_hz_mean', 'kern_cpu_hz_max']
  all_cols = metrics + associations
  subset = df[all_cols].copy()
  subset['cumulative_running'] = subset["cumulative_running"].apply(lambda x: len(x))
  subset['exact_running'] = subset["exact_running"].apply(lambda x: len(x))

  cor_df = subset.corr(method = "pearson")#.round(2)

  ##########################################################################
  # Energy usages correlated with system metrics

  wanted = cor_df[metrics].values[len(metrics):]
  (x_size, y_size) = wanted.shape
  fig, ax = plt.subplots(figsize = (x_size+2, y_size+2))
  plt.tight_layout()

  cax = ax.imshow(wanted, interpolation='nearest', cmap = 'Reds', vmin = 0, vmax = 1)
  
  ax.set_xticks(ticks = range(len(metrics)))
  ax.set_xticklabels(metrics)
  ax.set_yticks(ticks = range(len(associations)))
  ax.set_yticklabels(associations)
                
  ax.tick_params(axis = "x", labelsize = 12, labelrotation = 90)
  ax.tick_params(axis = "y", labelsize = 12, labelrotation = 0)
  fig.colorbar(cax).ax.tick_params(labelsize = 12)
  for (x, y), t in np.ndenumerate(wanted):
      ax.annotate("{:.2f}".format(t),
                  xy = (y,x), # these are reversed for some reason
                  va = "center",
                  ha = "center")

  ax.set_title("Correlations with system performance metrics with energy usage")
  save_fname = os.path.join(folder, "metric-correlations.png")
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)

  ##########################################################################
  # Energy usages correlated with each other

  wanted = cor_df[metrics].values[:len(metrics)]
  fig, ax = plt.subplots(figsize = wanted.shape)
  plt.tight_layout()

  cax = ax.imshow(wanted, interpolation='nearest', cmap = 'Reds', vmin = 0, vmax = 1)
  
  ax.set_xticks(ticks = range(len(metrics)))
  ax.set_xticklabels(metrics)
  ax.set_yticks(ticks = range(len(metrics)))
  ax.set_yticklabels(metrics)
                
  ax.tick_params(axis = "x", labelsize = 12, labelrotation = 90)
  ax.tick_params(axis = "y", labelsize = 12, labelrotation = 0)
  fig.colorbar(cax).ax.tick_params(labelsize = 12)
  for (x, y), t in np.ndenumerate(wanted):
      ax.annotate("{:.2f}".format(t),
                  xy = (x, y),
                  va = "center",
                  ha = "center")

  ax.set_title("Correlations with Energy usage with each other")
  save_fname = os.path.join(folder, "energy-correlations.png")
  plt.savefig(save_fname, bbox_inches="tight")
  plt.close(fig)


plot_poly_fits()
for metric in ["energy_pkg", "energy_ram", "ipmi", "rapl_uj_diff"]:
  plot_cpu_speed(metric)
  plot_cpu_pct(metric)
