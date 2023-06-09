import argparse, os
import matplotlib as mpl
import matplotlib.patches as mpatches
mpl.rcParams.update({'font.size': 14})
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
mpl.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

"""
A script to plot simple results from the output of the co-located `combine-logs.py` script

MUST BE RUN ON THE SAME MACHINE AS THE ORIGINAL DATA GATHERING
"""

with open("/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj", "r") as f:
  max_rapl_uj = int(f.read())

argparser = argparse.ArgumentParser()
argparser.add_argument("--logs-folder", '-l', help="The folder worker logs are stored in", required=True, type=str)
args = argparser.parse_args()

csv = os.path.join(args.logs_folder, "combined-energy-output.csv")
df = pd.read_csv(csv)

load_type = df.columns[-1]

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

hands = []

hands.append(*ax.plot(df.index, df["ipmi"], label="ipmi", color='blue'))
ax2 = ax.twinx()
hands.append(*ax2.plot(df.index, df[load_type], label=load_type, color='red'))

ax.set_title("IPMI reported wattage as load increases")
ax2.set_ylabel("{} being used".format(load_type))
ax2.yaxis.label.set_color('red')
ax.yaxis.label.set_color('blue')
ax.set_ylabel("Wattage")
ax.set_xlabel("Time (sec)")
# ax.set_xticklabels(labels, rotation = 90)
# plt.legend(handles=hands, labels=['IPMI Watts', 'CPUs'])
save_fname = os.path.join(args.logs_folder, "{}-ipmi-load.png".format(load_type))
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)

################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

hands = []

rapl_data = []
for i in range(1, len(df["rapl_uj"])):
  left = df["rapl_uj"][i-1]
  right = df["rapl_uj"][i]
  if right < left:
    uj = right + (max_rapl_uj - left)
  else:
    uj = right - left
  rapl_data.append(uj)

rapl_data.append(rapl_data[-1])

hands.append(*ax.plot(df.index, rapl_data, color='blue'))
ax2 = ax.twinx()
hands.append(*ax2.plot(df.index, df[load_type], color='red'))

ax.set_title("Rapl reported uJ as load increases")
ax.set_yscale('log')
ax2.set_ylabel("{} being used".format(load_type))
ax2.yaxis.label.set_color('red')
ax.yaxis.label.set_color('blue')
ax.set_ylabel("used uJ")
ax.set_xlabel("Time (sec)")
# ax.set_xticklabels(labels, rotation = 90)
# plt.legend(handles=hands, labels=['IPMI Watts', 'CPUs'])
save_fname = os.path.join(args.logs_folder, "{}-rapl-load.png".format(load_type))
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)


################################################################
fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

hands = []

hands.append(*ax.plot(df.index, df["perf_stat"], color='blue'))
ax2 = ax.twinx()
hands.append(*ax2.plot(df.index, df[load_type], color='red'))

ax.set_title("Perf reported Joules as load increases")
ax2.set_ylabel("{} being used".format(load_type))
ax2.yaxis.label.set_color('red')
ax.yaxis.label.set_color('blue')
ax.set_ylabel("Joules")
ax.set_xlabel("Time (sec)")
# ax.set_xticklabels(labels, rotation = 90)
# plt.legend(handles=hands, labels=['IPMI Watts', 'CPUs'])
save_fname = os.path.join(args.logs_folder, "{}-perf-load.png".format(load_type))
plt.savefig(save_fname, bbox_inches="tight")
plt.close(fig)
