import os, os.path, json
import numpy as np
from collections import defaultdict
import argparse
import matplotlib as mpl

mpl.use("Agg")
mpl.rcParams.update({"font.size": 14})
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter

parser = argparse.ArgumentParser()
parser.add_argument('-d', "--duration", default=120, type=int, help="Per-scaling, in seconds")
args = parser.parse_args()

base = "results"
plot_data = defaultdict(list)
for server in os.listdir(base):
    server_pth = os.path.join(base, server)
    if os.path.isdir(server_pth):
        for procs in os.listdir(server_pth):
            procs_pth = os.path.join(server_pth, procs)
            file = os.path.join(procs_pth, f"{procs}.json")
            invoke_cnt = 0
            overheads = []
            with open(file) as f:
                data = json.load(f)
                for thread in data:
                    for response in thread["data"]:
                        # {"worker_response":{"json_result":"{\"body\": "
                        #  "{\"greeting\": \"Hello TESTING from python!\", \"cold\": true, \"start\": 1739799273.068233, "
                        #  "\"end\": 1739799273.0682342, \"latency\": 1.1920928955078125e-06}}","success":true,"duration_us":142215,
                        #  "compute":1,"container_state":3},"function_output":{"body":{"cold":true,"start":1739799273.068233,"end":1739799273.0682342,
                        #  "latency":1.1920928955078125e-6}},"client_latency_us":143160,"function_name":"scaling-1","function_version":"0",
                        #  "tid":"0-76b12f3a2d0d14fa68e9c9b7a6be72ee","invoke_start":"2025-02-17 08:34:32.926188699"},
                        try:
                            exec_time_sec = float(json.loads(response["worker_response"]["json_result"])["body"]["latency"])
                        except Exception as e:
                            print(file, response["tid"], response["worker_response"])
                            # raise  e
                        e2e_sec = float(response["client_latency_us"]) / 1_000.0
                        overheads.append(e2e_sec-exec_time_sec)
                    invoke_cnt += len(thread["data"])
            plot_data[server].append((int(procs), invoke_cnt / args.duration, np.mean(overheads), np.std(overheads)))

def server_to_leg(server):
    if server == "http":
        return "HTTP Server"
    elif server == "unix":
        return "UNIX Socket"
    else:
        raise Exception(f"Unknown server {server}")

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 5)

labels = []
for server in plot_data.keys():
    sever_data = sorted(plot_data[server], key=lambda x: x[0])
    xs, invokes, mean_over, std_over = zip(*sever_data)
    ax.plot(xs, invokes, label=server_to_leg(server))

ax.legend()
ax.set_xticks(xs)
ax.set_xticklabels(list(map(str, xs)))
ax.set_ylabel("Invokes per Sec")
ax.set_xlabel("Num Procs")
plt.savefig(os.path.join(base, "invoke_scaling.png"), bbox_inches="tight")
plt.close(fig)

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 5)

labels = []
for server in plot_data.keys():
    sever_data = sorted(plot_data[server], key=lambda x: x[0])
    xs, invokes, mean_over, std_over = zip(*sever_data)
    ax.errorbar(xs, mean_over, yerr=std_over, label=server_to_leg(server))

ax.legend()
ax.set_xticks(xs)
ax.set_xticklabels(list(map(str, xs)))
ax.set_yscale('log')
plt.tick_params(axis='y', which='minor')
ax.yaxis.set_minor_formatter(FormatStrFormatter("%.1f"))
ax.set_ylabel("Avg. Overhead (ms.)")
ax.set_xlabel("Num Procs")
plt.savefig(os.path.join(base, "overheads.png"), bbox_inches="tight")
plt.close(fig)
