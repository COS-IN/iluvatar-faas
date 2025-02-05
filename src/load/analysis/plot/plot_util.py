import os, sys
from ..log_parser import WorkerLogParser
import numpy as np
import pandas as pd
import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt

mpl.rcParams.update({"font.size": 14})
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42


def tick_formatter(resolution):
    def tick(x, pos) -> str:
        fmt = str(x)
        if resolution == "ns":
            t = 1_000_000_000.0 * 60.0  # * 1000.0
            fmt = "{:.1f}".format(x / t)
        elif resolution == "us":
            t = 1_000_000.0 * 60.0  # * 1000.0
            fmt = "{:.1f}".format(x / t)
        else:
            raise Exception(f"Unknown time resolution '{resolution}'")
        # print(resolution, fmt, x, pos)
        return fmt

    return tick


def plot_util(log: WorkerLogParser):
    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)
    ax2 = ax.twinx()
    cmap = mpl.colormaps["tab10"].colors
    to_use_colors = list(cmap)

    start_t = log.status_df[
        (log.status_df["num_running"] > 0)
        | (log.status_df["cpu_queue_len"] > 0)
        | (log.status_df["gpu_queue_len"] > 0)
    ]["norm_time"].min()
    status_df = log.status_df[log.status_df["norm_time"] >= start_t]

    ax.plot(
        status_df["norm_time"],
        status_df["cpu_util"],
        label="CPU %",
        color=to_use_colors.pop(),
    )
    ax.plot(
        status_df["norm_time"],
        status_df["gpu_util"],
        label="GPU %",
        color=to_use_colors.pop(),
    )
    ax.plot(
        status_df["norm_time"],
        status_df["num_running"],
        label="Running",
        color=to_use_colors.pop(),
    )

    ax2.plot(
        status_df["norm_time"],
        status_df["cpu_queue_len"],
        label="CPU Enqueued",
        linestyle="--",
        color=to_use_colors.pop(),
    )
    ax2.plot(
        status_df["norm_time"],
        status_df["gpu_queue_len"],
        label="GPU Enqueued",
        linestyle="--",
        color=to_use_colors.pop(),
    )

    resolution = status_df["norm_time"].max().resolution_string

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, ncols=3, loc=(-0.2, 1))

    ax2.set_ylabel("Queue len")
    ax.xaxis.set_major_formatter(tick_formatter(resolution))
    ax.set_ylabel("Device Util")
    ax.set_xlabel("Time (min)")
    save_fname = os.path.join(log.source, f"utilization.png")
    plt.savefig(save_fname, bbox_inches="tight")
    plt.close(fig)


def plot_device_data(log: WorkerLogParser):
    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)
    ax2 = ax.twinx()
    cmap = mpl.colormaps["tab10"].colors
    to_use_colors = list(cmap)

    start_t = log.status_df[
        (log.status_df["num_running"] > 0)
        | (log.status_df["cpu_queue_len"] > 0)
        | (log.status_df["gpu_queue_len"] > 0)
    ]["norm_time"].min()
    status_df = log.status_df[log.status_df["norm_time"] >= start_t]

    ax2.plot(
        status_df["norm_time"],
        status_df["gpu_load"],
        label="GPU Load",
        color=to_use_colors.pop(),
        linestyle="--",
    )
    ax2.plot(
        status_df["norm_time"],
        status_df["gpu_load_avg"],
        label="GPU Load Avg",
        color=to_use_colors.pop(),
        linestyle="--",
    )

    ax2.plot(
        status_df["norm_time"],
        status_df["cpu_load"],
        label="CPU Load",
        color=to_use_colors.pop(),
        linestyle="--",
    )
    ax2.plot(
        status_df["norm_time"],
        status_df["cpu_load_avg"],
        label="CPU Load Avg",
        color=to_use_colors.pop(),
        linestyle="--",
    )

    ax.plot(
        status_df["norm_time"],
        status_df["gpu_tput"] / 2,
        label="Norm. GPU Tput",
        color=to_use_colors.pop(),
    )
    ax.plot(
        status_df["norm_time"],
        status_df["cpu_tput"] / 48,
        label="Norm. CPU Tput",
        color=to_use_colors.pop(),
    )

    min, max = ax2.get_ylim()
    if min == 0:
        min = 1
    if len(str(max / min)) > 3:
        ax2.set_yscale("symlog")
    ax.set_yscale("log")

    resolution = status_df["norm_time"].max().resolution_string

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.set_ylabel("Queue Tput")
    ax.legend(h1 + h2, l1 + l2, ncols=3, loc=(-0.2, 1))
    ax.xaxis.set_major_formatter(tick_formatter(resolution))
    ax2.set_ylabel("Queue Load & Load Avg")
    ax.set_xlabel("Time (min)")
    save_fname = os.path.join(log.source, f"device_data.png")
    plt.savefig(save_fname, bbox_inches="tight")
    plt.close(fig)
