import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from load.run.run_trace import rust_build, run_live, RunTarget, BuildTarget, make_host_queue
import multiprocessing as mp

# build the solution
build_level = BuildTarget.RELEASE
rust_build(ILU_HOME, None, build_level)
SSH_Q = make_host_queue([("gpu1", "v-gpu1.victor.futuresystems.org"),("gpu3", "v-gpu3.victor.futuresystems.org")])

def run_exp(running:int, queue):
    from load.run.run_trace import RunTarget, RunType
    CORES = 48
    MEMORY = 120*1024
    results_dir = os.path.join(os.getcwd(), f"results-d={running}")
    benchmark = "./worker_function_benchmarks.json"
    os.makedirs(results_dir, exist_ok=True)

    ansible_dir = os.path.join(ILU_HOME, "ansible")
    kwargs = {
        "ilu_home": ILU_HOME,
        "ansible_hosts_addrs": "@"
        + os.path.join(ansible_dir, "group_vars/local_addresses.yml"),
        "ansible_dir": ansible_dir,
        "build_level": build_level,
        "cores": CORES,
        "memory": MEMORY,
        "worker_status_ms": 1000,
        "cpu_queue_policy": "fcfs",
        "target": RunTarget.WORKER,
        "prewarm": 0,
        "gpus":1,
        "fpd": 32,
        "snapshotter": "zfs",
        "benchmark_file": benchmark,
        "gpu_queue":"mqfq",
        "gpu_queue_policy":"mqfq_select_out_len",
        "select_cnt": 30,
        "gpu_running": running,
        "force": True,
        "mqfq_ttl_sec": 500
    }
    # run entire experiment
    input_csv = "./in.csv"
    meta_csv = "./meta.csv"
    run_live(input_csv, meta_csv, results_dir, queue, **kwargs)

    ## plot some results
    from load.analysis import WorkerLogParser
    import matplotlib as mpl
    import numpy as np

    mpl.use("Agg")
    import matplotlib.pyplot as plt

    mpl.rcParams.update({"font.size": 14})
    mpl.rcParams["pdf.fonttype"] = 42
    mpl.rcParams["ps.fonttype"] = 42


    parser = WorkerLogParser(
        results_dir, input_csv, meta_csv, benchmark, RunType.LIVE, RunTarget.WORKER
    )
    parser.parse_logs(fail_if_errors=False)

    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)

    ax.scatter(parser.invokes_df["invoke_sent"], parser.invokes_df["queueing_sec"])

    ax.set_yscale('log')
    ax.set_ylabel(f"Metric: queueing_sec")
    ax.set_xlabel("")
    plt.savefig(os.path.join(results_dir, "queueing_sec.png"), bbox_inches="tight")
    plt.close(fig)


    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)

    df = parser.invokes_df
    next_pt = 0
    print(df["state"].unique())
    states = ["warm", "prewarm", "cold"]
    colors = ["red", "orange", "cyan"]
    for state, color in zip(states, colors):
        sub_df = df[df["state"] == state]
        sub_df = sub_df[~sub_df["was_cold"]]
        if len(sub_df) == 0:
            continue
        print(sub_df[sub_df["exec_overhead"] > .3])
        pts = np.arange(len(sub_df))
        if len(pts) > 0:
            ax.scatter(pts+next_pt, sub_df["exec_overhead"].sort_values(), color=color, label=state)
            next_pt = max(pts)+next_pt


    ax.legend()
    ax.set_ylabel(f"Metric: exec_overhead")
    ax.set_xlabel("")
    plt.savefig(os.path.join(results_dir, f"exec_overhead.png"), bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5, 3)

    df = parser.invokes_df
    df = df[df["state"] != "cold"]

    ax.scatter(df["queueing_sec"], df["exec_overhead"])

    df = df[df["exec_overhead"] > 1.0]
    print(df.columns)

    ax.set_yscale('log')
    ax.set_ylabel(f"Exec overhead")
    ax.set_xlabel("Queuing")
    plt.savefig(os.path.join(results_dir, "queueing_vs_execoverhead.png"), bbox_inches="tight")
    plt.close(fig)

with mp.Pool() as p:
    exps = []
    for running in [1,2]:
        exps.append((running, SSH_Q))
    p.starmap(run_exp, exps)
