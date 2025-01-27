import os
import os.path
import pandas as pd
import numpy as np
from math import ceil
import argparse
import multiprocessing as mp
from statsmodels.distributions.empirical_distribution import ECDF

buckets = [str(i) for i in range(1, 1441)]


def ecdf(index, row):
    iats = compute_row_iat(row)
    iats.sort()
    cdf = ECDF(iats)
    return cdf.x, cdf.y, iats


def compute_row_iat(row):
    iats = []
    last_t = -1
    tot = 0
    for minute in buckets:
        invokes = row[minute]
        minute = int(minute)
        tot += int(invokes)
        time_ms = minute * 1000
        if invokes == 0:
            continue
        elif invokes == 1:
            if last_t == -1:
                last_t = time_ms
                continue

            diff = time_ms - last_t
            iats.append(diff)
            last_t = time_ms
        else:
            if last_t == -1:
                last_t = time_ms

            sep = 1000.0 / float(invokes)
            for i in range(invokes):
                diff = (time_ms + i * sep) - last_t
                if diff == 0:
                    continue
                iats.append(diff)
                last_t = time_ms + i * sep
    r = np.array(iats)
    r.sort()
    return r


def compute_row_iat_stats(index, row) -> float:
    iats = compute_row_iat(row)
    if len(iats) < 1:
        print(iats, sum(row[buckets]))
        exit(1)
    return np.mean(iats), np.std(iats), len(iats)


def insert_iats(df: pd.DataFrame, debug: bool) -> pd.DataFrame:
    p = mp.Pool()
    if debug:
        print("Computing IATs")
    iat_data = p.starmap(compute_row_iat_stats, df.iterrows())
    df["IAT_mean"] = list(map(lambda x: x[0], iat_data))
    df["IAT_std"] = list(map(lambda x: x[1], iat_data))
    df["IAT_cnt"] = list(map(lambda x: x[2], iat_data))
    # df["IATs"] = list(map(lambda x: x[3], iat_data))
    return df


def insert_ecdfs(df: pd.DataFrame, debug: bool) -> pd.DataFrame:
    if debug:
        print("Computing ECDFs")
    p = mp.Pool()
    ecdf_data = p.starmap(ecdf, df.iterrows())
    df["ecdf_xs"] = list(map(lambda x: x[0], ecdf_data))
    df["ecdf_ys"] = list(map(lambda x: x[1], ecdf_data))
    return df


def join_day_one(
    datapath: str,
    force: bool,
    debug: bool = False,
    iats: bool = True,
    ecfds: bool = False,
):
    # TODO: use all the files in the dataset
    durations_file = "function_durations_percentiles.anon.d01.csv"
    invocations_file = "invocations_per_function_md.anon.d01.csv"
    mem_fnames_file = "app_memory_percentiles.anon.d01.csv"
    outfile_pckl = os.path.join(datapath, "joined_d01_trace.pckl")
    outfile_csv = os.path.join(datapath, "joined_d01_trace.csv")

    if force or (not os.path.exists(outfile_pckl) and not os.path.exists(outfile_csv)):
        if debug:
            print("Generating dataframe from scratch")

        file = os.path.join(datapath, durations_file)
        durations = pd.read_csv(file)
        durations.index = durations["HashFunction"]
        durations = durations.drop_duplicates("HashFunction")

        group_by_app = durations.groupby("HashApp").size()

        file = os.path.join(datapath, invocations_file)
        invocations = pd.read_csv(file)
        invocations = invocations.dropna()
        invocations.index = invocations["HashFunction"]
        sums = invocations[buckets].sum(axis=1)
        invocations["total_invocations"] = sums
        invocations = invocations[sums > 1]  # action must be invoked at least twice
        invocations = invocations.drop_duplicates("HashFunction")

        if iats:
            invocations = insert_iats(invocations, debug)
        if ecfds:
            invocations = insert_ecdfs(invocations, debug)

        joined = invocations.join(durations, how="inner", lsuffix="", rsuffix="_durs")

        file = os.path.join(datapath, mem_fnames_file)
        memory = pd.read_csv(file)
        memory = memory.drop_duplicates("HashApp")
        memory.index = memory["HashApp"]

        # memory is tabulated per _application_, but invocations a per-function
        # distribute the memory evenly between all functions in an application
        new_mem = memory.apply(
            lambda x: divive_by_func_num(x, group_by_app),
            axis=1,
            raw=False,
            result_type="expand",
        )
        memory["divvied"] = new_mem

        joined = joined.join(
            memory, how="inner", on="HashApp", lsuffix="", rsuffix="_mems"
        )

        # prevent 0 duration invocations, don't know why they're in the dataset
        joined = joined[joined["Maximum"] > 0]
        joined = joined[joined["percentile_Average_25"] > 0]

        if "IAT_mean" in joined.columns:
            joined["dur_iat_ratio"] = (
                joined["percentile_Average_25"] / joined["IAT_mean"]
            )
        joined.to_pickle(outfile_pckl, compression=None, protocol=3)
        to_drop = [
            col for col in ["IATs", "ecdf_xs", "ecdf_ys"] if col in joined.columns
        ]
        joined.drop(to_drop, axis=1).to_csv(outfile_csv)

        return joined
    elif os.path.exists(outfile_pckl):
        if debug:
            print("Loading dataframe from pickle file")
        return pd.read_pickle(outfile_pckl, compression=None)
    elif os.path.exists(outfile_csv):
        if debug:
            print("Regenerating dataframe from csv")

        df = pd.read_csv(outfile_csv)
        if iats:
            df = insert_iats(df, debug)
        if ecfds:
            df = insert_ecdfs(df, debug)
        df.to_pickle(outfile_pckl, compression=None, protocol=3)

        return df

    else:
        raise Exception("unable to generate dataframe, fell through")


def iat_invoke_times(
    iat_mean: float, iat_std: float, duration_min: int, scale: float = 1.0
):
    secs_p_min = 60
    milis_p_sec = 1000
    end_ms = duration_min * secs_p_min * milis_p_sec
    rng = np.random.default_rng(None)
    trace = list(
        np.ceil(
            rng.normal(
                loc=iat_mean, scale=iat_std, size=int(end_ms / iat_mean)
            ).cumsum()
            * scale
        )
    )
    time = trace[-1]
    while time < end_ms:
        sample = ceil(rng.normal(loc=iat_mean, scale=iat_std) * scale)
        while sample < 0:
            sample = ceil(rng.normal(loc=iat_mean, scale=iat_std) * scale)
        time += int(sample)
        if time < end_ms:
            trace.append(time)
    return list(filter(lambda x: x < end_ms, trace))


def iat_trace_row(func_name, row, duration_min: int, scale: float = 1.0):
    """
    Create invocations for the function using the function's IAT
    """
    cold_dur = int(row["Maximum"])
    warm_dur = int(row["percentile_Average_25"])
    mean = float(row["IAT_mean"]) * scale
    std = float(row["IAT_std"])
    mem = int(row["divvied"])
    trace = iat_invoke_times(mean, std, duration_min, scale)
    trace = [(func_name, int(t)) for t in trace]
    return trace, (func_name, cold_dur, warm_dur, mem, mean)


def real_trace_row(func_name, row, min_start=0, min_end=1440):
    """
    Create invocations for the function using the exact invocation times of the function from the trace
    """
    secs_p_min = 60
    milis_p_sec = 1000
    trace = list()
    cold_dur = int(row["Maximum"])
    warm_dur = int(row["percentile_Average_25"])
    mem = int(row["divvied"])
    for minute, invocs in enumerate(row[buckets[min_start:min_end]]):
        start = minute * secs_p_min * milis_p_sec
        if invocs == 0:
            continue
        elif invocs == 1:
            # if only one invocation, start randomly within that minute
            # avoid "thundering heard" of invocations at start of minute
            start_ms = np.random.randint(0, (secs_p_min * milis_p_sec) - 1)
            trace.append((func_name, start + start_ms))
        else:
            every = (secs_p_min * milis_p_sec) / invocs
            trace += [(func_name, int(start + i * every)) for i in range(invocs)]

    return trace, (func_name, cold_dur, warm_dur, mem, float(row["IAT_mean"]))


def ecdf_trace_row(
    func_name, row, duration_min: int, scale: float = 1.0, seed: int = None
):
    """
    Create invocations for the function using the function's ECDF
    """
    mean_iat_ms = float(row["IAT_mean"])
    if "ecdf_xs" in row:
        xs = row["ecdf_xs"]
        ys = row["ecdf_ys"]
    else:
        xs, ys, iats = ecdf(func_name, row)
    secs_p_min = 60
    milis_p_sec = 1000
    cold_dur = int(row["Maximum"])
    warm_dur = int(row["percentile_Average_25"])
    mem = int(row["divvied"])
    rng = np.random.default_rng(seed)

    end_ms = duration_min * secs_p_min * milis_p_sec
    expected_invokes = ceil(end_ms / mean_iat_ms)
    for i, x in enumerate(xs):
        if x >= 0:
            a = ys[i]
            break
    else:
        raise Exception("No positive number found")
    for i in range(len(xs) - 1, 0, -1):
        if xs[i] < np.inf:
            b = ys[i]
            break
    else:
        raise Exception("No non-inf number found")
    bulk = (b - a) * rng.random(expected_invokes) + a
    trace = np.interp(bulk, ys, xs) * scale
    trace = np.cumsum(trace)
    trace = trace.tolist()
    time = trace[-1]
    while time < end_ms:
        point = np.interp([rng.random()], ys, xs)
        while point == -float("inf"):
            point = np.interp([rng.random()], ys, xs)

        time += point * scale
        trace.append(float(time))
    trace = [(func_name, int(t)) for t in trace if t < end_ms]
    return trace, (func_name, cold_dur, warm_dur, mem, mean_iat_ms)


def divive_by_func_num(row, grouped_by_app):
    return ceil(row["AverageAllocatedMb"] / grouped_by_app[row["HashApp"]])


def write_trace(trace, metadata, trace_save_pth, metadata_save_pth):
    with open(trace_save_pth, "w") as f:
        f.write("{},{}\n".format("func_name", "invoke_time_ms"))
        for func_name, time_ms in trace:
            f.write("{},{}\n".format(func_name[:10], int(time_ms)))

    with open(metadata_save_pth, "w") as f:
        f.write(
            "{},{},{},{},{}\n".format(
                "func_name", "cold_dur_ms", "warm_dur_ms", "mem_mb", "mean_iat"
            )
        )
        for func_name, cold_dur, warm_dur, mem, mean_iat in metadata:
            f.write(
                "{},{},{},{},{}\n".format(
                    func_name[:10], cold_dur, warm_dur, mem, mean_iat
                )
            )


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--out-folder", "-o", required=True)
    argparser.add_argument("--data-path", "-d", required=True)
    argparser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Overwrite an existing trace that has the same number of functions",
    )
    argparser.add_argument("--debug", action="store_true", help="Enable debug printing")
    args = argparser.parse_args()
    store = args.out_folder

    joined = join_day_one(args.data_path, args.force, args.debug, iats=True)
    # for idx, row in joined.iterrows():
    # trace, data = iat_trace_row(idx, row, 0, 10)
    # print(data)
    # print(trace)
    # break
