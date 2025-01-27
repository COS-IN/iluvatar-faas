from dataset import join_day_one, ecdf_trace_row, write_trace
from trace_analyze import run_trace_csv
import os
import time
import argparse
from contextlib import suppress
import multiprocessing as mp

"""
Use ECDFs to generate a trace from a specified number of functions
"""

argparser = argparse.ArgumentParser()
argparser.add_argument(
    "--out-folder",
    "-o",
    help="The folder to store the output csv files into",
    required=True,
)
argparser.add_argument(
    "--data-path",
    "-d",
    help="The folder where the azure dataset has been downloaded to",
    required=True,
)
argparser.add_argument(
    "--num-funcs",
    "-n",
    type=int,
    help="Number of functions to sample for the trace",
    required=False,
)
argparser.add_argument(
    "--force",
    "-f",
    action="store_true",
    help="Overwrite an existing trace that has the same number of functions",
)
argparser.add_argument(
    "--seed", type=int, help="Random seed to fix sampling", required=False
)
argparser.add_argument(
    "--duration", type=int, help="The length in minutes of the trace", default=60
)
args = argparser.parse_args()

dataset = join_day_one(args.data_path, args.force, ecfds=True)

trace = []
function_metadata = []
metadata_save_pth = os.path.join(args.out_folder, "metadata-chosen-ecdf.csv")
trace_save_pth = os.path.join(args.out_folder, "chosen-ecdf.csv")

with suppress(FileExistsError):
    os.makedirs(args.out_folder)

if (
    not (os.path.exists(metadata_save_pth) and os.path.exists(trace_save_pth))
    or args.force
):

    map_args = []

    rows = dataset.sample(args.num_funcs, random_state=args.seed)
    for index, row in rows.iterrows():
        map_args.append((index[:10], row, args.duration))

    pool = mp.Pool()
    ret = pool.starmap(ecdf_trace_row, map_args)

    for traced_row, (func_name, cold_dur, warm_dur, mem, mean_iat) in ret:
        trace += traced_row
        function_metadata.append((func_name, cold_dur, warm_dur, mem, mean_iat))

    out_trace = sorted(trace, key=lambda x: x[1])

    write_trace(out_trace, function_metadata, trace_save_pth, metadata_save_pth)

    print("done", trace_save_pth)
    print("warm_pct, max_mem, max_running, mean_running, running_75th, running_90th")
    print(*run_trace_csv(trace_save_pth, 0.80, metadata_save_pth))
    print(*run_trace_csv(trace_save_pth, 0.90, metadata_save_pth))
    print(*run_trace_csv(trace_save_pth, 0.99, metadata_save_pth))
