from dataset import join_day_one, iat_trace_row, write_trace
from trace_analyze import run_trace_csv
import os, time, argparse
import multiprocessing as mp

def gen_trace(args):
  out_folder, data_folder, num_funcs, duration = args
  dataset = join_day_one(data_folder, False, iats=True)
  dataset = dataset[dataset["dur_iat_ratio"] < 4]
  quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]

  per_qant = num_funcs // (len(quantiles)-1)
  qts = dataset["IAT_mean"].quantile(quantiles)
  trace = []
  function_metadata = []
  out_folder = os.path.join(out_folder, str(num_funcs))
  metadata_save_pth = os.path.join(out_folder, "metadata-{}.csv".format(num_funcs))
  os.makedirs(out_folder, exist_ok=True)

  func_name = 0
  for i in range(4):
    low = qts.iloc[i]
    high = qts.iloc[i+1]
    choose_from = dataset[dataset["IAT_mean"].between(low, high)]
    chosen = choose_from.sample(per_qant, random_state=time.time_ns() % pow(2, 31))

    for index, row in chosen.iterrows():
      traced_row, (func_name, cold_dur, warm_dur, mem) = iat_trace_row(row["HashFunction"], row, func_name, duration)
      trace += traced_row
      function_metadata.append((func_name, cold_dur, warm_dur, mem, func_name))
      func_name += 1

  out_trace = sorted(trace, key=lambda x:x[1]) #(func_name, time_ms)

  trace_save_pth = os.path.join(out_folder, "{}.csv".format(num_funcs))
  write_trace(out_trace, function_metadata, trace_save_pth, metadata_save_pth)

  return (num_funcs, *run_trace_csv(trace_save_pth, 0.80, metadata_save_pth))

if __name__ == "__main__":
  argparser = argparse.ArgumentParser()
  argparser.add_argument("--out-folder", '-o', help="The folder to store the output csv files into", required=True)
  argparser.add_argument("--data-folder", '-d', help="The folder where the azure dataset has been downloaded to", required=True)
  argparser.add_argument("--num-funcs", '-n', type=int, help="Number of functions to sample for the trace", required=True)
  argparser.add_argument("--duration", type=int, help="The length in minutes of the trace", default=60)
  args = argparser.parse_args()


  pool_iter = [(args.out_folder, args.data_folder, i, args.duration) for i in range(args.num_funcs-5, args.num_funcs+5)]
  p = mp.Pool()
  with open(os.path.join(args.out_folder, "README.md"), 'w') as f:
    f.write("num_funcs, warm_pct, max_mem, max_running, mean_running, running_75th, running_90th\n")

    results = p.map(gen_trace, pool_iter)
    results = sorted(results, key=lambda x : x[0])
    for r in results:
      f.write("{}, {}, {}, {}, {}, {}, {}\n".format(*r))  