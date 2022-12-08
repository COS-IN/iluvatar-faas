from time import time, sleep
import subprocess, signal

cold = True

def main(args):
  start = time()
  global cold
  was_cold = cold
  cold = False
  start = time()

  cold_run_ms = float(args.get("cold_run", 2000))
  warm_run_ms = float(args.get("warm_run", 1000))
  ncpus = float(args.get("ncpus", 1))
  mem_mb = int(args.get("mem_mb", 128))

  mem_util = "--mem-util={}MB".format(mem_mb)
  if was_cold:
    timeout = cold_run_ms / 1000.0
  else:
    timeout = warm_run_ms / 1000.0

  # this will vary the CPU utilization from 25-100 several times before we exit
  peak = int(timeout/4)
  period = int(timeout/2)
  ncpus="--ncpus={}".format(ncpus)
  if peak >= 1:
    period="--cpu-curve-period={}".format(int(timeout/2))
    peak="--cpu-curve-peak={}".format(int(timeout/4))
    mode="--cpu-mode=curve"
    util="--cpu-util=25-100"
  else:
    period=""
    peak=""
    mode="--cpu-mode=fixed"
    util="--cpu-util=75"
  lookbusy = subprocess.Popen(args=["lookbusy", ncpus, mem_util, util, mode, period, peak])
  sleep(timeout)
  lookbusy.send_signal(signal.SIGINT)

  end = time()
  latency = end - start

  return {"body": {'latency': latency, "cold":was_cold, "start":start, "end":end}}

if __name__ == "__main__":
  print(main({}))