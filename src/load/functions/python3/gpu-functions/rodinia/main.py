msg = "good"
import traceback
try:
    import os, sys, subprocess
    import uuid
    from time import time
    import logging
except Exception as e:
    msg = traceback.format_exc()

bins = "/app/"
cold = True

def has_gpu() -> bool:
  return os.path.isfile("/usr/bin/nvidia-smi")  

def get_args(args):
  bin = args.get("bin", "myocyte.out")

  if has_gpu():
    ret = None
    bin_path = os.path.join(bins, "cuda", bin)
    if "args" in args:
      error, bin_args = False, args["args"].split(' ')
      bin_args.insert(0, bin_path)
      return False, bin_args

    if bin == "pathfinder":
      ret = [bin_path, "100000", "100", "2"]
    elif bin == "srad":
      ret = [bin_path, "1", "0.5", "20480", "20480"]
    elif bin == "needle":
      ret = [bin_path, "10240", "10"]
    elif bin == "myocyte.out":
      ret = [bin_path, "100", "3000", "1"]
    elif bin == "backprop":
      ret = [bin_path, "112097152"]
    elif bin == "gaussian":
      ret = [bin_path, "-s", "2048"]
    elif bin == "lavaMD":
      ret = [bin_path, "-boxes1d", "30"]
    elif bin == "lud":
      bin_path = os.path.join(bins, "cuda", "lud_cuda")
      ret = [bin_path, "-s", "8192"]
    else:
      return True, {"body": { "error": f"Unknown GPU binary '{bin}'" }}

  else:
    bin_path = os.path.join(bins, "openmp", bin)
    if "args" in args:
      error, bin_args = False, args["args"].split(' ')
      bin_args.insert(0, bin_path)

      return False, bin_args
    if bin == "pathfinder":
      ret = [bin_path, "100000", "100"]
    elif bin == "srad":
      ret = [bin_path, "100", "0.5", "20480", "20480", "1"]
    elif bin == "needle":
      ret = [bin_path, "1024", "10", "2"]
    elif bin == "myocyte.out":
      ret = [bin_path, "100", "3000", "1", "1"]
    elif bin == "backprop":
      ret = [bin_path, "12097152"]
    elif bin == "lavaMD":
      ret = [bin_path, "-boxes1d", "10"]
    elif bin == "lud":
      bin_path = os.path.join(bins, "openmp", "lud_omp")
      ret = [bin_path, "-s", "1024"]
    else:
      return True, {"body": { "error": f"Unknown CPU binary '{bin}'" }}
    
  return False, ret

def main(args):
    global cold
    start = time()
    was_cold = cold
    cold = False
    try:
        error, bin_args = get_args(args)
        
        if error:
          return bin_args
        else:
          completed = subprocess.run(args=bin_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
          completed.check_returncode()
          end = time()
          return {"body": { "latency":end-start, "stdout":completed.stdout, "cold":was_cold, "start":start, "end":end, "bin":bin_args }}
           
    except Exception as e:
        err = str(e)
        try:
            err = traceback.format_exc()
        except Exception as fug:
            err = str(fug)
        return {"body": { "cust_error":msg, "thing":err, "cold":was_cold }}
    