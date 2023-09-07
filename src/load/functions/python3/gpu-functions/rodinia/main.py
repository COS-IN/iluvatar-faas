msg = "good"
import traceback
try:
    import os, sys, subprocess
    import uuid
    from time import time
    import logging
except Exception as e:
    msg = traceback.format_exc()

bins = "/usr/bin/"
cold = True

def main(args):
    global cold
    start = time()
    was_cold = cold
    cold = False
    try:
        bin = args.get("bin", "myocyte.out")
        args = None

        bin_path = os.path.join(bins, bin)
        if bin == "pathfinder":
          args = [bin_path, "100000", "100", "2"]
        elif bin == "srad":
          args = [bin_path, "1", "0.5", "20480", "20480"]
        elif bin == "needle":
          args = [bin_path, "10240", "10"]
        elif bin == "myocyte.out":
          args = [bin_path, "100", "3000", "1"]
        elif bin == "backprop":
          args = [bin_path, "112097152"]
        elif bin == "gaussian":
          args = [bin_path, "-s", "2048"]
        elif bin == "lavaMD":
          args = [bin_path, "-boxes1d", "30"]
        elif bin == "lud_cuda":
          args = [bin_path, "-s", "8192"]
        else:
          return {"body": { "error": f"Unknown binary '{bin}'" }}
           
        subprocess.call(args=args)
            
        end = time()
        return {"body": { "latency":end-start, "msg":msg, "cold":was_cold, "start":start, "end":end }}
    except Exception as e:
        err = str(e)
        try:
            err = traceback.format_exc()
        except Exception as fug:
            err = str(fug)
        return {"body": { "cust_error":msg, "thing":err, "cold":was_cold }}