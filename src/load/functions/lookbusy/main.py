from time import time, sleep
import os
import subprocess

cold = True

def main(args):
    global cold
    was_cold = cold
    cold = False
    cold_run_ms = int(args.get("cold_run", 2000))
    warm_run_ms = int(args.get("warm_run", 1000))
    mem_mb = int(args.get("mem_mb", 128))

    start = time()
    
    mem_util = "--mem-util={}MB".format(mem_mb)
    if was_cold:
        timeout = cold_run_ms / 1000
    else:
        timeout = warm_run_ms / 1000
    print(mem_util, timeout)

    run = time()
    wsk = subprocess.Popen(args=["lookbusy", "--ncpus=1", mem_util, "--cpu-util=25-75"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    sleep(timeout)
    wsk.kill()

    latency = time() - start

    return {"body": {'latency': latency, "cold":was_cold}}

if __name__ == "__main__":
    print(main({}))