msg = "good"
import traceback

try:
  from time import time
  import math
  import numpy as np
  import os
  from equation_of_state import run_gpu, run_cpu
except Exception as e:
  msg = traceback.format_exc()

cold = True

def has_gpu() -> bool:
  return os.path.isfile("/usr/bin/nvidia-smi")  

def main(args):
  global cold
  was_cold = cold
  cold = False
  try:
    start = time()
    
    size = int(args.get('size', 50000))

    shape = (
        math.ceil(2 * size ** (1 / 3)),
        math.ceil(2 * size ** (1 / 3)),
        math.ceil(0.25 * size ** (1 / 3)),
    )

    if has_gpu():
      import cupy
      mempool = cupy.get_default_memory_pool()
      mempool.set_limit(size=2 * (1024**3))  # 2 GiB

      s = cupy.random.uniform(1e-2, 10, size=shape)
      t = cupy.random.uniform(-12, 20, size=shape)
      p = cupy.random.uniform(0, 1000, size=(1, 1, shape[-1]))
      run_gpu(s, t, p)
    else:
      s = np.random.uniform(1e-2, 10, size=shape)
      t = np.random.uniform(-12, 20, size=shape)
      p = np.random.uniform(0, 1000, size=(1, 1, shape[-1]))
      run_cpu(s, t, p)

    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end }}
  except Exception as e:
    err = str(e)
    try:
        trace = traceback.format_exc()
    except Exception as fug:
        trace = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "trace":trace, "cold":was_cold }}
