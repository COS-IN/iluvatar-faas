msg = "good"
import traceback

try:
  import os
  from time import time
  import numpy as np
  import cupy
  import cupyx.scipy.fft as cufft
except Exception as e:
  msg = traceback.format_exc()

tmp = "/tmp/"
cold = True

def math(size, iters):
  squared_diff_generic = cupy.ElementwiseKernel(
    'T x, T y',
    'T z',
    'z = (x - y) * (x - y)',
    'squared_diff_generic')
  
  A = cupy.random.rand(size, size, dtype=cupy.float32)
  B = cupy.random.rand(size, size, dtype=cupy.float32)

  for i in range(iters):
    X = cufft.fft(squared_diff_generic(A,B))

def main(args):
  global cold
  was_cold = cold
  cold = False
  try:
    start = time()
    
    size = int(args.get('size', 500))
    iters = int(args.get('iters', 500))

    mempool = cupy.get_default_memory_pool()
    mempool.set_limit(size=2 * (1024**3))  # 2 GiB

    dev = cupy.cuda.Device()
    Capability = dev.compute_capability
    Memory = dev.mem_info
    math(size, iters)
    # mempool = cupy.get_default_memory_pool()
    # pinned_mempool = cupy.get_default_pinned_memory_pool()

    # mempool.free_all_blocks()
    # pinned_mempool.free_all_blocks()

    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end, "Capability":Capability, "Memory":Memory }}
  except Exception as e:
    err = str(e)
    try:
        trace = traceback.format_exc()
    except Exception as fug:
        trace = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "trace":trace, "cold":was_cold }}
