msg = "good"
import traceback

try:
  import os
  from time import time
  import numpy as np
except Exception as e:
  msg = traceback.format_exc()

tmp = "/tmp/"
cold = True

def has_gpu() -> bool:
  return os.path.isfile("/usr/bin/nvidia-smi")  

def math_cupy(size, iters):
  import cupy
  import cupyx.scipy.fft as cufft
  mempool = cupy.get_default_memory_pool()
  mempool.set_limit(size=2 * (1024**3))  # 2 GiB

  dev = cupy.cuda.Device()
  Capability = dev.compute_capability
  Memory = dev.mem_info

  squared_diff_generic = cupy.ElementwiseKernel(
    'T x, T y',
    'T z',
    'z = (x - y) * (x - y)',
    'squared_diff_generic')
  
  A = cupy.random.rand(size, size, dtype=cupy.float32)
  B = cupy.random.rand(size, size, dtype=cupy.float32)

  for i in range(iters):
    X = cufft.fft(squared_diff_generic(A,B))

def math_numpy(size, iters):
  A = np.random.rand(size, size)
  B = np.random.rand(size, size)

  for i in range(iters):
    z = (A - B) * (A - B)
    X = np.fft.fft(z)

def main(args):
  global cold
  was_cold = cold
  cold = False
  try:
    start = time()
    
    size = int(args.get('size', 500))
    iters = int(args.get('iters', 1000))

    if has_gpu():
      math_cupy(size, iters)
    else:
      math_numpy(size, iters)

    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end }}
  except Exception as e:
    err = str(e)
    try:
        trace = traceback.format_exc()
    except Exception as fug:
        trace = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "trace":trace, "cold":was_cold }}
