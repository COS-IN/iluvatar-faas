msg = "good"
import traceback

try:
  import os
  from time import time
  import numpy as np
  import ctypes
except Exception as e:
  msg = traceback.format_exc()

tmp = "/tmp/"
cold = True
last_size = -1
A = None
B = None
X = None

def has_gpu() -> bool:
    return True

sync = True
def math_cupy(size, iters):
  import cupy
  import cupyx.scipy.fft as cufft
  global A, B, X, last_size, sync
  squared_diff_generic = cupy.ElementwiseKernel(
    'T x, T y',
    'T z',
    'z = (x - y) * (x - y)',
    'squared_diff_generic')
  if size != last_size:
    print(f"allocating memory {last_size} vs {size}")
    last_size = size
  A = cupy.random.rand(size, size, dtype=cupy.float32)
  B = cupy.random.rand(size, size, dtype=cupy.float32)
  for i in range(iters):
    X = cufft.fft(squared_diff_generic(A,B))
  return str(cupy.mean(cupy.mean(X)))

def math_numpy(size, iters):
  global A, B, X, last_size
  if size != last_size:
    print(f"allocating memory {last_size} vs {size}")
    last_size = size
    A = np.random.rand(size, size)
    B = np.random.rand(size, size)

  for i in range(iters):
    z = (A - B) * (A - B)
    X = np.fft.fft(z)
  return str(np.mean(np.mean(X)))

def main(args):
  global cold, sync
  was_cold = cold
  cold = False
  try:
    start = time()
    
    size = int(args.get('size', 6000))
    iters = int(args.get('iters', 150))

    if has_gpu():
      mean = math_cupy(size, iters)
    else:
      mean = math_numpy(size, iters)

    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end, "sync":sync, "mean":mean }}
  except Exception as e:
    err = str(e)
    try:
        trace = traceback.format_exc()
    except Exception as fug:
        trace = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "trace":trace, "cold":was_cold }}

if __name__ == "__main__":
    main({})
