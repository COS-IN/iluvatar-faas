msg = "good"
import traceback

try:
  from time import time
  import math
  import numpy as np
  import os
  from isoneural import run_gpu, run_cpu
except Exception as e:
  msg = traceback.format_exc()

cold = True

def has_gpu() -> bool:
  return os.path.isfile("/usr/bin/nvidia-smi")  

def generate_inputs(size, module):
    shape = (
        math.ceil(2 * size ** (1 / 3)),
        math.ceil(2 * size ** (1 / 3)),
        math.ceil(0.25 * size ** (1 / 3)),
    )

    # masks
    maskT, maskU, maskV, maskW = (
        (module.random.rand(*shape) < 0.8).astype("float64") for _ in range(4)
    )

    # 1d arrays
    dxt, dxu = (module.random.randn(shape[0]) for _ in range(2))
    dyt, dyu = (module.random.randn(shape[1]) for _ in range(2))
    dzt, dzw, zt = (module.random.randn(shape[2]) for _ in range(3))
    cost, cosu = (module.random.randn(shape[1]) for _ in range(2))

    # 3d arrays
    K_iso, K_11, K_22, K_33 = (module.random.randn(*shape) for _ in range(4))

    # 4d arrays
    salt, temp = (module.random.randn(*shape, 3) for _ in range(2))

    # 5d arrays
    Ai_ez, Ai_nz, Ai_bx, Ai_by = (module.zeros((*shape, 2, 2)) for _ in range(4))

    return (
        maskT,
        maskU,
        maskV,
        maskW,
        dxt,
        dxu,
        dyt,
        dyu,
        dzt,
        dzw,
        cost,
        cosu,
        salt,
        temp,
        zt,
        K_iso,
        K_11,
        K_22,
        K_33,
        Ai_ez,
        Ai_nz,
        Ai_bx,
        Ai_by,
    )

def main(args):
  global cold
  was_cold = cold
  cold = False
  try:
    start = time()
    
    size = int(args.get('size', 500000))

    if has_gpu():
      import cupy
      mempool = cupy.get_default_memory_pool()
      mempool.set_limit(size=2 * (1024**3))  # 2 GiB

      inputs = generate_inputs(size, cupy)
      run_gpu(*inputs)
    else:
      inputs = generate_inputs(size, np)
      run_cpu(*inputs)

    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end }}
  except Exception as e:
    err = str(e)
    try:
        trace = traceback.format_exc()
    except Exception as fug:
        trace = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "trace":trace, "cold":was_cold }}
