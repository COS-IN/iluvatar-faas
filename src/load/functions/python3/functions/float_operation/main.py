import math
from time import time


def float_operations(n):
    start = time()
    for i in range(0, n):
        sin_i = math.sin(i)
        cos_i = math.cos(i)
        sqrt_i = math.sqrt(i)
    end = time()
    latency = end - start
    return latency, start, end

cold = True

def main(args):
    global cold
    was_cold = cold
    cold = False

    n = int(args.get("n", 20))
    result, start, end = float_operations(n)
    # print(result)
    return {"body": { "result" : result, "cold":was_cold, "start":start, "end":end, "latency":end-start }}
