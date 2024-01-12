from numpy import matrix, linalg, random
from time import time

def linpack(n):
    start = time()

    # LINPACK benchmarks
    ops = (2.0 * n) * n * n / 3.0 + (2.0 * n) * n

    # Create AxA array of random numbers -0.5 to 0.5
    A = random.random_sample((n, n)) - 0.5
    B = A.sum(axis=1)

    # Convert to matrices
    A = matrix(A)
    B = matrix(B.reshape((n, 1)))

    # Ax = B
    x = linalg.solve(A, B)
    end = time()
    latency = end - start

    mflops = (ops * 1e-6 / latency)

    result = {
        'mflops':mflops,
        'latency':latency
    }

    return result, start, end

cold = True

def main(args):
    global cold
    was_cold = cold
    cold = False
    try:
        n = int(args.get("n", 20))
        result, start, end = linpack(n)
        # print(result)
        return {"body": {"result":result, "cold":was_cold, "start":start, "end":end, "latency":end-start}}
    except Exception as e:
        return {"body": {"result":str(e), "cold":was_cold}}