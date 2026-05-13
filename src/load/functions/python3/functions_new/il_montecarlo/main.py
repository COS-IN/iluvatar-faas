import torch
import time
import math

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

def main(args):
    try:
        num_paths = int(args.get("num_paths", 10_000_000))
        num_steps = int(args.get("num_steps", 252))

        # Black-Scholes parameters for a European call option
        S0 = 100.0       # Initial stock price
        K = 105.0         # Strike price
        T = 1.0           # Time to maturity (1 year)
        r = 0.05          # Risk-free rate
        sigma = 0.2       # Volatility
        dt = T / num_steps

        total_start = time.perf_counter()

        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_start = time.perf_counter()

        # Generate all random increments at once on device
        Z = torch.randn(num_paths, num_steps, device=DEVICE, dtype=torch.float32)

        # Simulate GBM paths using log returns
        log_returns = (r - 0.5 * sigma**2) * dt + sigma * math.sqrt(dt) * Z
        log_S = torch.cumsum(log_returns, dim=1)
        S_T = S0 * torch.exp(log_S[:, -1])

        # Calculate option payoffs
        payoffs = torch.clamp(S_T - K, min=0.0)

        # Discount to present value
        option_price = torch.exp(torch.tensor(-r * T, device=DEVICE)) * payoffs.mean()

        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_end = time.perf_counter()

        # Compute standard error
        std_err = (payoffs.std() / math.sqrt(num_paths)).item()
        price = option_price.item()

        total_end = time.perf_counter()

        return {
            "body": {
                "Success": True,
                "Option Price": price,
                "Std Error": std_err,
                "Num Paths": num_paths,
                "Num Steps": num_steps,
                "cold": False,
                "Device": DEVICE,
                "Total Time (s)": total_end - total_start,
                "Simulation Time (s)": infer_end - infer_start,
                "latency": total_end - total_start,
                "start": time.time() - (total_end - total_start),
                "end": time.time()
            }
        }

    except Exception as e:
        raise e
