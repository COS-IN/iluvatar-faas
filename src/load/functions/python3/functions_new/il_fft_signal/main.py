import torch
import time
import math

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

def main(args):
    try:
        size = int(args.get("size", 4096))

        total_start = time.perf_counter()

        # Generate 2D signal directly on device — no CPU overhead
        x = torch.linspace(0, 4 * math.pi, size, device=DEVICE)
        y = torch.linspace(0, 4 * math.pi, size, device=DEVICE)
        xx, yy = torch.meshgrid(x, y, indexing='ij')
        signal = torch.sin(xx * 3) + torch.cos(yy * 5) + 0.5 * torch.sin(xx * yy * 0.01)
        signal += torch.randn(size, size, device=DEVICE) * 0.1
        gen_end = time.perf_counter()

        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_start = time.perf_counter()

        # Forward FFT
        freq = torch.fft.fft2(signal)

        # Spectral filtering — low-pass filter
        cutoff = size // 4
        mask = torch.zeros(size, size, device=DEVICE)
        mask[:cutoff, :cutoff] = 1
        mask[:cutoff, -cutoff:] = 1
        mask[-cutoff:, :cutoff] = 1
        mask[-cutoff:, -cutoff:] = 1
        filtered_freq = freq * mask

        # Inverse FFT
        filtered_signal = torch.fft.ifft2(filtered_freq).real

        # Additional processing — compute power spectrum
        power_spectrum = torch.abs(freq) ** 2
        total_energy = power_spectrum.sum().item()

        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_end = time.perf_counter()

        total_end = time.perf_counter()

        return {
            "body": {
                "Success": True,
                "Signal Size": f"{size}x{size}",
                "Total Energy": total_energy,
                "Filtered Mean": filtered_signal.mean().item(),
                "Filtered Std": filtered_signal.std().item(),
                "cold": False,
                "Device": DEVICE,
                "Total Time (s)": total_end - total_start,
                "Signal Gen Time (s)": gen_end - total_start,
                "FFT + Filter Time (s)": infer_end - infer_start,
                "latency": total_end - total_start,
                "start": time.time() - (total_end - total_start),
                "end": time.time()
            }
        }

    except Exception as e:
        raise e
