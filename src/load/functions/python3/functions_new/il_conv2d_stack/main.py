import torch
import torch.nn as nn
import time

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

MODEL = None

def build_conv_stack(in_channels=3, mid_channels=256, num_layers=8):
    """Build a deep stack of Conv2D + BatchNorm + ReLU layers."""
    layers = []
    c_in = in_channels
    for i in range(num_layers):
        c_out = mid_channels
        layers.append(nn.Conv2d(c_in, c_out, kernel_size=3, padding=1))
        layers.append(nn.BatchNorm2d(c_out))
        layers.append(nn.ReLU(inplace=True))
        c_in = c_out
    # Final reduction
    layers.append(nn.AdaptiveAvgPool2d(1))
    layers.append(nn.Flatten())
    layers.append(nn.Linear(mid_channels, 10))
    return nn.Sequential(*layers)

def get_model(channels):
    global MODEL
    was_cold = False
    if MODEL is None:
        MODEL = build_conv_stack(in_channels=3, mid_channels=channels, num_layers=8)
        MODEL = MODEL.to(DEVICE)
        MODEL.eval()
        was_cold = True
    return MODEL, was_cold

def main(args):
    try:
        channels = int(args.get("channels", 256))
        size = int(args.get("size", 512))
        batch = int(args.get("batch", 4))

        total_start = time.perf_counter()

        model, was_cold = get_model(channels)
        model_access_end = time.perf_counter()

        # Generate random input tensor directly on device — no CPU image decode
        input_tensor = torch.randn(batch, 3, size, size, device=DEVICE)

        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_start = time.perf_counter()

        with torch.inference_mode():
            output = model(input_tensor)

        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_end = time.perf_counter()

        total_end = time.perf_counter()

        return {
            "body": {
                "Success": True,
                "Channels": channels,
                "Input Size": f"{batch}x3x{size}x{size}",
                "Output Shape": list(output.shape),
                "Output Norm": output.norm().item(),
                "cold": was_cold,
                "Device": DEVICE,
                "Total Time (s)": total_end - total_start,
                "Model Access Time (s)": model_access_end - total_start,
                "Inference Time (s)": infer_end - infer_start,
                "latency": total_end - total_start,
                "start": time.time() - (total_end - total_start),
                "end": time.time()
            }
        }

    except Exception as e:
        raise e
