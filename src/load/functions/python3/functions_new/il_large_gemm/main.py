import torch
import time

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

MATRIX_A = None
MATRIX_B = None

def get_matrices(n=8192):
    global MATRIX_A, MATRIX_B
    was_cold = False
    if MATRIX_A is None or MATRIX_B is None:
        MATRIX_A = torch.randn(n, n, device=DEVICE, dtype=torch.float32)
        MATRIX_B = torch.randn(n, n, device=DEVICE, dtype=torch.float32)
        was_cold = True
    return MATRIX_A, MATRIX_B, was_cold

def main(args):
    try:
        n = args.get("n", 8192)
        
        total_start = time.perf_counter()
        
        # Cold start: matrix generation
        mat_A, mat_B, was_cold = get_matrices(n)
        
        if DEVICE == "cuda":
            torch.cuda.synchronize()
            
        infer_start = time.perf_counter()
        
        # Computation
        C = torch.matmul(mat_A, mat_B)
        
        if DEVICE == "cuda":
            torch.cuda.synchronize()
            
        infer_end = time.perf_counter()
        total_end = time.perf_counter()
        
        # Output calculation to prevent optimization
        sum_val = C.sum().item()
        
        return {
            "body": {
                "Success! Matrix Size": f"{n}x{n}",
                "Sum": sum_val,
                "cold": was_cold,
                "Device": DEVICE,
                "Total Time (s)": total_end - total_start,
                "Inference Time (s)": infer_end - infer_start,
                "latency": total_end - total_start,
                "start": time.time() - (total_end - total_start),
                "end": time.time()
            }
        }
        
    except Exception as e:
        raise e
