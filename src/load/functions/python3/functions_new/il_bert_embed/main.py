import torch
from transformers import BertTokenizer, BertModel
import time

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

MODEL = None
TOKENIZER = None

def load_model():
    global MODEL, TOKENIZER
    TOKENIZER = BertTokenizer.from_pretrained("bert-base-uncased")
    MODEL = BertModel.from_pretrained("bert-base-uncased")
    MODEL = MODEL.to(DEVICE)
    MODEL.eval()

def get_model():
    global MODEL, TOKENIZER
    was_cold = False
    if MODEL is None:
        load_model()
        was_cold = True
    return MODEL, TOKENIZER, was_cold

def main(args):
    try:
        text = args.get("text", "The quick brown fox jumps over the lazy dog. " * 10)

        total_start = time.perf_counter()

        model, tokenizer, was_cold = get_model()
        model_access_end = time.perf_counter()

        # Tokenize — very fast, ~1ms
        inputs = tokenizer(text, return_tensors="pt", padding=True,
                          truncation=True, max_length=512)
        inputs = {k: v.to(DEVICE) for k, v in inputs.items()}
        tokenize_end = time.perf_counter()

        # Forward pass — GPU heavy
        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_start = time.perf_counter()

        with torch.inference_mode():
            outputs = model(**inputs)

        if DEVICE == "cuda":
            torch.cuda.synchronize()
        infer_end = time.perf_counter()

        # Extract [CLS] embedding
        embedding = outputs.last_hidden_state[:, 0, :]
        embed_norm = embedding.norm().item()
        embed_dim = embedding.shape[-1]

        total_end = time.perf_counter()

        return {
            "body": {
                "Success": True,
                "Model": "bert-base-uncased",
                "Embedding Dim": embed_dim,
                "Embedding Norm": embed_norm,
                "Token Count": inputs["input_ids"].shape[1],
                "cold": was_cold,
                "Device": DEVICE,
                "Total Time (s)": total_end - total_start,
                "Model Access Time (s)": model_access_end - total_start,
                "Tokenize Time (s)": tokenize_end - model_access_end,
                "Inference Time (s)": infer_end - infer_start,
                "latency": total_end - total_start,
                "start": time.time() - (total_end - total_start),
                "end": time.time()
            }
        }

    except Exception as e:
        raise e
