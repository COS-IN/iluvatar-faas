from PIL import Image
from torchvision import transforms, models
import torch
import io
import time
import base64

MODEL_NAME = "resnet50"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

MODEL = None

try:
    with open("imagenet_classes.txt", "r") as f:
        LABELS = [s.strip() for s in f.readlines()]
except:
    LABELS = [f"Class {i}" for i in range(1000)]

PREPROCESS = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(
        mean=[0.485, 0.456, 0.406],
        std=[0.229, 0.224, 0.225]
    ),
])

def pre_process(image):
    input_image = image.convert("RGB")
    input_tensor = PREPROCESS(input_image)
    return input_tensor.to(DEVICE)

def load_model():
    global MODEL
    model = models.resnet50(
        weights=models.ResNet50_Weights.DEFAULT
    )
    model = model.to(DEVICE)
    model.eval()
    MODEL = model

def get_model():
    global MODEL
    was_cold = False
    if MODEL is None:
        load_model()
        was_cold = True
    return MODEL, was_cold

def predict(input_tensor, model):
    if DEVICE == "cuda":
        torch.cuda.synchronize()
    infer_start = time.perf_counter()

    with torch.inference_mode():
        output = model(input_tensor)

    if DEVICE == "cuda":
        torch.cuda.synchronize()
    infer_end = time.perf_counter()

    # Get predictions for the first element in batch
    prob = torch.nn.functional.softmax(output[0], dim=0)
    high_prob, pred_label = torch.topk(prob, 1)

    predicted_class = LABELS[pred_label[0].item()]
    probability = high_prob[0].item()
    return str(predicted_class), probability, (infer_end - infer_start)

def main(args):
    try:
        image_b64 = args.get("image_data")
        batch_size = int(args.get("batch_size", 32))
        
        if image_b64 is None:
            return {"body": {"Failed": "No input image!", "cold": False}}

        total_start = time.perf_counter()

        image_bytes = base64.b64decode(image_b64)
        image = Image.open(io.BytesIO(image_bytes))

        # We decode and preprocess ONCE to avoid CPU bottleneck.
        # Then we copy it on the target device to simulate a batch of images.
        single_tensor = pre_process(image)
        # expand to simulate a batch
        batched_tensor = single_tensor.unsqueeze(0).repeat(batch_size, 1, 1, 1)
        
        preprocess_end = time.perf_counter()

        model, was_cold = get_model()
        model_access_end = time.perf_counter()

        prediction, probability, inference_time = predict(batched_tensor, model)
        total_end = time.perf_counter()

        return {
            "body": {
                "Success! Using model": MODEL_NAME,
                "Batch Size": batch_size,
                "Prediction Class": prediction,
                "Probability": probability,
                "cold": was_cold,
                "Device": DEVICE,
                "Total Time (s)": total_end - total_start,
                "Pre-process Time (s)": preprocess_end - total_start,
                "Model Access Time (s)": model_access_end - preprocess_end,
                "Inference Time (s)": inference_time,
                "latency": total_end - total_start,
                "start": time.time() - (total_end - total_start),
                "end": time.time()
            }
        }

    except Exception as e:
        raise e
