from PIL import Image
from torchvision import transforms, models
import torch
import io
import time
import base64

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

LOADED_MODELS = {}

with open("imagenet_classes.txt", "r") as f:
    LABELS = [s.strip() for s in f.readlines()]


def pre_process(image):
    input_image = image.convert("RGB")
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225]
        ),
    ])
    input_tensor = preprocess(input_image)
    return input_tensor.unsqueeze(0).to(DEVICE)


def load_model(model_name):
    if model_name == "mobilenet_v3_small":
        model = models.mobilenet_v3_small(weights=models.MobileNet_V3_Small_Weights.DEFAULT)
    elif model_name == "resnet18":
        model = models.resnet18(weights=models.ResNet18_Weights.DEFAULT)
    elif model_name == "resnet34":
        model = models.resnet34(weights=models.ResNet34_Weights.DEFAULT)
    elif model_name == "resnet50":
        model = models.resnet50(weights=models.ResNet50_Weights.DEFAULT)
    elif model_name == "resnet101":
        model = models.resnet101(weights=models.ResNet101_Weights.DEFAULT)
    elif model_name == "vit_b_16":
        model = models.vit_b_16(weights=models.ViT_B_16_Weights.DEFAULT)
    else:
        raise ValueError(f"Unsupported model: {model_name}")

    model = model.to(DEVICE)
    model.eval()
    return model


def get_model(model_name):
    global LOADED_MODELS
    if model_name not in LOADED_MODELS:
        LOADED_MODELS[model_name] = load_model(model_name)
        return LOADED_MODELS[model_name], True
    return LOADED_MODELS[model_name], False


def predict(input_tensor, model):
    with torch.no_grad():
        output = model(input_tensor)

    prob = torch.nn.functional.softmax(output[0], dim=0)
    high_prob, pred_label = torch.topk(prob, 1)

    predicted_class = LABELS[pred_label[0].item()]
    probability = high_prob[0].item()
    return str(predicted_class), probability


def main(args):
    try:
        model_name = args.get("model_name", "resnet18")
        image_b64 = args.get("image_data", None)

        if image_b64 is None:
            return {"body": {"Failed": "No input image!", "cold": False}}

        start = time.perf_counter()

        image_bytes = base64.b64decode(image_b64)
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        preprocessed_input = pre_process(image)
        preprocess_time = time.perf_counter()

        model, was_cold = get_model(model_name)
        model_access_time = time.perf_counter()

        prediction, probability = predict(preprocessed_input, model)

        if DEVICE == "cuda":
            torch.cuda.synchronize()

        prediction_time = time.perf_counter()

        return {
            "body": {
                "Success! Using model": model_name,
                "Prediction Class": prediction,
                "Probability": probability,
                "cold": was_cold,
                "Device": DEVICE,
                "CUDA Available": torch.cuda.is_available(),
                "GPU Name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else "No GPU",
                "Total Time (s)": prediction_time - start,
                "Pre-process Time (s)": preprocess_time - start,
                "Model Access Time (s)": model_access_time - preprocess_time,
                "Inference Time (s)": prediction_time - model_access_time,
                "latency": prediction_time - start,
                "start": time.time() - (prediction_time - start),
                "end": time.time()
            }
        }

    except Exception as e:
        raise e