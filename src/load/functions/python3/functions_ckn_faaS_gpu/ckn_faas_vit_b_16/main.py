# from PIL import Image
# from torchvision import transforms
# import torch
# import io
# import time
# import base64
#
# DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
#
# if DEVICE == "cpu":
#     torch.set_num_threads(2)
#     torch.set_num_interop_threads(1)
#
# # Keep loaded models in memory
# LOADED_MODELS = {}
#
# # Load labels once
# with open("imagenet_classes.txt", "r") as f:
#     LABELS = [s.strip() for s in f.readlines()]
#
# def pre_process(image):
#     """
#     Pre-processes the image to allow the image to be fed into the PyTorch model.
#     """
#     input_image = image.convert("RGB")
#     preprocess = transforms.Compose([
#         transforms.Resize(256),
#         transforms.CenterCrop(224),
#         transforms.ToTensor(),
#         transforms.Normalize(
#             mean=[0.485, 0.456, 0.406],
#             std=[0.229, 0.224, 0.225]
#         ),
#     ])
#     input_tensor = preprocess(input_image)
#     input_batch = input_tensor.unsqueeze(0)
#     input_batch = input_batch.to(DEVICE)
#     return input_batch
#
# def predict(input_tensor, model):
#     """
#     Predict the class for a given pre-processed input.
#     """
#     with torch.no_grad():
#         output = model(input_tensor)
#
#     prob = torch.nn.functional.softmax(output[0], dim=0)
#     high_prob, pred_label = torch.topk(prob, 1)
#
#     predicted_class = LABELS[pred_label[0].item()]
#     probability = high_prob[0].item()
#
#     print(f"Predicted class: {predicted_class}, Probability: {probability:.4f}")
#     return str(predicted_class), probability
#
# def load_model(model_name):
#     """
#     Load the model once and prepare it for inference.
#     """
#     model = torch.hub.load("pytorch/vision:v0.10.0", model_name, pretrained=True)
#     model = model.to(DEVICE)
#     model.eval()
#     return model
#
# def get_model(model_name):
#     """
#     Return a model from memory if already loaded.
#     """
#     global LOADED_MODELS
#
#     if model_name not in LOADED_MODELS:
#         LOADED_MODELS[model_name] = load_model(model_name)
#         return LOADED_MODELS[model_name], True
#
#     return LOADED_MODELS[model_name], False
#
# def main(args):
#     try:
#         model_name = args.get("model_name", "vit_l_16")
#         image_b64 = args.get("image_data", None)
#
#         if image_b64 is None:
#             return {"body": {"Failed": "No input image!", "cold": False}}
#
#         start = time.perf_counter()
#
#         image_bytes = base64.b64decode(image_b64)
#         image = Image.open(io.BytesIO(image_bytes))
#         preprocessed_input = pre_process(image)
#         preprocess_time = time.perf_counter()
#
#         model, was_cold = get_model(model_name)
#         model_access_time = time.perf_counter()
#
#         prediction, probability = predict(preprocessed_input, model)
#
#         if DEVICE == "cuda":
#             torch.cuda.synchronize()
#
#         prediction_time = time.perf_counter()
#
#         return {
#             "body": {
#                 "Success! Using model": model_name,
#                 "Prediction Class": prediction,
#                 "Probability": probability,
#                 "cold": was_cold,
#                 "Device": DEVICE,
#                 "Torch Threads": torch.get_num_threads() if DEVICE == "cpu" else "N/A",
#                 "Torch Interop Threads": torch.get_num_interop_threads() if DEVICE == "cpu" else "N/A",
#                 "Total Time (s)": prediction_time - start,
#                 "Pre-process Time (s)": preprocess_time - start,
#                 "Model Access Time (s)": model_access_time - preprocess_time,
#                 "Inference Time (s)": prediction_time - model_access_time
#             }
#         }
#
#     except Exception as e:
#         import traceback
#         return {
#             "error": str(e),
#             "traceback": traceback.format_exc(),
#             "body": {"Failed": "Model crashed", "cold": False}
#         }, 500


from PIL import Image
from torchvision import transforms, models
import torch
import io
import time
import base64

MODEL_NAME = "vit_b_16"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

if DEVICE == "cpu":
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)

MODEL = None

with open("imagenet_classes.txt", "r") as f:
    LABELS = [s.strip() for s in f.readlines()]

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
    return input_tensor.unsqueeze(0).to(DEVICE)

def load_model():
    global MODEL
    model = models.vit_b_16(
        weights=models.ViT_B_16_Weights.DEFAULT
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

    prob = torch.nn.functional.softmax(output[0], dim=0)
    high_prob, pred_label = torch.topk(prob, 1)

    predicted_class = LABELS[pred_label[0].item()]
    probability = high_prob[0].item()
    return str(predicted_class), probability, (infer_end - infer_start)

def main(args):
    try:
        image_b64 = args.get("image_data")
        if image_b64 is None:
            return {"body": {"Failed": "No input image!", "cold": False}}

        total_start = time.perf_counter()

        image_bytes = base64.b64decode(image_b64)
        image = Image.open(io.BytesIO(image_bytes))

        preprocessed_input = pre_process(image)
        preprocess_end = time.perf_counter()

        model, was_cold = get_model()
        model_access_end = time.perf_counter()

        prediction, probability, inference_time = predict(preprocessed_input, model)
        total_end = time.perf_counter()

        return {
            "body": {
                "Success! Using model": MODEL_NAME,
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