# # In here, there should be a middleware to fetch the data and model switch name from ckn, which is a REST request, and then send to the iluvatar worker, which is a http request.import torch
# from PIL import Image
# from torchvision import transforms
# from torchvision import models
# import torch
# import io
# import time
# import base64
# import os
#
#
# torch.set_num_threads(3)
# torch.set_num_interop_threads(1)
#
# cold = True
#
# def pre_process(image):
#     """
#     Pre-processes the image to allow the image to be fed into the PyTorch model.
#     :image: image.
#     :return: Pre-processed image tensor.
#     """
#     input_image = image.convert("RGB")  # Ensure the image is in RGB format
#     preprocess = transforms.Compose([
#         transforms.Resize(256),
#         transforms.CenterCrop(224),
#         transforms.ToTensor(),
#         transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
#     ])
#     input_tensor = preprocess(input_image)
#     input_batch = input_tensor.unsqueeze(0)
#     return input_batch
#
#
# def predict(input,model):
#     """
#     Predicting the class for a given pre-processed input
#     :param input:
#     :return: prediction class
#     """
#     with torch.no_grad():
#         output = model(input)
#     prob = torch.nn.functional.softmax(output[0], dim=0)
#     with open("imagenet_classes.txt", "r") as f:
#         labels = [s.strip() for s in f.readlines()]
#     # retrieve top probability for the input
#     high_prob, pred_label = torch.topk(prob, 1)
#     print(f"Predicted class: {labels[pred_label[0]]}, Probability: {high_prob[0].item():.4f}")
#     return str((labels[pred_label[0]])), high_prob[0].item()
#
# def load_model(model_name):
#     return torch.hub.load('pytorch/vision:v0.10.0', model_name, pretrained=True)
#
# def main(args):
#     try:
#         global cold
#         model_name = args.get("model_name", 'mobilenet_v3_small')
#         image_b64 = args.get("image_data", None)
#         if image_b64 is not None:
#             was_cold = cold
#             cold = False
#             start = time.perf_counter()
#             image_bytes = base64.b64decode(image_b64)
#             image = Image.open(io.BytesIO(image_bytes))
#             # image.save('input_image.jpeg', format="JPEG")
#             preprocessed_input = pre_process(image)
#             preprocess_time = time.perf_counter()
#
#             model_path = f'model_{model_name}.pth'
#             was_cold = not os.path.exists(model_path)
#             if was_cold:
#                 model = load_model(model_name)
#                 torch.save(model, model_path)
#             else:
#                 model = torch.load(model_path, weights_only=False)
#             model.eval()
#             load_model_time = time.perf_counter()
#             prediction, probability = predict(preprocessed_input, model)
#             prediction_time = time.perf_counter()
#             return {
#                 "body": {
#                     "Success! Using model": model_name,
#                     "Prediction Class": prediction,
#                     "Probability": probability,
#                     "Cold": was_cold,
#                     "Total Time (s)": prediction_time - start,
#                     "Pre-process Time (s)": preprocess_time - start,
#                     "Model Load Time (s)": load_model_time - preprocess_time,
#                     "Inference Time (s)": prediction_time - load_model_time
#                 }
#             }
#         else:
#             return {"body": {"Failed": "No input image!"}}
#     except Exception as e:
#         import traceback
#         return {
#             "error": str(e),
#             "traceback": traceback.format_exc(),
#             "body": "Model crashed"
#         }, 500



from PIL import Image
from torchvision import transforms
import torch
import io
import time
import base64

torch.set_num_threads(3)
torch.set_num_interop_threads(1)

# Keep loaded models in memory
LOADED_MODELS = {}

# Load labels once
with open("imagenet_classes.txt", "r") as f:
    LABELS = [s.strip() for s in f.readlines()]


def pre_process(image):
    """
    Pre-processes the image to allow the image to be fed into the PyTorch model.
    """
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
    input_batch = input_tensor.unsqueeze(0)
    return input_batch


def predict(input_tensor, model):
    """
    Predict the class for a given pre-processed input.
    """
    with torch.no_grad():
        output = model(input_tensor)

    prob = torch.nn.functional.softmax(output[0], dim=0)
    high_prob, pred_label = torch.topk(prob, 1)

    predicted_class = LABELS[pred_label[0]]
    probability = high_prob[0].item()

    print(f"Predicted class: {predicted_class}, Probability: {probability:.4f}")
    return str(predicted_class), probability


def load_model(model_name):
    """
    Load the model once and prepare it for CPU inference.
    """
    model = torch.hub.load("pytorch/vision:v0.10.0", model_name, pretrained=True)
    model.to("cpu")
    model.eval()
    return model


def get_model(model_name):
    """
    Return a model from memory if already loaded.
    """
    global LOADED_MODELS

    if model_name not in LOADED_MODELS:
        LOADED_MODELS[model_name] = load_model(model_name)
        return LOADED_MODELS[model_name], True

    return LOADED_MODELS[model_name], False


def main(args):
    try:
        model_name = args.get("model_name", "mobilenet_v3_small")
        image_b64 = args.get("image_data", None)

        if image_b64 is None:
            return {"body": {"Failed": "No input image!"}}

        start = time.perf_counter()

        image_bytes = base64.b64decode(image_b64)
        image = Image.open(io.BytesIO(image_bytes))
        preprocessed_input = pre_process(image)
        preprocess_time = time.perf_counter()

        model, was_cold = get_model(model_name)
        model_access_time = time.perf_counter()

        prediction, probability = predict(preprocessed_input, model)
        prediction_time = time.perf_counter()

        return {
            "body": {
                "Success! Using model": model_name,
                "Prediction Class": prediction,
                "Probability": probability,
                "Cold": was_cold,
                "Torch Threads": torch.get_num_threads(),
                "Torch Interop Threads": torch.get_num_interop_threads(),
                "Total Time (s)": prediction_time - start,
                "Pre-process Time (s)": preprocess_time - start,
                "Model Access Time (s)": model_access_time - preprocess_time,
                "Inference Time (s)": prediction_time - model_access_time
            }
        }

    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "body": "Model crashed"
        }, 500
