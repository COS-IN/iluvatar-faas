msg = "good"
import traceback
import os

try:
  import os
  import pickle
  import onnxruntime as rt
  from time import time
  from transformers import RobertaForSequenceClassification, RobertaTokenizer
  import numpy as np
except Exception as e:
  msg = traceback.format_exc()
  print(msg)
  exit(-1)


tmp = "/tmp/"
cold = True
model_object_key = "roberta-sequence-classification-9.onnx"
model_path = tmp + model_object_key

def has_gpu() -> bool:
  return True 

# Check if models are available
# Download model if model is not already present
if not os.path.isfile(model_path):
  print("Error: model not found!")
  exit(-1)

providers=['CPUExecutionProvider']
if has_gpu():
  providers=['CUDAExecutionProvider', 'CPUExecutionProvider']
session = rt.InferenceSession(model_path, providers=providers)
   
input_id = session.get_inputs()[0].name

def main(args):
  global cold
  was_cold = cold
  cold = False
  try:
    start = time()
    
    input_text = args.get('input', "This film is so good")

    tokenizer = RobertaTokenizer.from_pretrained('roberta-base')
    input_ids = [np.array(tokenizer.encode(input_text, add_special_tokens=True))]

    # for input_meta in session.get_inputs():
      # print(input_meta)
    # print(input_ids, to_numpy(input_ids))
    results = session.run([], {input_id: input_ids})

    pred = np.argmax(results)
    if(pred == 0):
        results = "Prediction: negative {}".format(results)
    elif(pred == 1):
        results = "Prediction: positive {}".format(results)

    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end, "output":results }}
  except Exception as e:
    err = str(e)
    try:
        trace = traceback.format_exc()
    except Exception as fug:
        trace = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "trace":trace, "cold":was_cold }}

if __name__ == '__main__':
    main({})

