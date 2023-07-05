msg = "good"
import traceback
try:
  import os
  import pickle
  import torch
  import rnn
  from time import time
  import urllib
except Exception as e:
  msg = traceback.format_exc()
  print(msg)

tmp = "/tmp/"
cold = True
model_url="https://raw.githubusercontent.com/ddps-lab/serverless-faas-workbench/master/dataset/model/rnn_model.pth"
params_url="https://raw.githubusercontent.com/ddps-lab/serverless-faas-workbench/master/dataset/model/rnn_params.pkl"

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(device)

"""
Language
 - Italian, German, Portuguese, Chinese, Greek, Polish, French
 - English, Spanish, Arabic, Crech, Russian, Irish, Dutch
 - Scottish, Vietnamese, Korean, Japanese
"""
def main(args):
  global cold
  was_cold = cold
  cold = False
  try:
    start = time()
    
    language = args.get('language', "Irish")
    start_letters = args.get('start_letters', "ABCDEFGHIJKLMNOP")
    model_parameter_object_key = args.get('model_parameter_object_key', "rnn_params.pkl")
    model_object_key = args.get('model_object_key', "rnn_model.pth")

    # Check if models are available
    # Download model from S3 if model is not already present
    parameter_path = tmp + model_parameter_object_key
    model_path = tmp + model_object_key

    if not os.path.isfile(parameter_path):
        urllib.request.urlretrieve (params_url, parameter_path)

    if not os.path.isfile(model_path):
      urllib.request.urlretrieve (model_url, model_path)

    with open(parameter_path, 'rb') as pkl:
        params = pickle.load(pkl)

    all_categories = params['all_categories']
    n_categories = params['n_categories']
    all_letters = params['all_letters']
    n_letters = params['n_letters']

    rnn_model = rnn.RNN(n_letters, 128, n_letters, all_categories, n_categories, all_letters, n_letters, device)
    rnn_model.load_state_dict(torch.load(model_path))
    rnn_model.eval()

    output_names = list(rnn_model.samples(language, start_letters))
    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end, "output":output_names }}
  except Exception as e:
    err = str(e)
    try:
        trace = traceback.format_exc()
    except Exception as fug:
        trace = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "trace":trace, "cold":was_cold }}

if __name__ == '__main__':
    main({})
