from shutil import ExecError
from flask import Flask, request, jsonify
import os, traceback, json
from datetime import datetime
from http import HTTPStatus
from math import ceil

DRIVER="libgpushare.so"
def driver_enabled() -> bool:
  has_gpu = os.path.isfile("/usr/bin/nvidia-smi")
  if "LD_PRELOAD" in os.environ and DRIVER in os.environ["LD_PRELOAD"]:
    return has_gpu
  return False

gpushare = None
if driver_enabled():
  import ctypes
  gpushare = ctypes.CDLL(DRIVER, mode=os.RTLD_GLOBAL)
  gpushare.total_cuda_allocations.restype = ctypes.c_double

FORMAT="%Y-%m-%d %H:%M:%S:%f+%z"
cold=True
#  Store import error of user code
import_msg = None
try:
  # import user code on startup
  from main import main
except:
  import_msg = traceback.format_exc()

app = Flask(__name__)

@app.route('/')
def index():
  global import_msg
  if import_msg is not None:
    return {"Status":"code import error", "user_error":import_msg}, HTTPStatus.INTERNAL_SERVER_ERROR
  else:
    return jsonify({"Status":"OK"})

@app.route('/to_dev', methods=["PUT"])
def to_dev():
  if gpushare is None:
    return jsonify({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold}), HTTPStatus.INTERNAL_SERVER_ERROR
  return jsonify({"Status":gpushare.madviseToDevice()})

@app.route('/off_dev', methods=["PUT"])
def off_dev():
  if gpushare is None:
    return jsonify({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold}), HTTPStatus.INTERNAL_SERVER_ERROR
  return jsonify({"Status":gpushare.madviseToHost()})

@app.route('/prefetch_host', methods=["PUT"])
def prefetch_host():
  if gpushare is None:
    return jsonify({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold}), HTTPStatus.INTERNAL_SERVER_ERROR
  return jsonify({"Status":gpushare.prefetchToHost()})

@app.route('/prefetch_dev', methods=["PUT"])
def prefetch_dev():
  if gpushare is None:
    return jsonify({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold}), HTTPStatus.INTERNAL_SERVER_ERROR
  return jsonify({"Status":gpushare.prefetchToDevice()})

@app.route('/prefetch_stream_host', methods=["PUT"])
def prefetch_stream_host():
  if gpushare is None:
    return jsonify({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold}), HTTPStatus.INTERNAL_SERVER_ERROR
  return jsonify({"Status":gpushare.prefetchStreamToHost()})
@app.route('/prefetch_stream_dev', methods=["PUT"])
def prefetch_stream_dev():
  if gpushare is None:
    return jsonify({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold}), HTTPStatus.INTERNAL_SERVER_ERROR
  return jsonify({"Status":gpushare.prefetchStreamToDevice()})

@app.route('/gpu_mem', methods=["GET"])
def gpu_mem():
  if gpushare is None:
    return jsonify({"platform_error":"gpushare was not preloaded, but required for call", "was_cold":cold}), HTTPStatus.INTERNAL_SERVER_ERROR
  return jsonify({"gpu_allocation_mb": ceil(gpushare.total_cuda_allocations())})

def append_metadata(user_ret, start, end, was_cold, success=True):
  duration = (end - start).total_seconds()
  ret = {"start": datetime.strftime(start, FORMAT), "end": datetime.strftime(end, FORMAT), "was_cold":was_cold, "duration_sec": duration}
  if gpushare is not None:
    ret["gpu_allocation_mb"] = ceil(gpushare.total_cuda_allocations())
  if success:
    ret["user_result"] = json.dumps(user_ret)
  else:
    ret["user_error"] = json.dumps(user_ret)
  return jsonify(ret)

@app.route('/invoke', methods=["POST"])
def invoke():
  global cold
  was_cold = cold
  cold = False
  if import_msg is not None:
    # If your custom main function from above was used (failed import), we return it's results here
    return append_metadata(import_msg, start=datetime.now(), end=datetime.now(), was_cold=was_cold, success=False), HTTPStatus.UNPROCESSABLE_ENTITY

  try:
    json_input = request.get_json()
  except Exception as e:
    # Usually comes from malformed json arguments
    return jsonify({"platform_error":str(e), "was_cold":was_cold}), HTTPStatus.INTERNAL_SERVER_ERROR

  if gpushare is not None:
    gpushare.ensure_on_device()
    gpushare.check_async_prefetch()

  start = datetime.now()
  try:
    ret = main(json_input)
    end = datetime.now()
    # wrap user output with our own recorded information
    return append_metadata(ret, start, end, was_cold)
  except Exception as e:
    # User code failed, report the error with the rest of our information
    end = datetime.now()
    return append_metadata(e, start, end, was_cold, success=False), HTTPStatus.UNPROCESSABLE_ENTITY

port = os.environ.get("__IL_PORT", 8080)
host = os.environ.get("__IL_HOST", "0.0.0.0")

if __name__ == "__main__":
  app.run(host=host, port=port)
