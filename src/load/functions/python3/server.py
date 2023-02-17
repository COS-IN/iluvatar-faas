from shutil import ExecError
from flask import Flask, request, jsonify
import os, traceback, json
from datetime import datetime

FORMAT="%Y-%m-%d %H:%M:%S:%f+%z"
cold=True
# import user code
msg = None
try:
  from main import main
except:
  msg = traceback.format_exc()
  def main(args):
    return {"Code import error":msg}, 500

app = Flask(__name__)

@app.route('/')
def index():
  global msg
  if msg is not None:
    return {"Status":"code import error", "code import error":msg}, 500
  else:
    return jsonify({"Status":"OK"})

@app.route('/invoke', methods=["POST"])
def invoke():
  # TODO: enforce concurrency limit
  # TODO: security to confirm invocation is from authorized source?
  global cold
  was_cold = cold
  cold = False
  try:
    json_input = request.get_json()
  except Exception as e:
    return jsonify({"platform_error":str(e), "was_cold":was_cold}), 500

  start = datetime.now()
  try:
    ret = main(json_input)
    end = datetime.now()
    duration = (end - start).total_seconds()
    if type(ret) is tuple:
      return ret
    else:
      return jsonify({"user_result":json.dumps(ret), "start": datetime.strftime(start, FORMAT), "end": datetime.strftime(end, FORMAT), "was_cold":was_cold, "duration_sec": duration})
  except Exception as e:
    end = datetime.now()
    duration = (end - start).total_seconds()
    return jsonify({"user_error":str(e), "start": datetime.strftime(start, FORMAT), "end": datetime.strftime(end, FORMAT), "was_cold":was_cold, "duration_sec": duration}), 500

port = os.environ.get("__IL_PORT", 8080)
host = os.environ.get("__IL_HOST", "0.0.0.0")

if __name__ == "__main__":
  app.run(host=host, port=port)
