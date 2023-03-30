from shutil import ExecError
from flask import Flask, request, jsonify
import os, traceback, json
from datetime import datetime
from http import HTTPStatus

FORMAT="%Y-%m-%d %H:%M:%S:%f+%z"
cold=True
#  Store import error of user code
msg = None
try:
  # import user code on startup
  from main import main
except:
  msg = traceback.format_exc()
  # we still need a "main" function to report an error occured on
  def main(args):
    return {"user_error":msg, "start": datetime.strftime(datetime.now(), FORMAT), "end": datetime.strftime(datetime.now(), FORMAT), "was_cold":True, "duration_sec": 0.0}, HTTPStatus.UNPROCESSABLE_ENTITY

app = Flask(__name__)

@app.route('/')
def index():
  global msg
  if msg is not None:
    return {"Status":"code import error", "user_error":msg}, HTTPStatus.INTERNAL_SERVER_ERROR
  else:
    return jsonify({"Status":"OK"})

@app.route('/invoke', methods=["POST"])
def invoke():
  global cold
  was_cold = cold
  cold = False
  try:
    json_input = request.get_json()
  except Exception as e:
    # Usually comes from malformed json arguments
    return jsonify({"platform_error":str(e), "was_cold":was_cold}), 500

  start = datetime.now()
  try:
    ret = main(json_input)
    end = datetime.now()
    duration = (end - start).total_seconds()
    if type(ret) is tuple:
      # If your custon main function from above was used (failed import), we return it's results here
      return ret
    else:
      # wrap user output with our own recorded information
      return jsonify({"user_result":json.dumps(ret), "start": datetime.strftime(start, FORMAT), "end": datetime.strftime(end, FORMAT), "was_cold":was_cold, "duration_sec": duration})
  except Exception as e:
    # User code failed, report the error with the rest of our information
    end = datetime.now()
    duration = (end - start).total_seconds()
    return jsonify({"user_error":str(e), "start": datetime.strftime(start, FORMAT), "end": datetime.strftime(end, FORMAT), "was_cold":was_cold, "duration_sec": duration}), HTTPStatus.UNPROCESSABLE_ENTITY

port = os.environ.get("__IL_PORT", 8080)
host = os.environ.get("__IL_HOST", "0.0.0.0")

if __name__ == "__main__":
  app.run(host=host, port=port)
