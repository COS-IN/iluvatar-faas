msg = "good"
import traceback
try:
    import os, sys, subprocess
    import uuid
    from time import time
    import logging
    import urllib.request
except Exception as e:
    msg = traceback.format_exc()

workdir = "/app"
cold = True

def has_gpu() -> bool:
  return os.path.isfile("/usr/bin/nvidia-smi")  

def download_video(input_vid) -> str:
  if os.path.exists(input_vid):
    return input_vid
  elif os.path.exists(os.path.join(workdir, input_vid)):
    return os.path.join(workdir, input_vid)
  else:
    print("downloading video")
    save_pth = os.path.join(workdir, input_vid)
    urllib.request.urlretrieve(input_vid, save_pth)
    return save_pth

def main(args):
    global cold
    start = time()
    was_cold = cold
    cold = False
    try:
        bin = os.path.join(workdir, "ffmpeg")
        input_vid = args.get("video", "SampleVideo_1280x720_10mb.mp4")
        input_vid = download_video(input_vid)
        if has_gpu():
          ffmpeg_args = [bin, "-y", "-vsync", "0", "-hwaccel", "cuda", "-hwaccel_output_format", "cuda", "-i", input_vid, "-c:a", "copy", "-c:v", "h264_nvenc", "-preset", "p6", "-tune", "ll", "-b:v", "5M", "-bufsize", "5M", "-maxrate", "10M", "-qmin", "1", "-qmax", "50", "-g", "250", "-bf", "3", "-b_ref_mode", "middle", "-temporal-aq", "1", "-rc-lookahead", "20", "-i_qfactor", "0.75", "-b_qfactor", "1.1", "output.mp4"]
        else:
            ffmpeg_args = [bin, "-y", "-vsync", "0", "-i", input_vid, "-c:a", "copy", "-preset", "p6", "-tune", "ll", "-b:v", "5M", "-bufsize", "5M", "-maxrate", "10M", "-qmin", "1", "-qmax", "50", "-g", "250", "-bf", "3", "-b_ref_mode", "middle", "-temporal-aq", "1", "-rc-lookahead", "20", "-i_qfactor", "0.75", "-b_qfactor", "1.1", "output.mp4"]

        completed = subprocess.run(args=ffmpeg_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(completed.stdout)
        completed.check_returncode()
        end = time()
        return {"body": { "latency":end-start, "stdout":completed.stdout, "cold":was_cold, "start":start, "end":end }}
    except Exception as e:
        err = str(e)
        try:
            err = traceback.format_exc()
        except Exception as fug:
            err = str(fug)
        return {"body": { "cust_error":msg, "thing":err, "cold":was_cold }}
