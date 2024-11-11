#!/bin/python3
import argparse
import subprocess
import os
import shutil

argparser = argparse.ArgumentParser()
argparser.add_argument("--repo", help="Repository the image will be in", required=False, default="alfuerst")
argparser.add_argument("--hub", help="Hub to push docker image too", required=False, default="docker.io")
argparser.add_argument("--version", help="Version to tag images with.", required=False, default="latest")
argparser.add_argument("--skip-push", '-s', help="Don't push images to remote.", action="store_true")
args = argparser.parse_args()

def image_name(func_name):
  return f"{args.repo}/{func_name}-iluvatar-gpu:{args.version}"

def base_image_name(func_name):
  return f"{args.repo}/{func_name}:{args.version}"

def action_base():
  return f"{args.hub}/{args.repo}/iluvatar-action-gpu-base"

def docker_cmd(args, log_file=None):
  args.insert(0, "docker")
  completed = subprocess.run(args=args, stdout=log_file, stderr=log_file, text=True)
  completed.check_returncode()

def push(func_name, log):
  docker_cmd(["push", image_name(func_name)], log)

def build(path, function_name, dockerfile_base, basename):
  shutil.copy("../server.py", path)
  shutil.copy(dockerfile_base, path)
  log_file = open(os.path.join(path, "build.log"), 'w')

  try:
    if os.path.exists(os.path.join(path, "Dockerfile")):
      base_args = ["build", "--build-arg", f"ACTION_BASE={action_base()}", "--file", os.path.join(path, dockerfile_base), "-t", base_image_name(basename), path]
      docker_cmd(base_args, log_file)

      img_args = ["build", "--build-arg", f"ACTION_BASE={action_base()}", "--file", os.path.join(path, "Dockerfile"), "-t", image_name(function_name), path]
      docker_cmd(img_args, log_file)

    else:
      img_args = ["build", "--build-arg", f"ACTION_BASE={action_base()}", "--file", os.path.join(path, dockerfile_base), "-t", image_name(function_name), path]
      docker_cmd(img_args, log_file)

    if not args.skip_push:
      push(function_name, log_file)
  finally:
    os.remove(os.path.join(path, "server.py"))
    os.remove(os.path.join(path, dockerfile_base))

if __name__ == "__main__":
  funcs_dir = "./functions"
  for func_name in os.listdir(funcs_dir):
    if os.path.isdir(os.path.join(funcs_dir, func_name)):
      dir = os.path.join(funcs_dir, func_name)
      build(dir, func_name, "../Dockerfile.cpu", "iluvatar-action-base")
      