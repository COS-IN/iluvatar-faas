#!/bin/python3
import argparse
import subprocess
import os
import shutil

argparser = argparse.ArgumentParser()
argparser.add_argument("--repo", help="Repository the image will be in", required=False, default="cosinfaas")
argparser.add_argument("--hub", help="Hub to push docker image too", required=False, default="docker.io")
argparser.add_argument("--version", help="Version to tag images with.", required=False, default="latest")
argparser.add_argument("--skip-push", '-s', help="Don't push images to remote.", action="store_true")
argparser.add_argument("--skip-cpu", help="Don't build CPU images.", action="store_true")
argparser.add_argument("--skip-gpu", help="Don't build CPU images.", action="store_true")
args = argparser.parse_args()

hooks_dir = "./driver-hooks"

def image_name(func_name, compute, server):
  return f"{args.hub}/{args.repo}/{func_name}-iluvatar-{compute}-{server}:{args.version}"

def action_base(compute, server):
  return f"{args.hub}/{args.repo}/iluvatar-{compute}-{server}:{args.version}"

def docker_cmd(args, log_file=None):
  args.insert(0, "docker")
  completed = subprocess.run(args=args, stdout=log_file, stderr=log_file, text=True)
  completed.check_returncode()

def push(image, log):
  docker_cmd(["push", image], log)

def build_action_base(dockerfile_base, compute, server):
    server_file=None
    if server == "http":
        server_file ="server.py"
    elif server == "unix":
        server_file ="socket_server.py"

    base_name = action_base(compute, server)
    base_args = ["build", "--build-arg", f"SERVER_PY={server_file}", "--file", dockerfile_base, "-t", base_name, "."]
    docker_cmd(base_args)
    if not args.skip_push:
        push(base_name, None)

def build(path, function_name, dockerfile_base, compute, server):
  shutil.copy("gunicorn.conf.py", path)
  if server == "http":
      shutil.copy("server.py", path)
  elif server == "unix":
      shutil.copy("socket_server.py", os.path.join(path,"server.py"))
  shutil.copy(dockerfile_base, path)
  shutil.copytree(
      os.path.abspath(hooks_dir), os.path.join(path, hooks_dir), dirs_exist_ok=True
  )
  log_file = open(os.path.join(path, "build.log"), 'w')

  try:
    if os.path.exists(os.path.join(path, "Dockerfile")):
      img_args = ["build", "--build-arg", f"ACTION_BASE={action_base(compute, server)}",  "--build-arg", "SERVER_PY=server.py", "--file", os.path.join(path, "Dockerfile"), "-t", image_name(function_name, compute, server), path]
      docker_cmd(img_args, log_file)

    else:
      img_args = ["build", "--build-arg", f"ACTION_BASE={action_base(compute, server)}",  "--build-arg", "SERVER_PY=server.py", "--file", os.path.join(path, dockerfile_base), "-t", image_name(function_name, compute, server), path]
      docker_cmd(img_args, log_file)

    if not args.skip_push:
      push(image_name(func_name, compute, server), log_file)
  finally:
    os.remove(os.path.join(path, "server.py"))
    os.remove(os.path.join(path, "gunicorn.conf.py"))
    os.remove(os.path.join(path, dockerfile_base))
    shutil.rmtree(os.path.join(path, hooks_dir))

def should_build(funcs_dir: str, func_name: str) -> bool:
    folder = os.path.join(funcs_dir, func_name)
    if not os.path.isdir(folder):
        return False
    dock = os.path.join(folder, "Dockerfile")
    return os.path.exists(dock)

if __name__ == "__main__":
  if not args.skip_cpu:
      funcs_dir = "./functions"
      for server in ["http", "unix"]:
          build_action_base("CPU.Dockerfile", "cpu", server)

          for func_name in os.listdir(funcs_dir):
            if should_build(funcs_dir, func_name):
              print("building", func_name)
              dir = os.path.join(funcs_dir, func_name)
              build(dir, func_name, "CPU.Dockerfile", "cpu", server)

  if not args.skip_gpu:
      funcs_dir = "./gpu-functions"
      for server in ["http", "unix"]:
          build_action_base("GPU.Dockerfile", "gpu", server)

          for func_name in os.listdir(funcs_dir):
              if should_build(funcs_dir, func_name):
                  dir = os.path.join(funcs_dir, func_name)
                  build(dir, func_name, "GPU.Dockerfile", "gpu", server)
