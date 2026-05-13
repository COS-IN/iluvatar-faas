#!/bin/python3
import argparse
import subprocess
import os
import shutil

argparser = argparse.ArgumentParser()
argparser.add_argument(
    "--repo", help="Repository the image will be in", required=False, default="iluvatar-faas"
)
argparser.add_argument(
    "--registry", help="Docker registry", required=False, default=""
)
argparser.add_argument(
    "--push", help="Push the image to the registry", action="store_true"
)
argparser.add_argument(
    "--version", help="Version to tag images with.", required=False, default="latest"
)
args = argparser.parse_args()

hooks_dir = "./driver-hooks"

def image_name(func_name, server):
    name = f"{func_name}-iluvatar-gpu-{server}:{args.version}"
    if args.repo:
        name = f"{args.repo}/{name}"
    if args.registry:
        name = f"{args.registry}/{name}"
    return name

def action_base():
    parts = []
    if args.registry:
        parts.append(args.registry)
    if args.repo:
        parts.append(args.repo)
    parts.append("iluvatar-action-gpu-base")
    return "/".join(parts)

def docker_cmd(cmd_args, log_file=None):
    cmd_args.insert(0, "docker")
    completed = subprocess.run(args=cmd_args, stdout=log_file, stderr=log_file, text=True)
    completed.check_returncode()

def build(path, function_name, dockerfile_base, server):
    shutil.copy("gunicorn.conf.py", path)
    if server == "http":
        shutil.copy("server.py", path)
    elif server == "unix":
        shutil.copy("socket_server.py", os.path.join(path, "server.py"))
    
    shutil.copy(dockerfile_base, path)
    shutil.copytree(
        os.path.abspath(hooks_dir), os.path.join(path, hooks_dir), dirs_exist_ok=True
    )
    log_file = open(os.path.join(path, "build.log"), "w")

    try:
        img_args = [
            "build",
            "--build-arg",
            f"ACTION_BASE={action_base()}",
            "--file",
            os.path.join(path, dockerfile_base),
            "-t",
            image_name(function_name, server),
            path,
        ]
        docker_cmd(img_args, log_file)
        print(f"  [OK] Built {image_name(function_name, server)}")

        if args.push:
            print(f"  Pushing {image_name(function_name, server)}...")
            docker_cmd(["push", image_name(function_name, server)], log_file)
            print(f"  [OK] Pushed {image_name(function_name, server)}")

    except Exception as e:
        print(f"  [FAIL] {function_name}: {e}")
    finally:
        for f in [os.path.join(path, "server.py"), os.path.join(path, "gunicorn.conf.py"), os.path.join(path, dockerfile_base)]:
            if os.path.exists(f):
                os.remove(f)
        hooks_path = os.path.join(path, hooks_dir)
        if os.path.exists(hooks_path):
            shutil.rmtree(hooks_path)

if __name__ == "__main__":
    funcs_dir = "./functions_new"
    target_funcs = [
        "il_large_gemm",
        "il_batched_resnet50",
        "il_bert_embed",
        "il_fft_signal",
        "il_montecarlo",
        "il_conv2d_stack",
    ]

    for server in ["http", "unix"]:
        for func_name in target_funcs:
            dir = os.path.join(funcs_dir, func_name)
            if os.path.isdir(dir):
                if func_name == "il_bert_embed":
                    dockerfile = "DockerfileBert.gpu"
                else:
                    dockerfile = "DockerfileCKNFaaS.gpu"
                print(f"Building {func_name} ({server}) with {dockerfile}...")
                build(
                    dir,
                    func_name,
                    dockerfile,
                    server
                )
