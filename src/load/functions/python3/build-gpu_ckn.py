#!/bin/python3
import argparse
import subprocess
import os
import shutil
import multiprocessing as mp

argparser = argparse.ArgumentParser()
argparser.add_argument(
    "--repo", help="Repository the image will be in", required=False, default="plalelab"
)
argparser.add_argument(
    "--hub", help="Hub to push docker image too", required=False, default=""
)
argparser.add_argument(
    "--version", help="Version to tag images with.", required=False, default="latest"
)
argparser.add_argument(
    "--skip-push", "-s", help="Don't push images to remote.", action="store_true"
)
args = argparser.parse_args()


def image_name(func_name, server):
    parts = []
    if args.hub:
        parts.append(args.hub)
    if args.repo:
        parts.append(args.repo)
    parts.append(f"{func_name}-iluvatar-gpu-{server}:{args.version}")
    return "/".join(parts)


def base_image_name(func_name):
    parts = []
    if args.hub:
        parts.append(args.hub)
    if args.repo:
        parts.append(args.repo)
    parts.append(f"{func_name}:{args.version}")
    return "/".join(parts)


def action_base():
    parts = []
    if args.hub:
        parts.append(args.hub)
    if args.repo:
        parts.append(args.repo)
    parts.append("iluvatar-action-gpu-base")
    return "/".join(parts)


hooks_dir = "./driver-hooks"


def docker_cmd(args, log_file=None):
    args.insert(0, "docker")
    completed = subprocess.run(args=args, stdout=log_file, stderr=log_file, text=True)
    completed.check_returncode()


def push(func_name, log_file, server):
    docker_cmd(["push", image_name(func_name, server)], log_file)


def build(path, function_name, dockerfile_base, basename, server):
    shutil.copy("gunicorn.conf.py", path)
    if server == "http":
        shutil.copy("server.py", path)
    elif server == "unix":
        shutil.copy("socket_server.py", os.path.join(path, "server.py"))
    shutil.copy(dockerfile_base, path)
    shutil.copy(dockerfile_base, path)
    shutil.copytree(
        os.path.abspath(hooks_dir), os.path.join(path, hooks_dir), dirs_exist_ok=True
    )
    log_file = open(os.path.join(path, "build.log"), "w")

    try:
        if os.path.exists(os.path.join(path, "Dockerfile")):
            base_args = [
                "build",
                "--build-arg",
                f"ACTION_BASE={action_base()}",
                "--file",
                os.path.join(path, dockerfile_base),
                "-t",
                base_image_name(basename),
                path,
            ]
            docker_cmd(base_args, log_file)

            img_args = [
                "build",
                "--build-arg",
                f"ACTION_BASE={action_base()}",
                "--file",
                os.path.join(path, "Dockerfile"),
                "-t",
                image_name(function_name, server),
                path,
            ]
            docker_cmd(img_args, log_file)

        else:
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

        if not args.skip_push:
            push(function_name, log_file, server)
    finally:
        os.remove(os.path.join(path, "server.py"))
        os.remove(os.path.join(path, "gunicorn.conf.py"))
        os.remove(os.path.join(path, dockerfile_base))
        shutil.rmtree(os.path.join(path, hooks_dir))


# if __name__ == "__main__":
#     funcs_dir = "./gpu-functions"
#     target_funcs = ["cnn_image_classification_gpu"]
#
#     for server in ["http", "unix"]:
#         for func_name in target_funcs:
#             dir = os.path.join(funcs_dir, func_name)
#             if os.path.isdir(dir):
#                 build(
#                     dir,
#                     func_name,
#                     "DockerfileCKNFaaS.gpu",
#                     "iluvatar-action-gpu-base",
#                     server
#                 )
if __name__ == "__main__":
    funcs_dir = "./functions_ckn_faaS_gpu"
    target_funcs = [
        "ckn_faas_mobilenet_v3_small",
        "ckn_faas_resnet18",
        "ckn_faas_resnet34",
        "ckn_faas_resnet50",
        "ckn_faas_resnet101",
        "ckn_faas_vit_b_16",
    ]

    for server in ["http", "unix"]:
        for func_name in target_funcs:
            dir = os.path.join(funcs_dir, func_name)
            if os.path.isdir(dir):
                build(
                    dir,
                    func_name,
                    "DockerfileCKNFaaS.gpu",
                    "iluvatar-action-gpu-base",
                    server
                )

# if __name__ == "__main__":
#     funcs_dir = "./functions_ckn_faaS_gpu"
#     target_funcs = ["cnn_image_classification_gpu"]
#
#     for server in ["http", "unix"]:
#         for func_name in target_funcs:
#             dir = os.path.join(funcs_dir, func_name)
#             if os.path.isdir(dir):
#                 build(
#                     dir,
#                     func_name,
#                     "DockerfileCKNFaaS.gpu",
#                     "iluvatar-action-gpu-base",
#                     server
#                 )

    # with mp.Pool() as p:
    #   results = []
    #   funcs_dir = "./gpu-functions"
    #   for func_name in os.listdir(funcs_dir):
    #     if os.path.isdir(os.path.join(funcs_dir, func_name)):
    #       dir = os.path.join(funcs_dir, func_name)
    #       results.append(p.apply_async(build, [dir, func_name, "Dockerfile.gpu", "iluvatar-action-gpu-base"]))
    #   for r in results:
    #     r.get()
