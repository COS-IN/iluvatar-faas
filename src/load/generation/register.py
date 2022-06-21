import argparse
import subprocess
import os

argparser = argparse.ArgumentParser()
argparser.add_argument("--test", '-t')
argparser.add_argument("--clipth", '-c', default="/home/alex/repos/efaas/src/Ilúvatar/target/debug/iluvatar_worker_cli", required=False)
argparser.add_argument("--worker", '-w', default="local", required=False)

args = argparser.parse_args()

actions = os.listdir("../functions/python3/functions")
for dir in actions:
  image = "docker.io/alfuerst/{}-iluvatar-action:latest".format(dir)
  proc_args = [args.clipth, "--worker", args.worker, "register", "--name", dir, "--version", "0.0.1", "--memory", "512", "--cpu", "1", "--image", image]
  # proc_args = [args.clipth, "--worker", args.worker, "register"]
  print(proc_args)
  cli = subprocess.run(args=proc_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  if cli.returncode != 0:
      print(cli.stderr)
      cli.check_returncode()