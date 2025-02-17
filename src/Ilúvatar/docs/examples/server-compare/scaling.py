import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from copy import deepcopy
import subprocess
import multiprocessing
import argparse
from load.run.run_trace import (
    rust_build,
    BuildTarget,
    RunTarget,
    ansible_clean,
    copy_logs,
    pre_run_cleanup,
    remote_cleanup,
    run_ansible,
)

parser = argparse.ArgumentParser()
parser.add_argument('-d', "--duration", default=120, type=int, help="Per-scaling, in seconds")
script_args = parser.parse_args()

build_level = BuildTarget.RELEASE
# build the solution
rust_build(ILU_HOME, None, build_level)

ansible_dir = os.path.join(ILU_HOME, "ansible")

def run_scaling(threads, server):
    out_dir = os.path.join(os.getcwd(), "results", server, str(threads))
    worker_log_dir= os.path.join(out_dir, "tmp")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(worker_log_dir, exist_ok=True)
    log_file = os.path.join(out_dir, "orchestration.log")
    kwargs = {
        "ilu_home": ILU_HOME,
        "ansible_hosts_addrs": "@"
                               + os.path.join(ansible_dir, "group_vars/local_addresses.yml"),
        "ansible_host_file": os.path.join(
            ansible_dir, "environments/local/hosts.ini"
        ),
        "host": "127.0.0.1",
        "ansible_dir": ansible_dir,
        "build_level": build_level,
        "cores": 4,
        "memory": 1024 * 5,
        "worker_status_ms": 500,
        "worker_log_dir": worker_log_dir,
    }
    print(kwargs["worker_log_dir"])
    with open(log_file, "w") as log:
        pre_run_cleanup(log, out_dir, **kwargs)
        try:
            run_ansible(log, **kwargs)
            bindir = os.path.join(
                ILU_HOME, "target", "x86_64-unknown-linux-gnu", str(build_level)
            )
            load_pth = os.path.join(
                bindir,
                "iluvatar_load_gen",
            )
            # 'hello' image has minimal actual execution time, so highlights platform performance differences
            img = f"docker.io/alfuerst/hello-iluvatar-action-{server}:latest"
            args = [
                load_pth,
                "scaling",
                "--out-folder",
                out_dir,
                "--port",
                str(8070),
                "--host",
                kwargs["host"],
                "--start",
                str(threads),
                "--end",
                str(threads),
                "--image",
                img,
                "--memory-mb",
                "1024",
                "--compute",
                "cpu",
                "--isolation",
                "containerd",
                "--server",
                server,
                "--duration",
                str(script_args.duration),
                "--target",
                "worker",
            ]
            env = deepcopy(os.environ)
            env["RUST_BACkTRACE"] = "1"
            completed = subprocess.run(
                args=args,
                stdout=log,
                stderr=log,
                text=True,
                env=env,
            )
            completed.check_returncode()
        finally:
            remote_cleanup(log, out_dir, **kwargs)

mx = multiprocessing.cpu_count()
points = 5
threads = list(range(1, multiprocessing.cpu_count(), mx // points))
threads[-1] = mx
for server in ["http", "unix"]:
    for tds in threads:
        run_scaling(tds, server)

