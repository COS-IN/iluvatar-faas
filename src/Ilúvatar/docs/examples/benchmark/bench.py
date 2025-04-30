import sys, os, shutil

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from copy import deepcopy
import subprocess
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

build_level = BuildTarget.RELEASE
results_dir = os.path.join(os.getcwd(), "results")
os.makedirs(results_dir, exist_ok=True)
worker_log_dir = os.path.join(os.getcwd(), "results", "tmp")
os.makedirs(worker_log_dir, exist_ok=True)
log_file = os.path.join(results_dir, "orchestration.log")

# build the solution
rust_build(ILU_HOME, None, build_level)

ansible_dir = os.path.join(ILU_HOME, "ansible")
kwargs = {
    "ilu_home": ILU_HOME,
    "ansible_hosts_addrs": "@"
    + os.path.join(ansible_dir, "group_vars/local_addresses.yml"),
    "ansible_host_file": os.path.join(
        ansible_dir, "environments/", "local", "hosts.ini"
    ),
    "host": "127.0.0.1",
    "ansible_dir": ansible_dir,
    "build_level": build_level,
    "cores": 3,
    "memory": 1024 * 10,
    "worker_status_ms": 1000,
    "worker_log_dir": worker_log_dir,
    "snapshotter": "overlayfs",
}


def run_benchmark(log_file):
    bindir = os.path.join(
        ILU_HOME, "target", "x86_64-unknown-linux-gnu", str(build_level)
    )
    load_pth = os.path.join(
        bindir,
        "iluvatar_load_gen",
    )
    src = os.path.join(
        bindir,
        "resources",
        "cpu-benchmark.csv",
    )
    args = [
        load_pth,
        "benchmark",
        "--out-folder",
        results_dir,
        "--port",
        str(8070),
        "--host",
        kwargs["host"],
        "--cold-iters",
        "1",
        "--warm-iters",
        "1",
        "--target",
        str(RunTarget.WORKER),
        "--function-file",
        src,
    ]
    env = deepcopy(os.environ)
    env["RUST_BACTRACE"] = "1"
    with open(log_file, "a") as log_file:
        completed = subprocess.run(
            args=args,
            stdout=log_file,
            stderr=log_file,
            text=True,
            env=env,
        )
        completed.check_returncode()


pre_run_cleanup(log_file, results_dir, **kwargs)
try:
    run_ansible(log_file, **kwargs)
    run_benchmark(log_file)
finally:
    remote_cleanup(log_file, results_dir, **kwargs)
