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
log_file = os.path.join(results_dir, "orchestration.log")

# build the solution
rust_build(ILU_HOME, None, build_level)

ansible_dir = os.path.join("..", "ansible")
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
    "cores": 4,
    "memory": 1024 * 5,
    "worker_status_ms": 500,
}


def run_scaling(log_file, threads, server):
    bindir = os.path.join(
        ILU_HOME, "target", "x86_64-unknown-linux-gnu", str(build_level)
    )
    load_pth = os.path.join(
        bindir,
        "iluvatar_load_gen",
    )
    img = "docker.io/alfuerst/json_dumps_loads-iluvatar-action-http"
    if server == "unix":
        img = "docker.io/alfuerst/json_dumps_loads-iluvatar-action-unix"
    args = [
        load_pth,
        "scaling",
        "--out-folder",
        results_dir,
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
        "--out-folder",
        server,
        "--server",
        server,
        "containerd",
        "--duration=120",
    ]
    env = deepcopy(os.environ)
    env["RUST_BACTRACE"] = "1"
    completed = subprocess.run(
        args=args,
        stdout=log_file,
        stderr=log_file,
        text=True,
        env=env,
    )
    completed.check_returncode()

threads = list(range(1, 50, 5))
print(threads)
exit()

pre_run_cleanup(log_file, results_dir, **kwargs)
try:
    run_ansible(log_file, **kwargs)
    with open(log_file, "a") as f:
        threads = list(range(1, 50, 5))
        threads[0] = 1
        for server in ["http", "unix"]:
            for tds in threads:
                run_scaling(f, tds, server)
finally:
    remote_cleanup(log_file, results_dir, **kwargs)
