import sys, os, shutil

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from copy import deepcopy
import subprocess
import multiprocessing
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
# build the solution
rust_build(ILU_HOME, None, build_level)
ansible_dir = os.path.join(ILU_HOME, "ansible")


def run_scaling(threads):
    out_folder = os.path.join(os.getcwd(), "results", str(threads))
    worker_log_dir= os.path.join(out_dir, "tmp")
    os.makedirs(worker_log_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)
    log_file = os.path.join(results_dir, "orchestration.log")
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
        "cores": threads,
        "memory": 1024 * threads,
        "worker_status_ms": 500,
        "worker_log_dir": worker_log_dir,
        "log_level": "warn"
    }
    print(kwargs["worker_log_dir"])
    bindir = os.path.join(
        ILU_HOME, "target", "x86_64-unknown-linux-gnu", str(build_level)
    )
    load_pth = os.path.join(
        bindir,
        "iluvatar_load_gen",
    )
    img = "docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest"
    with open(log_file, "w") as log:
        pre_run_cleanup(log_file, results_dir, **kwargs)
        try:
            run_ansible(log_file, **kwargs)
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
                "containerd",
                "--out-folder",
                out_folder,
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
        finally:
            remote_cleanup(log_file, results_dir, **kwargs)


mx = multiprocessing.cpu_count()
points = 5
threads = list(range(1, multiprocessing.cpu_count(), mx // points))
for tds in threads:
    run_scaling(tds)