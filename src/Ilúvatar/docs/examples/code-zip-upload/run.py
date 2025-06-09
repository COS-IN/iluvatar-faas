import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from copy import deepcopy
import subprocess
from load.run.run_trace import (
    rust_build_native,
    BuildTarget,
    RunTarget,
    ansible_clean,
    copy_logs,
    pre_run_cleanup,
    remote_cleanup,
    run_ansible,
)
from load.run.logging import create_logger

out_folder = os.path.join(os.getcwd(), "results")
os.makedirs(out_folder, exist_ok=True)
worker_log_dir= os.path.join(out_folder, "tmp")
os.makedirs(worker_log_dir, exist_ok=True)
log_file = os.path.join(out_folder, "orchestration.log")
logger = create_logger(log_file)


build_level = BuildTarget.RELEASE
# build the solution
rust_build_native(ILU_HOME, logger, build_level)
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
    "cores": 4,
    "memory": 1024 * 10,
    "worker_status_ms": 500,
    "worker_log_dir": worker_log_dir,
    "log_level": "warn",
    "snapshotter": "overlayfs",
}
print(kwargs["worker_log_dir"])
bindir = os.path.join(
    ILU_HOME, "target", "x86_64-unknown-linux-gnu", str(build_level)
)
cli_pth = os.path.join(
    bindir,
    "iluvatar_worker_cli",
)
pre_run_cleanup(log_file, out_folder, **kwargs)
try:
    run_ansible(log_file, **kwargs)
    args = [
        cli_pth,
        "--port",
        str(8070),
        "--host",
        kwargs["host"],
        "register",
        "--memory",
        "1024",
        "--isolation",
        "CONTAINERD",
        "--runtime",
        "python3",
        "--code-zip",
        "code.tar.gz",
        "--name",
        "test",
    ]
    env = deepcopy(os.environ)
    env["RUST_BACTRACE"] = "1"
    completed = subprocess.run(
        args=args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    logger.info(completed.stdout)
    logger.info(completed.stderr)
    completed.check_returncode()
finally:
    pass
#     remote_cleanup(log_file, out_folder, **kwargs)
