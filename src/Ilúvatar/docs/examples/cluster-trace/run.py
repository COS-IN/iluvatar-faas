import sys, os

sys.path.append("../../../../load/run")
from run_trace import (
    rust_build,
    run_cmd,
    run_live,
    RunTarget,
    BuildTarget,
    make_host_queue,
)
import shutil
from Crypto.PublicKey import RSA

ILU_HOME = "../../.."
CORES = 2
MEMORY = 4096
build_level = BuildTarget.RELEASE
worker_log_dir = os.path.join(os.getcwd(), "temp_results")
results_dir = os.path.join(os.getcwd(), "results")
log_file = os.path.join(results_dir, "orchestration.log")
benchmark = "../benchmark/worker_function_benchmarks.json"
os.makedirs(results_dir, exist_ok=True)
os.makedirs(worker_log_dir, exist_ok=True)

# build the solution
rust_build(ILU_HOME, None, build_level)

ansible_dir = os.path.join(ILU_HOME, "ansible")
kwargs = {
    "ilu_home": ILU_HOME,
    "ansible_hosts_addrs": "@"
    + os.path.join(ansible_dir, "group_vars/local_addresses.yml"),
    "ansible_dir": ansible_dir,
    "build_level": build_level,
    "cores": CORES,
    "memory": MEMORY,
    "snapshotter": "overlayfs",
    "worker_status_ms": 500,
    "worker_log_dir": worker_log_dir,
    "controller_log_dir": worker_log_dir,
    "cpu_queue_policy": "fcfs",
    "target": RunTarget.CONTROLLER,
    "prewarm": 1,
    "benchmark_file": benchmark,
    "influx_enabled": True,
}

SSH_Q = make_host_queue([("sshd", "127.0.0.1")])

keys_backup = os.path.join(os.getenv("HOME"), ".ssh/authorized_keys_bak")
keys_src = os.path.join(os.getenv("HOME"), ".ssh/authorized_keys")
shutil.copy(keys_src, keys_backup)

passphrase = "cluster-trace-example"
ssh_key = "./example-ssh"
# Demonstrate using SSH key to connect to 'remote' machines for setup
# even if this example is all local, still uses SSH connection for Ansible
key = RSA.generate(2048)
with open(ssh_key, "wb") as content_file:
    os.chmod(ssh_key, 0o600)
    content_file.write(key.exportKey("PEM"))
pubkey = key.publickey()
with open(f"{ssh_key}.pub", "wb") as content_file:
    content_file.write(pubkey.exportKey("OpenSSH"))

with open(f"{ssh_key}.pub", "r") as f:
    key = f.read()
with open(keys_src, "a") as f:
    f.write(f"{key}\n")

kwargs["private_ssh_key"] = os.path.join(os.getcwd(), ssh_key)

try:
    # run entire experiment
    run_live(
        "four-functions.csv",
        "four-functions-metadata.csv",
        results_dir,
        SSH_Q,
        **kwargs,
    )
    pass
finally:
    shutil.move(keys_backup, keys_src)
