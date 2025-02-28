import sys, os

ILU_HOME = "../../.."

sys.path.append(os.path.join(ILU_HOME, ".."))
from load.run.run_trace import (
    rust_build,
    run_live,
    RunTarget,
    BuildTarget,
    make_host_queue,
)
import shutil
from Crypto.PublicKey import RSA

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
    "controller_log_level": "info",
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
    "cpu_queue_policy": "fcfs",
    "prewarm": 1,
    "benchmark_file": benchmark,
    # we need cluster-specific stuff now
    "target": RunTarget.CONTROLLER,
    "influx_enabled": True,
    "controller_log_dir": worker_log_dir,
    "force": False
}

SSH_Q = make_host_queue([("sshd", "127.0.0.1")])

keys_backup = os.path.join(os.getenv("HOME"), ".ssh/authorized_keys_bak")
keys_src = os.path.join(os.getenv("HOME"), ".ssh/authorized_keys")
shutil.copy(keys_src, keys_backup)

# Ansible doesn't allow passing the ssh key password via command line, so we only support unencrpyted keys currently.
_passphrase = "cluster-trace-example"
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

input_csv = "four-functions.csv"
meta_csv = "four-functions-metadata.csv"
try:
    # run entire experiment
    run_live(
        input_csv,
        meta_csv,
        results_dir,
        SSH_Q,
        **kwargs,
    )
    pass
finally:
    shutil.move(keys_backup, keys_src)


## plot some results
from load.analysis import LogParser
from load.run.run_trace import RunTarget, RunType
import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt

mpl.rcParams.update({"font.size": 14})
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42


parser = LogParser(results_dir, input_csv, meta_csv, benchmark, RunType.LIVE)
parser.parse_logs()

fig, ax = plt.subplots()
plt.tight_layout()
fig.set_size_inches(5, 3)

labels = []
for i, (func, df) in enumerate(
    parser.worker_parsers[0].invokes_df.groupby("function_name")
):
    ax.bar(i, height=df["e2e_overhead"].mean(), yerr=df["e2e_overhead"].std())
    labels.append(func)


ax.set_xticks(list(range(len(labels))))
ax.set_xticklabels(labels)
ax.set_ylabel("Avg. Platform Overhead (sec.)")
ax.set_xlabel("Function Name")
plt.savefig(os.path.join(results_dir, "overhead.png"), bbox_inches="tight")
plt.close(fig)
