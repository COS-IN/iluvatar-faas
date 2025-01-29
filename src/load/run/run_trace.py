#!/usr/bin/python3
import os
from time import sleep
import subprocess
import json
import traceback
from enum import Enum
from copy import deepcopy
import shutil
from .config import LoadConfig

from .multiproc import *


class BuildTarget(Enum):
    DEBUG = "debug"
    DEBUG_SPANS = "spansd"
    RELEASE = "release"
    RELEASE_SPANS = "spans"

    def __str__(self) -> str:
        return self.value

    def make_name(self) -> str:
        return str(self)

    def path_name(self) -> str:
        if self == self.DEBUG or self == self.DEBUG_SPANS:
            return "debug"
        elif self == self.RELEASE or self == self.RELEASE_SPANS:
            return "release"


def rust_build(ilu_home, log_file=None, build: BuildTarget = BuildTarget.RELEASE):
    pwd = os.getcwd()
    os.chdir(ilu_home)
    build_args = ["make", build.make_name()]
    _run_cmd(build_args, log_file)
    os.chdir(pwd)


class RunType(Enum):
    SIM = "sim"
    LIVE = "live"

    def __str__(self) -> str:
        return self.value

    def is_sim(self):
        return self == RunType.SIM

    def is_live(self):
        return self == RunType.LIVE


class RunTarget(Enum):
    WORKER = "worker"
    CONTROLLER = "controller"

    def __str__(self) -> str:
        return self.value

    def yml(self) -> str:
        if self == self.WORKER:
            return "worker.yml"
        elif self == self.CONTROLLER:
            return "iluvatar.yml"


def trace_output(type, trace_in):
    basename = trace_base_name(trace_in)
    addtl = ""
    if type == "json":
        addtl = "-full"
    return f"output{addtl}-{basename}.{type}"


def has_results(results_dir, function_trace_name):
    output_json = os.path.join(results_dir, trace_output("json", function_trace_name))
    output_csv = os.path.join(results_dir, trace_output("csv", function_trace_name))
    worker_log = os.path.join(results_dir, f"worker_worker1.log")
    if not os.path.exists(output_json):
        return False
    if not os.path.exists(output_csv):
        return False
    if not os.path.exists(worker_log):
        # simulation log
        worker_log = os.path.join(results_dir, f"load_gen.log")
        if not os.path.exists(worker_log):
            return False
    with open(output_json) as f:
        if len(f.readlines()) == 0:
            return False
    try:
        with open(output_json) as jsonf:
            _parsed = json.load(jsonf)
    except:
        return False
    with open(output_csv) as f:
        if len(f.readlines()) == 0:
            return False
    with open(worker_log) as f:
        if len(f.readlines()) < 100:
            return False
    return True


def trace_base_name(trace_in_csv: str):
    return os.path.splitext(os.path.basename(trace_in_csv))[0]


def _run_ansible_clean(log_file, kwargs):
    run_args = [
        "ansible-playbook",
        "-i",
        kwargs["ansible_host_file"],
        os.path.join(kwargs["ilu_home"], "ansible", kwargs["target"].yml()),
    ]
    env_args = [kwargs["ansible_hosts_addrs"], "mode=clean"]

    worker_env = kwargs.to_env_var_dict("worker")
    worker_env = json.dumps({"worker_environment": worker_env})
    env_args.append(worker_env)
    if kwargs["target"] == RunTarget.CONTROLLER:
        controller_env = kwargs.to_env_var_dict("controller")
        controller_env = json.dumps({"controller_environment": controller_env})
        env_args.append(controller_env)

    for env_arg in env_args:
        run_args.append("-e")
        run_args.append(env_arg)

    if kwargs["private_ssh_key"] is not None:
        run_args.append(f"--private-key={kwargs['private_ssh_key']}")
    _run_cmd(run_args, log_file)


def _copy_logs(log_file, results_dir, kwargs):
    if kwargs["host"] == "localhost" or kwargs["host"] == "127.0.0.1":
        for subdir in os.listdir(kwargs["worker_log_dir"]):
            src = os.path.join(kwargs["worker_log_dir"], subdir)
            dest = os.path.join(results_dir, subdir)
            if src != dest:
                shutil.move(src, dest)
    else:
        from paramiko import SSHClient
        import paramiko
        from scp import SCPClient

        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.load_system_host_keys()
            ssh.load_host_keys(os.path.expanduser("~/.ssh/known_hosts"))
            ssh.connect(kwargs["host"])
            with SCPClient(ssh.get_transport(), sanitize=lambda x: x) as scp:
                scp.get(f"{kwargs['worker_log_dir']}/*", results_dir, recursive=True)


def _pre_run_cleanup(
    log_file,
    results_dir,
    kwargs,
):
    _run_ansible_clean(log_file, kwargs)
    clean_dir = os.path.join(results_dir, "precleanup")
    os.makedirs(clean_dir, exist_ok=True)
    _copy_logs(log_file, clean_dir, kwargs)


def _remote_cleanup(
    log_file,
    results_dir,
    kwargs,
):
    print("Cleanup:", results_dir)
    _copy_logs(log_file, results_dir, kwargs)
    _run_ansible_clean(log_file, kwargs)

    if kwargs["host"] == "localhost" or kwargs["host"] == "127.0.0.1":
        if results_dir != kwargs["worker_log_dir"]:
            _run_cmd(["rm", "-rf", kwargs["worker_log_dir"]], log_file, shell=False)
    else:
        from paramiko import SSHClient
        import paramiko
        from scp import SCPClient

        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.load_system_host_keys()
            ssh.load_host_keys(os.path.expanduser("~/.ssh/known_hosts"))
            ssh.connect(kwargs["host"])
            ssh.exec_command(f"sudo rm -rf {kwargs['worker_log_dir']}")


def _run_cmd(cmd_args, log_file, shell: bool = False):
    opened_log = False
    if type(log_file) is str:
        log_file = open(log_file, "a")
        opened_log = True
    try:
        formatted_args = []
        for x in cmd_args:
            string = str(x)
            if string.startswith("-e") and string != "-e":
                raise Exception(f"Bad ansible argument: {string}")

            formatted_args.append(string)
        env = deepcopy(os.environ)
        env["RUST_BACTRACE"] = "1"
        completed = subprocess.run(
            args=formatted_args,
            stdout=log_file,
            stderr=log_file,
            text=True,
            shell=shell,
            env=env,
        )
        completed.check_returncode()
        return completed.stdout
    except Exception as e:
        if log_file is not None:
            log_file.write("Exception encountered:\n")
            log_file.write(str(e))
            log_file.write("\n")
            log_file.write(traceback.format_exc())
            log_file.write("\n")
            for arg in cmd_args:
                log_file.write(f"{arg} ")
                log_file.write("\n")
        raise e
    finally:
        if opened_log:
            log_file.close()


def _run_ansible(
    log_file,
    kwargs,
):
    env_args = [
        kwargs["ansible_hosts_addrs"],
        "mode=deploy",
        f"target={kwargs['build_level'].path_name()}",
    ]
    worker_env = kwargs.to_env_var_dict("worker")
    worker_env = json.dumps({"worker_environment": worker_env})
    env_args.append(worker_env)
    if kwargs["target"] == RunTarget.CONTROLLER:
        controller_env = kwargs.to_env_var_dict("controller")
        controller_env = json.dumps({"controller_environment": controller_env})
        env_args.append(controller_env)
    if kwargs["target"] == RunTarget.CONTROLLER:
        env_args.append("cluster=true")

    run_args = [
        "ansible-playbook",
        "-i",
        kwargs["ansible_host_file"],
        os.path.join(kwargs["ilu_home"], "ansible", kwargs["target"].yml()),
    ]
    if kwargs["private_ssh_key"] is not None:
        run_args.append(f"--private-key={kwargs['private_ssh_key']}")

    for env_arg in env_args:
        run_args.append("-e")
        run_args.append(env_arg)

    if type(kwargs["ansible_args"]) == str and len(kwargs["ansible_args"]) > 0:
        ansible_args = kwargs["ansible_args"].split(" ")
        run_args.extend(ansible_args)
    elif type(kwargs["ansible_args"]) == list and len(kwargs["ansible_args"]) > 0:
        run_args.extend(kwargs["ansible_args"])
    _run_cmd(run_args, log_file)


def _run_load(log_file, results_dir, input_csv, metadata, kwargs):
    setup = "live"
    if kwargs["simulation"]:
        setup = "simulation"
    shutil.copy(input_csv, results_dir)
    shutil.copy(metadata, results_dir)

    gen_path = os.path.join(
        kwargs["ilu_home"],
        "target/x86_64-unknown-linux-gnu",
        kwargs["build_level"].path_name(),
        "iluvatar_load_gen",
    )
    port = kwargs["worker_port"]
    if kwargs["target"] == RunTarget.CONTROLLER:
        port = kwargs["controller_port"]
    load_args = [
        gen_path,
        "trace",
        "--out-folder",
        results_dir,
        "--port",
        port,
        "--host",
        kwargs["host"],
        "--target",
        str(kwargs["target"]),
        "--setup",
        setup,
        "--load-type",
        kwargs["load_type"],
        "--input-csv",
        input_csv,
        "--metadata-csv",
        metadata,
        "--function-data",
        kwargs["benchmark_file"],
        "--prewarms",
        kwargs["prewarm"],
        "--max-prewarms",
        1,
    ]
    if kwargs["simulation"]:
        load_args.append("--worker-config")
        load_args.append(kwargs["worker_config_file"])
        if "controller_config_file" in kwargs:
            load_args.append("--controller-config")
            load_args.append(kwargs["controller_config_file"])
            load_args.append("--workers")
            load_args.append(kwargs["num_workers"])
        load_args.append("--sim-gran")
        load_args.append(kwargs["sim_gran"])
        load_args.append("--tick-step")
        load_args.append(kwargs["tick_step"])
    _run_cmd(load_args, log_file)


runner_config_kwargs = [
    ("ilu_home", "NOT_SET"),
    ("ansible_dir", "NOT_SET"),
    ("ansible_hosts_addrs", "NOT_SET"),
    ("benchmark_file", "NOT_SET"),
    ("build_level", BuildTarget.RELEASE),
    ("force", False),
    ("function_trace_name", "chosen-ecdf"),
    ("host", "NOT_SET"),
    ("host_queue", None),
    ("private_ssh_key", None),
    ("private_ssh_key_pass", None),
    ("ansible_args", []),
]

load_gen_kwargs = {
    ("load_type", "functions"),
    ("prewarm", 1),
    ("simulation", None),
    ("tick_step", 300),
    ("sim_gran", "us"),
    ("num_workers", 1),
    ("target", RunTarget.WORKER),
}

controller_kwargs = {
    ("controller_log_dir", "/tmp/iluvatar/logs/ansible", ("logging", "directory")),
    ("controller_log_level", "info", ("logging", "level")),
    ("controller_port", 8089, ("port",)),
    ("controller_algorithm", "LeastLoaded", ("load_balancer", "algorithm")),
    ("controller_thread_sleep_ms", 5000, ("load_balancer", "thread_sleep_ms")),
    ("controller_load_metric", "loadavg", ("load_balancer", "load_metric")),
}
worker_kwargs = {
    ("worker_port", 8070, ("port",)),
    ("load_balancer_url", "", ("load_balancer_url",)),
    # invoke basics
    ("memory", 20 * 1024, ("container_resources", "memory_mb")),
    ("cores", 12, ("container_resources", "cpu_resource", "count")),
    ("cpu_queue", "serial", ("invocation", "queues", "cpu")),
    ("cpu_queue_policy", "minheap_ed", ("invocation", "queue_policies", "cpu")),
    ("gpu_queue", "mqfq", ("invocation", "queues", "gpu")),
    (
        "gpu_queue_policy",
        "mqfq_select_out_len",
        ("invocation", "queue_policies", "gpu"),
    ),
    ("enqueueing", "All", ("invocation", "enqueueing_policy")),
    ("invoke_queue_sleep_ms", 500, ("invocation", "queue_sleep_ms")),
    ("enqueuing_log_details", False, ("invocation", "enqueuing_log_details")),
    # logging
    ("log_level", "info", ("logging", "level")),
    ("worker_spanning", "NONE", ("logging", "spanning")),
    ("worker_log_dir", "/tmp/iluvatar/logs/ansible", ("logging", "directory")),
    ("worker_status_ms", 500, ("status", "report_freq_ms")),
    # energy
    ("ipmi_freq_ms", 0, ("energy", "ipmi_freq_ms")),
    ("ipmi_pass_file", "", ("energy", "ipmi_pass_file")),
    ("ipmi_ip_addr", "", ("energy", "ipmi_ip_addr")),
    ("perf_freq_ms", 0, ("energy", "perf_freq_ms")),
    ("rapl_freq_ms", 0, ("energy", "rapl_freq_ms")),
    ("energy_log_folder", "/tmp/iluvatar/logs/ansible", ("energy", "log_folder")),
    ("process_freq_ms", 0, ("energy", "process_freq_ms")),
    ("tegra_freq_ms", 0, ("energy", "tegra_freq_ms")),
    # gpu
    ("gpus", 0, ("container_resources", "gpu_resource", "count")),
    ("fpd", 16, ("container_resources", "gpu_resource", "funcs_per_device")),
    (
        "per_func_gpu_memory",
        16 * 1024,
        ("container_resources", "gpu_resource", "per_func_memory_mb"),
    ),
    ("mps", False, ("container_resources", "gpu_resource", "use_standalone_mps")),
    (
        "gpu_stat_check",
        500,
        ("container_resources", "gpu_resource", "status_update_freq_ms"),
    ),
    ("use_driver", True, ("container_resources", "gpu_resource", "use_driver_hook")),
    ("prefetch", True, ("container_resources", "gpu_resource", "prefetch_memory")),
    ("gpu_util", 95, ("container_resources", "gpu_resource", "limit_on_utilization")),
    (
        "gpu_running",
        2,
        ("container_resources", "gpu_resource", "concurrent_running_funcs"),
    ),
    # containers
    ("concurrent_creation", 5, ("container_resources", "concurrent_creation")),
    ("snapshotter", "zfs", ("container_resources", "snapshotter")),
    ("worker_memory_buffer", 1024, ("container_resources", "memory_buffer_mb")),
    # influx
    ("influx_enabled", False, ("influx", "enabled")),
    ("influx_freq", 500, ("influx", "update_freq_ms")),
    # mqfq
    ("allowed_overrun", 5.0, ("invocation", "mqfq_config", "allowed_overrun")),
    ("service_average", 0.0, ("invocation", "mqfq_config", "service_average")),
    ("mqfq_ttl_sec", -1.5, ("invocation", "mqfq_config", "ttl_sec")),
    ("select_cnt", 20, ("invocation", "mqfq_config", "flow_select_cnt")),
    (
        "mqfq_weight_logging_ms",
        0,
        ("invocation", "mqfq_config", "mqfq_weight_logging_ms"),
    ),
    (
        "mqfq_time_estimation",
        "FallbackLinReg",
        ("invocation", "mqfq_config", "time_estimation"),
    ),
    (
        "mqfq_add_estimation_error",
        False,
        ("invocation", "mqfq_config", "add_estimation_error"),
    ),
    # landlord
    ("cache_size", 10, ("invocation", "landlord_config", "cache_size")),
    ("lnd_max_size", 20, ("invocation", "landlord_config", "max_size")),
    ("lnd_load_thresh", 5.0, ("invocation", "landlord_config", "load_thresh")),
    ("lnd_slowdown_thresh", 6.0, ("invocation", "landlord_config", "slowdown_thresh")),
    ("log_cache_info", False, ("invocation", "landlord_config", "log_cache_info")),
    ("lnd_fixed_mode", False, ("invocation", "landlord_config", "fixed_mode")),
    # greedy
    ("greedy_load", 3.0, ("invocation", "greedy_weight_config", "allow_load")),
    ("greedy_policy", "TopQuarter", ("invocation", "greedy_weight_config", "allow")),
    ("greedy_log", False, ("invocation", "greedy_weight_config", "log")),
    ("greedy_cache_size", 0, ("invocation", "greedy_weight_config", "cache_size")),
    (
        "greedy_fixed_assignment",
        False,
        ("invocation", "greedy_weight_config", "fixed_assignment"),
    ),
}

default_kwargs = LoadConfig()
default_kwargs.bulk_add("runner", runner_config_kwargs)
default_kwargs.bulk_add("load", load_gen_kwargs)
default_kwargs.bulk_add("controller", controller_kwargs)
default_kwargs.bulk_add("worker", worker_kwargs)


def run_live(
    trace_in: str,
    trace_meta: str,
    results_dir: str,
    queue: CustQueue,
    **kwargs,
):
    """
    Run the given experiment on a live system, using `queue` for host control.
    `trace_in`: input csv of invocations
    `trace_meta`: csv of function metadata for registration
    `results_dir`: where to store all results
    `kwargs`: custom config for the experimental run
    """
    kwargs = default_kwargs.overwrite(**kwargs)
    os.makedirs(results_dir, exist_ok=True)
    log_file = os.path.join(results_dir, "orchestration.log")
    kwargs["function_trace_name"] = trace_base_name(trace_in)

    if not kwargs["force"] and has_results(results_dir, kwargs["function_trace_name"]):
        print(f"Skipping {results_dir}")
    else:
        with HeldHost(queue.get(), queue) as held_host:
            kwargs["ansible_host_file"] = os.path.join(
                kwargs["ansible_dir"],
                "environments/",
                held_host.ansible_env,
                "hosts.ini",
            )
            kwargs["host"] = held_host.address
            print(f"Running {results_dir} on {kwargs['host']}")

            with open(log_file, "w") as log_file_fp:
                try:
                    _pre_run_cleanup(
                        log_file_fp,
                        results_dir,
                        kwargs,
                    )
                    _run_ansible(log_file_fp, kwargs)
                    sleep(5)
                    _run_load(
                        log_file_fp,
                        results_dir,
                        trace_in,
                        trace_meta,
                        kwargs,
                    )
                    sleep(5)
                    _remote_cleanup(
                        log_file_fp,
                        results_dir,
                        kwargs,
                    )
                except Exception as e:
                    log_file_fp.write("Exception encountered:\n")
                    log_file_fp.write(str(e))
                    log_file_fp.write("\n")
                    log_file_fp.write(traceback.format_exc())
                    log_file_fp.write("\n")
                    _remote_cleanup(
                        log_file_fp,
                        results_dir,
                        kwargs,
                    )
                    raise e


def run_sim(
    trace_in,
    trace_meta,
    results_dir,
    **kwargs,
):
    """
    Run the given experiment as a simulation.
    `trace_in`: input CSV of invocations
    `trace_meta`: CSV of function metadata for registration
    `results_dir`: where to store all results
    `kwargs`: custom config for the experimental run
    """
    kwargs = default_kwargs.overwrite(**kwargs)
    kwargs["host"] = "NOT_SET_SIMULATION"
    os.makedirs(results_dir, exist_ok=True)
    log_file = os.path.join(results_dir, "orchestration.log")
    kwargs["simulation"] = True
    kwargs["function_trace_name"] = trace_base_name(trace_in)

    if not kwargs["force"] and has_results(results_dir, kwargs["function_trace_name"]):
        print(f"Skipping {results_dir}")
        return

    if kwargs["target"] == RunTarget.CONTROLLER:
        controller_config_file = os.path.join(results_dir, "controller.json")
        src = os.path.join(
            kwargs["ilu_home"], "iluvatar_controller/src/controller.dev.json"
        )
        shutil.copy(src, controller_config_file)
        kwargs["controller_config_file"] = controller_config_file
        with open(kwargs["controller_config_file"], "r+") as f:
            json_data = json.load(f)
            kwargs.to_json("controller", json_data)
            f.seek(0)
            json.dump(json_data, f, indent=4)

    src = os.path.join(kwargs["ilu_home"], "iluvatar_worker/src/worker.dev.json")
    worker_config_file = os.path.join(results_dir, "worker.json")
    shutil.copy(src, worker_config_file)
    kwargs["worker_config_file"] = worker_config_file

    with open(kwargs["worker_config_file"], "r+") as f:
        json_data = json.load(f)
        kwargs.to_json("worker", json_data)
        f.seek(0)
        json.dump(json_data, f, indent=4)

    print(f"Running {results_dir}")
    _run_load(
        log_file,
        results_dir,
        trace_in,
        trace_meta,
        kwargs,
    )
