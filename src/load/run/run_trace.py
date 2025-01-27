#!/usr/bin/python3
import os
from time import sleep
import subprocess
import json
import traceback
from enum import Enum
from copy import deepcopy
import shutil

from multiproc import *
from ansible import *


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


def has_results(results_dir, function_trace_name, log_file):
    output_json = os.path.join(results_dir, f"output-full-{function_trace_name}.json")
    output_csv = os.path.join(results_dir, f"output-{function_trace_name}.csv")
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


def run_ansible_clean(log_file, **kwargs):
    run_args = [
        "ansible-playbook",
        "-i",
        kwargs["ansible_host_file"],
        os.path.join(kwargs["ilu_home"], "ansible", kwargs["target"].yml()),
        "-e",
        kwargs["ansible_hosts_addrs"],
        "-e",
        "mode=clean",
        "-e",
        f"target={kwargs['build_level'].path_name()}",
        "-e",
        f"worker_log_dir={kwargs['worker_log_dir']}",
        "-e",
        f"worker_log_level={kwargs['log_level']}",
    ]
    if kwargs["private_ssh_key"] is not None:
        run_args.append(f"--private-key={kwargs['private_ssh_key']}")
        # if kwargs["private_ssh_key_pass"] is not None:
        #     run_args.append(f"--private-key=={kwargs['private_ssh_key_pass']}")
    run_cmd(run_args, log_file)


def copy_logs(log_file, results_dir, **kwargs):
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


def pre_run_cleanup(
    log_file,
    results_dir,
    **kwargs,
):
    run_ansible_clean(log_file, **kwargs)
    clean_dir = os.path.join(results_dir, "precleanup")
    os.makedirs(clean_dir, exist_ok=True)
    copy_logs(log_file, clean_dir, **kwargs)


def remote_cleanup(
    log_file,
    results_dir,
    **kwargs,
):
    print("Cleanup:", results_dir)
    copy_logs(log_file, results_dir, **kwargs)
    run_ansible_clean(log_file, **kwargs)

    if kwargs["host"] == "localhost" or kwargs["host"] == "127.0.0.1":
        if results_dir != kwargs["worker_log_dir"]:
            run_cmd(["rm", "-rf", kwargs["worker_log_dir"]], log_file, shell=False)
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


def run_cmd(cmd_args, log_file, shell: bool = False):
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


def run_ansible(
    log_file,
    **kwargs,
):
    env_args = [
        kwargs["ansible_hosts_addrs"],
        "mode=deploy",
        f"target={kwargs['build_level'].path_name()}",
        f"worker_memory_mb={kwargs['memory']}",
        f"worker_cores={kwargs['cores']}",
        f"worker_log_dir={kwargs['worker_log_dir']}",
        f"worker_cpu_queue_policy={kwargs['cpu_queue_policy']}",
        f"worker_status_ms={kwargs['stat_update']}",
        f"worker_enqueueing={kwargs['enqueueing']}",
        f"use_driver_hook={kwargs['use_driver']}",
        f"funcs_per_device={kwargs['fpd']}",
        f"standalone_mps={kwargs['mps']}",
        f"worker_gpus={kwargs['gpus']}",
        f"worker_gpu_queue={kwargs['gpu_queue']}",
        f"gpu_limit_on_utilization={kwargs['gpu_util']}",
        f"gpu_prefetch_memory={kwargs['prefetch']}",
        f"worker_concurrent_creation={kwargs['concurrent_creation']}",
        f"gpu_concurrent_running={kwargs['gpu_running']}",
        f"gpu_status_update={kwargs['stat_update']}",
        f"mig_shares={kwargs['mig_shares']}",
        f"mqfq_allowed_overrun={kwargs['allowed_overrun']}",
        f"worker_log_level={kwargs['log_level']}",
        f"mqfq_service_average={kwargs['service_average']}",
        f"worker_gpu_queue_policy={kwargs['gpu_queue_policy']}",
        f"mqfq_ttl_sec={kwargs['mqfq_ttl_sec']}",
        f"mqfq_weight_logging_ms={0}",
        f"mqfq_flow_select_cnt={kwargs['select_cnt']}",
        f"lnd_cache_size={kwargs['cache_size']}",
        f"lnd_max_size={kwargs['lnd_max_size']}",
        f"lnd_load_thresh={kwargs['lnd_load_thresh']}",
        f"lnd_slowdown_thresh={kwargs['lnd_slowdown_thresh']}",
        f"mqfq_add_estimation_error={kwargs['mqfq_add_estimation_error']}",
        f"mqfq_time_estimation={kwargs['mqfq_time_estimation']}",
        f"greedy_policy={kwargs['greedy_policy']}",
        f"greedy_load={kwargs['greedy_load']}",
        f"greedy_log={kwargs['greedy_log']}",
        f"greedy_cache_size={kwargs['greedy_cache_size']}",
        f"greedy_fixed_assignment={kwargs['greedy_fixed_assignment']}",
        f"worker_spanning={kwargs['worker_spanning']}",
        "lnd_cache_log=true",
        "worker_enqueueing_details=true",
        "worker_ipmi_log_freq_ms=0",
        "worker_perf_log_freq_ms=0",
        "worker_rapl_log_freq_ms=0",
        f"controller_log_dir={kwargs['controller_log_dir']}",
        f"controller_port={kwargs['controller_port']}",
        f"controller_algorithm={kwargs['controller_algorithm']}",
        f"controller_load_metric={kwargs['controller_load_metric']}",
        f"controller_log_level={kwargs['log_level']}",
        f"influx_enabled={kwargs['influx_enabled']}",
    ]
    if kwargs["log_level"] == "debug":
        env_args += [f"worker_include_spans={True}"]
    run_args = [
        "ansible-playbook",
        "-i",
        kwargs["ansible_host_file"],
        os.path.join(kwargs["ilu_home"], "ansible", kwargs["target"].yml()),
    ]
    if kwargs["target"] == RunTarget.CONTROLLER:
        env_args.append("cluster=true")
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
    run_cmd(run_args, log_file)


def run_load(log_file, results_dir, input_csv, metadata, **kwargs):
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
    run_cmd(load_args, log_file)


def rust_build(ilu_home, log_file=None, build: BuildTarget = BuildTarget.RELEASE):
    pwd = os.getcwd()
    os.chdir(ilu_home)
    build_args = ["make", build.make_name()]
    run_cmd(build_args, log_file)
    os.chdir(pwd)


runner_config_kwargs = {
    "ilu_home": "NOT_SET",
    "ansible_dir": "NOT_SET",
    "ansible_hosts_addrs": "NOT_SET",
    "benchmark_file": "NOT_SET",
    "build_level": BuildTarget.RELEASE,
    "force": False,
    "function_trace_name": "chosen-ecdf",
    "host": "NOT_SET",
    "host_queue": None,
    "private_ssh_key": None,
    "private_ssh_key_pass": None,
}

load_gen_kwargs = {
    "load_type": "functions",
    "prewarm": 1,
    "simulation": None,
    "tick_step": 300,
    "sim_gran": "us",
    "num_workers": 1,
    "target": RunTarget.WORKER,
}

controller_kwargs = {
    "controller_log_dir": "/tmp/iluvatar/logs/ansible",
    "controller_port": "8089",
    "controller_algorithm": "LeastLoaded",
    "controller_load_metric": "loadavg",
}
worker_kwargs = {
    "influx_enabled": False,
    "ansible_args": [],
    "log_level": "info",
    "worker_spanning": "NONE",
    "worker_log_dir": "/tmp/iluvatar/logs/ansible",
    "worker_port": 8070,
    "memory": 20 * 1024,
    "cores": 12,
    "gpus": 0,
    "fpd": 16,
    "mps": False,
    "use_driver": True,
    "prefetch": True,
    "gpu_util": 95,
    "concurrent_creation": 5,
    "gpu_running": 2,
    "stat_update": 500,
    "allowed_overrun": 5.0,
    "service_average": 0.0,
    "mqfq_ttl_sec": -1.5,
    "select_cnt": 20,
    "enqueueing": "All",
    "cpu_queue": "serial",
    "cpu_queue_policy": "minheap_ed",
    "gpu_queue": "mqfq",
    "gpu_queue_policy": "mqfq_select_out_len",
    "mqfq_time_estimation": "FallbackLinReg",
    "mqfq_add_estimation_error": False,
    "cache_size": 10,
    "lnd_max_size": 20,
    "lnd_load_thresh": 5.0,
    "lnd_slowdown_thresh": 6.0,
    "mig_shares": 0,
    "log_cache_info": False,
    "enqueuing_log_details": False,
    "greedy_load": 3.0,
    "greedy_policy": "TopQuarter",
    "greedy_log": False,
    "greedy_cache_size": 0,
    "greedy_fixed_assignment": False,
    "lnd_fixed_mode": False,
}

default_kwargs = {
    # runner config
    **runner_config_kwargs,
    # load gen config
    **load_gen_kwargs,
    # iluvatar config
    **worker_kwargs,
    **controller_kwargs,
}


def run_live(
    trace_in: str,
    trace_meta: str,
    results_dir: str,
    queue: CustQueue,
    **kwargs,
):
    kwargs = {**default_kwargs, **kwargs}
    os.makedirs(results_dir, exist_ok=True)
    log_file = os.path.join(results_dir, "orchestration.log")

    if not kwargs["force"] and has_results(
        results_dir, kwargs["function_trace_name"], log_file
    ):
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
                    pre_run_cleanup(
                        log_file_fp,
                        results_dir,
                        **kwargs,
                    )
                    run_ansible(log_file=log_file_fp, **kwargs)
                    sleep(5)
                    run_load(
                        log_file_fp,
                        results_dir,
                        trace_in,
                        trace_meta,
                        **kwargs,
                    )
                    sleep(5)
                    remote_cleanup(
                        log_file_fp,
                        results_dir,
                        **kwargs,
                    )
                except Exception as e:
                    log_file_fp.write("Exception encountered:\n")
                    log_file_fp.write(str(e))
                    log_file_fp.write("\n")
                    log_file_fp.write(traceback.format_exc())
                    log_file_fp.write("\n")
                    remote_cleanup(
                        log_file_fp,
                        results_dir,
                        **kwargs,
                    )
                    raise e


def run_sim(
    trace_in,
    trace_meta,
    results_dir,
    **kwargs,
):
    kwargs = {**default_kwargs, **kwargs}
    kwargs["host"] = "NOT_SET_SIMULATION"
    os.makedirs(results_dir, exist_ok=True)
    log_file = os.path.join(results_dir, "orchestration.log")
    kwargs["simulation"] = True
    if kwargs["target"] == RunTarget.CONTROLLER:
        controller_config_file = os.path.join(results_dir, "controller.json")
        src = os.path.join(
            kwargs["ilu_home"], "iluvatar_controller/src/controller.dev.json"
        )
        shutil.copy(src, controller_config_file)
        kwargs["controller_config_file"] = controller_config_file

    src = os.path.join(
        kwargs["ilu_home"], "iluvatar_controller/src/controller.dev.json"
    )
    worker_config_file = os.path.join(results_dir, "worker.json")
    shutil.copy(src, worker_config_file)
    kwargs["worker_config_file"] = worker_config_file

    with open("./worker.dev.json") as js:
        config = json.load(js)

        def add(path, val):
            c = config
            for p in path[:-1]:
                if p in c:
                    pass
                else:
                    c[p] = dict()
                c = c[p]
            c[path[-1]] = val

        add(["container_resources", "memory_mb"], kwargs["memory"])
        add(["container_resources", "cpu_resource", "count"], kwargs["cores"])
        add(["invocation", "queues", "cpu"], "serial")
        add(["invocation", "queue_policies", "cpu"], kwargs["cpu_queue_policy"])
        add(["invocation", "queue_sleep_ms"], kwargs["stat_update"])
        add(["invocation", "enqueueing_policy"], kwargs["enqueueing"])
        add(["invocation", "queues", "gpu"], kwargs["gpu_queue"])
        add(["invocation", "queue_policies", "gpu"], kwargs["gpu_queue_policy"])

        add(["logging", "level"], kwargs["log_level"])
        add(["logging", "directory"], results_dir)
        add(["status", "report_freq_ms"], kwargs["stat_update"])

        add(["container_resources", "gpu_resource", "count"], kwargs["gpus"])
        add(["container_resources", "gpu_resource", "per_func_memory_mb"], 16 * 1024)
        add(["container_resources", "gpu_resource", "memory_mb"], 16 * 1024)
        add(["container_resources", "gpu_resource", "funcs_per_device"], kwargs["fpd"])
        add(
            ["container_resources", "gpu_resource", "concurrent_running_funcs"],
            kwargs["gpu_running"],
        )
        add(
            ["container_resources", "gpu_resource", "use_driver_hook"],
            kwargs["prefetch"],
        )
        add(["container_resources", "pool_freq_ms"], 1000)
        add(["invocation", "mqfq_config", "allowed_overrun"], kwargs["allowed_overrun"])
        add(["invocation", "mqfq_config", "service_average"], kwargs["service_average"])
        add(["invocation", "mqfq_config", "ttl_sec"], kwargs["mqfq_ttl_sec"])
        add(["invocation", "mqfq_config", "weight_logging_ms"], 0)
        add(["invocation", "mqfq_config", "flow_select_cnt"], kwargs["select_cnt"])
        add(
            ["invocation", "mqfq_config", "time_estimation"],
            kwargs["mqfq_time_estimation"],
        )
        add(
            ["invocation", "mqfq_config", "add_estimation_error"],
            kwargs["mqfq_add_estimation_error"],
        )

        add(["invocation", "landlord_config", "cache_size"], int(kwargs["cache_size"]))
        add(
            ["invocation", "landlord_config", "log_cache_info"],
            kwargs["log_cache_info"],
        )
        add(["invocation", "landlord_config", "max_size"], kwargs["lnd_max_size"])
        add(["invocation", "landlord_config", "load_thresh"], kwargs["lnd_load_thresh"])
        add(["invocation", "landlord_config", "fixed_mode"], kwargs["lnd_fixed_mode"])
        add(
            ["invocation", "landlord_config", "slowdown_thresh"],
            kwargs["lnd_slowdown_thresh"],
        )
        add(["invocation", "enqueuing_log_details"], kwargs["enqueuing_log_details"])

        add(["invocation", "greedy_weight_config", "allow"], kwargs["greedy_policy"])
        add(["invocation", "greedy_weight_config", "allow_load"], kwargs["greedy_load"])
        add(["invocation", "greedy_weight_config", "log"], kwargs["greedy_log"])
        add(
            ["invocation", "greedy_weight_config", "cache_size"],
            kwargs["greedy_cache_size"],
        )
        add(
            ["invocation", "greedy_weight_config", "fixed_assignment"],
            kwargs["greedy_fixed_assignment"],
        )

        with open(worker_config_file, "w") as dump:
            json.dump(config, dump)

    if not kwargs["force"] and has_results(
        results_dir, kwargs["function_trace_name"], log_file
    ):
        print(f"Skipping {results_dir}")
        return

    print(f"Running {results_dir}")
    run_load(
        log_file,
        results_dir,
        trace_in,
        trace_meta,
        **kwargs,
    )
