#!/usr/bin/python3
import os
from time import sleep
import json
import traceback
from enum import Enum
from typing import Optional
import shutil
from .config import LoadConfig

from .multiproc import *
from .ansible import (
    _run_cmd,
    _copy_logs,
    _pre_run_cleanup,
    _remote_cleanup,
    _run_ansible,
    _run_ansible_clean,
    RunTarget,
)
from .logging import create_logger


class BuildTarget(Enum):
    DEBUG = "debug"
    DEBUG_SPANS = "spansd"
    RELEASE = "release"
    RELEASE_WITH_DEBUG = "relwdebug"
    RELEASE_SPANS = "spans"

    def __str__(self) -> str:
        return self.value

    def make_name(self) -> str:
        return str(self)

    def path_name(self) -> str:
        return self.value


def rust_build(ilu_home, log_file: Optional[str]=None, build: BuildTarget = BuildTarget.RELEASE, target_arch: str = "x86_64-unknown-linux-gnu"):
    """
    Build the solution 'src/Ilúvatar' is located.

    :param ilu_home: Directory where the
    :param log_file: Optional log file to write build stdout to
    :param build: build target
    :param target_arch: rustc target triple build target architecture
    """
    pwd = os.getcwd()
    os.chdir(ilu_home)
    build_args = ["make", build.make_name()]
    logger = create_logger(log_file)
    _run_cmd(build_args, logger, env={"TARGET_PLAT":target_arch})
    os.chdir(pwd)

def rust_build_native(ilu_home, log_file: Optional[str]=None, build: BuildTarget = BuildTarget.RELEASE, target_arch: str = "x86_64-unknown-linux-gnu"):
    """
    Build Ilúvatar to run on the local CPU, with native optimizations and features enabled.
    WARNING: This build will only work on the local machine as it uses CPU-family specific instructions. Be aware of how you use it

    :param ilu_home: Directory where the
    :param log_file: Optional log file to write build stdout to
    :param build: build target
    :param target_arch: rustc target triple build target architecture
    """
    pwd = os.getcwd()
    os.chdir(ilu_home)
    build_args = ["make", build.make_name()]
    logger = create_logger(log_file)
    _run_cmd(build_args, logger, env={"TARGET_CPU":"native","TARGET_PLAT":target_arch})
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


def trace_output(type, trace_in):
    basename = trace_base_name(trace_in)
    addtl = ""
    if type == "json":
        addtl = "-full"
    return f"output{addtl}-{basename}.{type}"


def has_results(results_dir, function_trace_name):
    output_json = os.path.join(results_dir, trace_output("json", function_trace_name))
    output_csv = os.path.join(results_dir, trace_output("csv", function_trace_name))
    if not os.path.exists(output_json):
        return False
    if not os.path.exists(output_csv):
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
    return True


def trace_base_name(trace_in_csv: str):
    return os.path.splitext(os.path.basename(trace_in_csv))[0]


def _run_load(logger, results_dir, input_csv, metadata, kwargs):
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

    load_env = kwargs.to_env_var_dict("load")
    _run_cmd(load_args, logger, env=load_env)


def ansible_clean(log_file: str, **kwargs):
    kwargs = load_kwargs(**kwargs)
    logger = create_logger(log_file)
    _run_ansible_clean(logger, kwargs)


def copy_logs(log_file: str, results_dir, **kwargs):
    kwargs = load_kwargs(**kwargs)
    logger = create_logger(log_file)
    _copy_logs(logger, results_dir, kwargs)


def pre_run_cleanup(log_file: str, results_dir, **kwargs):
    kwargs = load_kwargs(**kwargs)
    logger = create_logger(log_file)
    _pre_run_cleanup(logger, results_dir, kwargs)


def remote_cleanup(log_file: str, results_dir, **kwargs):
    kwargs = load_kwargs(**kwargs)
    logger = create_logger(log_file)
    _remote_cleanup(logger, results_dir, kwargs)


def run_ansible(log_file: str, **kwargs):
    kwargs = load_kwargs(**kwargs)
    logger = create_logger(log_file)
    _run_ansible(logger, kwargs)


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

load_gen_kwargs = [
    ("load_type", "functions"),
    ("prewarm", 1),
    ("simulation", None),
    ("tick_step", 300),
    ("sim_gran", "us"),
    ("num_workers", 1),
    ("target", RunTarget.WORKER),
    ("load_log_level", "info", ("level",)),
    ("load_log_stdout", True, ("stdout",)),
    ("load_log_spanning", "NONE", ("spanning",)),
    ("load_spans_json", False, ("include_spans_json",)),
]

controller_kwargs = [
    ("controller_log_dir", "/tmp/iluvatar/logs/ansible", ("logging", "directory")),
    ("controller_spanning", "NONE", ("logging", "spanning")),
    ("controller_include_spans_json", False, ("logging", "include_spans_json")),
    ("controller_log_level", "info", ("logging", "level")),
    ("controller_port", 8089, ("port",)),
    ("controller_algorithm", "CHRLU", ("load_balancer", "algorithm", "type")),
    ("controller_thread_sleep_ms", 500, ("load_balancer", "algorithm", "load_metric", "thread_sleep_ms")),
    ("controller_load_metric", "LoadAvg", ("load_balancer", "algorithm", "load_metric", "load_metric")),
    # CH-RLU
    ("controller_popular_pct", 0.1, ("load_balancer", "algorithm", "popular_pct")),
    ("controller_bounded_ceil", 1.5, ("load_balancer", "algorithm", "bounded_ceil")),
    ("controller_chain_len", 4, ("load_balancer", "algorithm", "chain_len")),
    ("controller_lb_vnodes", 3, ("load_balancer", "algorithm", "vnodes")),
]
worker_kwargs = [
    ("worker_port", 8070, ("port",)),
    ("load_balancer_url", "", ("load_balancer_url",)),
    # limits
    ("mem_min_mb", 5, ("limits", "mem_min_mb")),
    ("mem_max_mb", 5000, ("limits", "mem_max_mb")),
    ("cpu_max", 1, ("limits", "cpu_max")),
    ("timeout_sec", 60 * 60, ("limits", "timeout_sec")),
    # invoke basics
    ("memory", 20 * 1024, ("container_resources", "memory_mb")),
    ("cores", 12, ("container_resources", "cpu_resource", "count")),
    ("cpu_queue", "serial", ("invocation", "queues", "CPU")),
    ("cpu_queue_policy", "minheap_ed", ("invocation", "queue_policies", "CPU")),
    ("gpu_queue", "mqfq", ("invocation", "queues", "GPU")),
    (
        "gpu_queue_policy",
        "mqfq_select_out_len",
        ("invocation", "queue_policies", "GPU"),
    ),
    ("enqueueing", "QueueAdjustAvgEstSpeedup", ("invocation", "enqueueing_policy")),
    ("invoke_queue_sleep_ms", 500, ("invocation", "queue_sleep_ms")),
    ("enqueuing_log_details", False, ("invocation", "enqueuing_log_details")),
    # docker
    (
        "docker_avoid_pull",
        "true",
        ("container_resources", "docker_config", "avoid_pull"),
    ),
    (
        "docker_username",
        "",
        ("container_resources", "docker_config", "auth", "username"),
    ),
    (
        "docker_password",
        "",
        ("container_resources", "docker_config", "auth", "password"),
    ),
    (
        "docker_repository",
        "",
        ("container_resources", "docker_config", "auth", "repository"),
    ),
    # logging
    ("log_level", "info", ("logging", "level")),
    ("worker_spanning", "NONE", ("logging", "spanning")),
    ("worker_log_dir", "/tmp/iluvatar/logs/ansible", ("logging", "directory")),
    ("worker_include_spans_json", False, ("logging", "include_spans_json")),
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
    ("gpu_memory", 16*1024, ("container_resources", "gpu_resource", "memory_mb")),
    ("fpd", 32, ("container_resources", "gpu_resource", "funcs_per_device")),
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
    ("mig_shares", 0, ("container_resources", "gpu_resource", "mig_shares")),
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
]
def load_kwargs(**kwargs):
    default_kwargs = LoadConfig()
    default_kwargs.bulk_add("runner", runner_config_kwargs)
    default_kwargs.bulk_add("load", load_gen_kwargs, env_var="LOAD_GEN")
    default_kwargs.bulk_add("controller", controller_kwargs, env_var="ILUVATAR_CONTROLLER")
    default_kwargs.bulk_add("worker", worker_kwargs, env_var="ILUVATAR_WORKER")
    default_kwargs.overwrite(**kwargs)
    return default_kwargs

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
    kwargs = load_kwargs(**kwargs)
    os.makedirs(results_dir, exist_ok=True)
    log_file = os.path.join(results_dir, "orchestration.log")
    logger = create_logger(log_file)
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

            try:
                _pre_run_cleanup(
                    logger,
                    results_dir,
                    kwargs,
                )
                _run_ansible(logger, kwargs)
                sleep(5)
                _run_load(
                    logger,
                    results_dir,
                    trace_in,
                    trace_meta,
                    kwargs,
                )
                sleep(5)
                _remote_cleanup(
                    logger,
                    results_dir,
                    kwargs,
                )
            except Exception as e:
                msg = "\n".join([
                    "Exception encountered:",
                    str(e),
                    traceback.format_exc()
                ])
                logger.error(msg)
                _remote_cleanup(
                    logger,
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
    kwargs = load_kwargs(**kwargs)
    kwargs["host"] = "NOT_SET_SIMULATION"
    os.makedirs(results_dir, exist_ok=True)
    log_file = os.path.join(results_dir, "orchestration.log")
    logger = create_logger(log_file)
    kwargs["simulation"] = True
    kwargs["function_trace_name"] = trace_base_name(trace_in)

    if not kwargs["force"] and has_results(results_dir, kwargs["function_trace_name"]):
        print(f"Skipping {results_dir}")
        return

    if kwargs["target"] == RunTarget.CONTROLLER:
        kwargs["worker_include_spans_json"] = True
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
        logger,
        results_dir,
        trace_in,
        trace_meta,
        kwargs,
    )
