import json
from typing import List, Tuple
from collections import defaultdict
import os, pickle
import numpy as np
import pandas as pd
from copy import deepcopy
from multiprocessing import Pool
from functools import reduce

from ..run.run_trace import RunType, RunTarget


def get_from_dict(dataDict, mapList, default):
    """Iterate nested dictionary"""
    try:
        return reduce(dict.get, mapList, dataDict)
    except TypeError:
        return default


def trace_output(type, trace_in):
    addtl = ""
    if type == "json":
        addtl = "-full"
    name = trace_in.split(".")[0]
    return f"output{addtl}-{name}.{type}"


def format_bench_data(func: str) -> str:
    bench_name = func
    if "torch_rnn" not in func:
        func = func.replace("_", "-")
    else:
        func = func.replace("_", "-").replace("torch-rnn", "torch_rnn")
    parts = func.split("-")
    try:
        _num = int(parts[-1])
        bench_name = "-".join(parts[:-1])
    except:
        pass
    return bench_name


def get_bench_data(func, compute, metric, benchmark_data):
    bench_name = format_bench_data(func)
    return benchmark_data[bench_name]["resource_data"][compute][metric]


def get_start_and_finish(tid, data):
    for invoke in data:
        if invoke["tid"] == tid:
            start = pd.to_datetime(invoke["invoke_start"])
            finish = start + pd.Timedelta(
                invoke["client_latency_us"] / 1_000_000.00, unit="s"
            )
            return start, finish


class LogParser:
    """
    Class to help parse logs from the result of a single experiment.

    Currently only works with a single worker, and iffy on cluster support
    """

    def __init__(
        self,
        folder_path,
        input_csv,
        metadata_csv,
        benchmark_file: str = None,
        run_type: RunType = RunType.LIVE,
        target: RunTarget = RunTarget.WORKER,
    ):
        self.source = folder_path
        self.input_csv = input_csv
        self.metadata_csv = metadata_csv
        self.target = target
        self.run_type = run_type

        input_basename = os.path.basename(self.input_csv)
        csv = os.path.join(folder_path, trace_output("csv", input_basename))
        log = os.path.join(folder_path, "worker_worker1.log")
        if self.run_type.is_sim():
            log = os.path.join(folder_path, "load_gen.log")
        json = os.path.join(folder_path, trace_output("json", input_basename))
        self.load_gen = os.path.join(folder_path, "load_gen.log")

        has_csv = os.path.isfile(csv)
        has_log = os.path.isfile(log)
        if not has_csv or not has_log:
            raise Exception(f"Missing logs in '{folder_path}'")

        self.results_csv = csv
        self.results_log = log
        self.results_json = json
        self.benchmark_file = benchmark_file
        self.benchmark_data = None

    def has_benchmark_data(self):
        return self.benchmark_file is not None

    def get_compute(self, tid, data):
        for invoke in data:
            # print(invoke)
            if invoke["tid"] == tid:
                if self.target == RunTarget.WORKER:
                    compute = invoke["worker_response"]["compute"]
                else:
                    compute = invoke["controller_response"]["compute"]

                if compute == 1:
                    return "cpu"
                elif compute == 2:
                    return "gpu"
                elif compute == 0:
                    return "failed"
                else:
                    raise Exception(f"Unknown compute '{compute}' for '{tid}'")

    def get_container_state(self, tid, data):
        for invoke in data:
            if invoke["tid"] == tid:
                if self.target == RunTarget.WORKER:
                    state = invoke["worker_response"]["container_state"]
                else:
                    state = invoke["controller_response"]["container_state"]
                if state == 1:
                    return "unhealthy"
                if state == 2:
                    return "cold"
                elif state == 3:
                    return "prewarm"
                elif state == 4:
                    return "warm"
                elif state == 0:
                    return "failed"
                else:
                    raise Exception(f"Unknown state '{state}' for '{tid}'")

    status_data = []

    def parse_status(self, log):
        # {"timestamp":"2024-03-23 16:29:53.340644488","level":"INFO","fields":{"message":"current load status","tid":"Status","status":"{\"cpu_queue_len\":0,\"gpu_queue_len\":227,\"used_mem\":-27348,\"total_mem\":204800,\"cpu_us\":1.8986021281034844,\"cpu_sy\":1.4500312956394743,\"cpu_id\":96.65136657625703,\"cpu_wa\":0.0,\"load_avg_1minute\":1.27,\"num_system_cores\":96,\"num_running_funcs\":0,\"num_containers\":3,\"gpu_utilization\":[{\"gpu_uuid\":\"GPU-1f76a0b9-71c4-73d5-d9d7-95308fcd4226\",\"pstate\":\"P0\",\"memory_total\":16280,\"memory_used\":1571,\"instant_utilization_gpu\":0.0,\"utilization_gpu\":1.1145477457769821,\"utilization_memory\":0.038400000067554,\"power_draw\":31.370918634264637,\"power_limit\":250.0,\"num_running\":1,\"est_utilization_gpu\":1.1145477457769821}]}"},"target":"iluvatar_worker_library::services::status::status_service"}
        t = pd.to_datetime(log["timestamp"])
        status = json.loads(log["fields"]["status"])
        gpu_util = sum(
            [gpu["utilization_gpu"] for gpu in status["gpu_utilization"]]
        ) / len(status["gpu_utilization"])
        cpu_util = (
            status["cpu_us"] + status["cpu_sy"]
        )  # + status["cpu_id"] + status["cpu_wa"]
        instant = sum(
            [gpu["utilization_gpu"] for gpu in status["gpu_utilization"]]
        ) / len(status["gpu_utilization"])
        mem = sum(
            [
                float(gpu["memory_used"]) / float(gpu["memory_total"])
                for gpu in status["gpu_utilization"]
            ]
        ) / len(status["gpu_utilization"])
        self.status_data.append(
            (
                t,
                gpu_util,
                cpu_util,
                mem * 100.0,
                instant,
                status["num_containers"],
                status["num_running_funcs"],
                get_from_dict(
                    status,
                    ["queue_load", str(2), "len"],
                    status["gpu_queue_len"],
                ),
                get_from_dict(
                    status,
                    ["queue_load", str(1), "len"],
                    status["cpu_queue_len"],
                ),
                get_from_dict(status, ["queue_load", str(2), "load"], 0.0),
                get_from_dict(status, ["queue_load", str(2), "laod_avg"], 0.0),
                get_from_dict(status, ["queue_load", str(2), "tput"], 0.0),
                get_from_dict(status, ["queue_load", str(1), "load"], 0.0),
                get_from_dict(status, ["queue_load", str(1), "laod_avg"], 0.0),
                get_from_dict(status, ["queue_load", str(1), "tput"], 0.0),
            )
        )

    batch_data = []
    batch_name = None
    batch_size = 0
    batch_start = None
    has_batch_sizes = False

    def parse_batching(self, log):
        self.has_batch_sizes = True
        ts = pd.to_datetime(log["timestamp"])
        fname = log["fields"]["fqdn"]
        if self.batch_name is None:
            self.batch_start = ts
            self.batch_name = fname
            self.batch_size = log["fields"]["batch_len"]
        elif self.batch_name == fname:
            self.batch_size += log["fields"]["batch_len"]
        else:
            self.batch_data.append((self.batch_start, self.batch_name, self.batch_size))
            self.batch_start = ts
            self.batch_name = fname
            self.batch_size = log["fields"]["batch_len"]

    def parse_batching_alternate(self, log):
        if not self.has_batch_sizes:
            tid = log["fields"]["tid"]
            fname = self.invoke_tid_to_fname[tid]
            ts = pd.to_datetime(log["timestamp"])
            if batch_name is None:
                self.batch_start = ts
                self.batch_name = fname
                self.batch_size = 1
            elif self.batch_name == fname:
                self.batch_size += 1
            else:
                self.batch_data.append(
                    (self.batch_start, self.batch_name, self.batch_size)
                )
                self.batch_start = ts
                self.batch_name = fname
                self.batch_size = 1

    est_invoke_time_data = defaultdict(lambda: np.zeros(4))

    def gpu_queue_time_est(self, log):
        tid = log["fields"]["tid"]
        self.est_invoke_time_data[tid][2] = log["fields"]["qt"]
        self.est_invoke_time_data[tid][3] = log["fields"]["runtime"]

    def cpu_queue_time_est(self, log):
        tid = log["fields"]["tid"]
        self.est_invoke_time_data[tid][0] = log["fields"]["queue_time"]
        self.est_invoke_time_data[tid][1] = log["fields"]["runtime"]

    est_e2e_time_data = {}

    def est_e2e_time(self, log):
        tid = log["fields"]["tid"]
        cpu = log["fields"]["cpu_est"]
        gpu = log["fields"]["gpu_est"]
        cpu_load = log["fields"].get("cpu_load", 0.0)
        gpu_load = log["fields"].get("gpu_load", 0.0)
        self.est_e2e_time_data[tid] = np.array([cpu, gpu, cpu_load, gpu_load])

    def compute_traffic_classes(self):
        data = defaultdict(dict)
        last = {}
        iats = defaultdict(list)
        rares = []
        func_iats = []
        max_iat = 0
        classes = np.arange(0.0, 1.1, 0.1)
        with open(self.input_csv) as f:
            f.readline()
            for row in f.readlines():
                func_name, time_ms = row.split(",")
                if func_name in last:
                    iats[func_name].append(int(time_ms) - last[func_name])
                last[func_name] = int(time_ms)

        func_iats = np.array([np.mean(v) for v in iats.values() if len(v) > 1])
        iat_quants = np.quantile(func_iats, classes)
        df_data = []
        for func in iats.keys():
            if len(iats[func]) <= 1:
                df_data.append((func, max(func_iats) + 1.0, max(iat_quants)))
            else:
                mean_iat = np.mean(iats[func])
                for i, iat_class in enumerate(classes):
                    if mean_iat <= iat_quants[i]:
                        df_data.append((func, mean_iat, iat_class))
        class_df = pd.DataFrame.from_records(
            df_data,
            columns=["func_name", "avg_iat", "iat_class"],
        )
        self.metadata_df = pd.merge(
            self.metadata_df,
            class_df,
            how="left",
            left_on="func_name",
            right_on="func_name",
        )

    tid_to_queueing = []
    invoke_tid_to_fname = {}
    cache_insertions = defaultdict(int)
    est_invoke_time_data = defaultdict(lambda: np.zeros(4))

    def parse_worker_log(self):
        with open(self.results_log) as f:
            for log in f.readlines():
                try:
                    log = json.loads(log)
                except Exception as e:
                    print(log_file)
                    print(log)
                    raise e
                if "message" not in log["fields"]:
                    continue

                if log["fields"]["message"] == "CPU estimated completion time of item":
                    self.cpu_queue_time_est(log)
                if log["fields"]["message"] == "GPU estimated completion time of item":
                    self.gpu_queue_time_est(log)

                if log["fields"]["message"] == "Est e2e time":
                    self.est_e2e_time(log)

                if log["fields"]["message"] == "Cache Insertion":
                    cache_insertions[log["fields"]["fqdn"][:-6]] += 1

                if log["fields"]["message"] == "current load status":
                    self.parse_status(log)

                if log["fields"]["message"] == "Item starting to execute":
                    insert = pd.to_datetime(log["fields"]["insert_time"])
                    remove = pd.to_datetime(log["fields"]["remove_time"])
                    elapsed = remove - insert
                    self.tid_to_queueing.append(
                        (log["fields"]["tid"], elapsed.total_seconds(), insert, remove)
                    )

                if log["fields"]["message"] == "Executing batch":
                    self.parse_batching(log)

                if log["fields"]["message"] == "Handling invocation request":
                    tid = log["fields"]["tid"]
                    fname = log["fields"]["function_name"]
                    self.invoke_tid_to_fname[tid] = fname
                if log["fields"]["message"] == "Item starting to execute":
                    self.parse_batching_alternate(log)

    def compute_overheads(self):
        data = []
        for i, row in self.metadata_df.iterrows():
            sub_df = self.invokes_df[self.invokes_df["function_name"] == row.func_name]
            min_exec = sub_df["code_duration_sec"].min()
            cold_exec_time = warm_exec_time = warm_e2e_time = 1000
            supported_computes = "cpu"
            if "compute" in row.index:
                supported_computes = row.compute
            for compute in supported_computes.split("|"):
                # normalizing will happen by the best time
                cold_comp_exec_time = np.mean(
                    get_bench_data(
                        row.benchmark_name,
                        compute.lower(),
                        "cold_results_sec",
                        self.benchmark_data,
                    )
                )
                cold_exec_time = min(cold_exec_time, cold_comp_exec_time)
                warm_gpu_exec_time = np.mean(
                    get_bench_data(
                        row.benchmark_name,
                        compute.lower(),
                        "warm_results_sec",
                        self.benchmark_data,
                    )
                )
                warm_exec_time = min(warm_exec_time, warm_gpu_exec_time)
                comp_warm_e2e_time_sec = (
                    np.mean(
                        get_bench_data(
                            row.benchmark_name,
                            compute.lower(),
                            "warm_worker_duration_us",
                            self.benchmark_data,
                        )
                    )
                    / 1_000_000
                )
                warm_e2e_time = min(warm_e2e_time, comp_warm_e2e_time_sec)

            for invoke_row in sub_df.itertuples():
                if invoke_row.was_cold:
                    exec_time = cold_exec_time
                else:
                    exec_time = warm_exec_time
                compute = self.get_compute(invoke_row[0], self.json_data)
                state = self.get_container_state(invoke_row[0], self.json_data)
                start, finish = get_start_and_finish(invoke_row[0], self.json_data)

                e2e_sec = invoke_row.e2e_duration_us / 1_000_000
                # The latency normalized by the expected e2e time (using GPU compute device)
                norm_e2e = (e2e_sec - warm_e2e_time) / warm_e2e_time
                if min_exec == 0:
                    exec_overhead = (
                        max(0, invoke_row.code_duration_sec - exec_time) / exec_time
                    )
                else:
                    exec_overhead = (
                        max(0, invoke_row.code_duration_sec - min_exec) / min_exec
                    )

                share = len(sub_df) / len(self.invokes_df)
                exec_overhead_norm = exec_overhead * share
                e2e_overhead = e2e_sec - warm_e2e_time
                e2e_overhead_portion = norm_e2e * share
                e2e_norm_portion = (e2e_sec / warm_e2e_time) * share
                e2e_portion = e2e_sec * share

                queue_portion = invoke_row.queueing_sec * share

                data.append(
                    (
                        invoke_row[0],
                        compute,
                        e2e_overhead,
                        exec_overhead,
                        norm_e2e,
                        e2e_overhead_portion,
                        e2e_sec,
                        queue_portion,
                        state,
                        exec_overhead_norm,
                        e2e_portion,
                        e2e_norm_portion,
                        start,
                        finish,
                    )
                )
        overhead_df = pd.DataFrame.from_records(
            data,
            columns=[
                "tid",
                "compute",
                "e2e_overhead",
                "exec_overhead",
                "norm_e2e",
                "e2e_overhead_portion",
                "e2e_sec",
                "queue_portion",
                "container_state",
                "exec_overhead_norm",
                "e2e_portion",
                "e2e_norm_portion",
                "start",
                "finish",
            ],
            index="tid",
        )
        self.invokes_df = self.invokes_df.join(overhead_df)

    def get_benchmark_mapped_names(self):
        name_map_data = []
        found = set()
        with open(self.load_gen) as f:
            for log in f.readlines():
                # TODO: parse load gen to get mapped code names
                log = json.loads(log)
                if (
                    log["fields"]["message"]
                    == "Function mapped to self name in benchmark"
                ):
                    name_map_data.append(
                        (log["fields"]["function"], log["fields"]["chosen_code"])
                    )
                    found.add(log["fields"]["function"])
                if (
                    log["fields"]["message"]
                    == "Function mapped to exact name in benchmark"
                ):
                    name_map_data.append(
                        (log["fields"]["function"], log["fields"]["function"])
                    )
                    found.add(log["fields"]["function"])
                if log["fields"]["message"] == "Function mapped to benchmark code":
                    name_map_data.append(
                        (log["fields"]["function"], log["fields"]["chosen_code"])
                    )
                    found.add(log["fields"]["function"])
        for i, row in self.metadata_df.iterrows():
            if row.func_name not in found:
                # If item doesn't have log message in load gen file, it gave a specific image in metadata
                image_name = row.image_name.split("/")[-1].split(":")[0]
                name_map_data.append((row.func_name, image_name.split("-")[0]))

        name_map = pd.DataFrame.from_records(
            name_map_data, columns=["func_name", "benchmark_name"]
        )
        self.metadata_df = pd.merge(
            self.metadata_df,
            name_map,
            how="left",
            left_on="func_name",
            right_on="func_name",
        )

    def parse_logs(
        self,
        include_errors: bool = False,
        fail_if_errors: bool = False,
    ):
        self.metadata_df = pd.read_csv(self.metadata_csv)

        self.invokes_df = pd.read_csv(self.results_csv, index_col="tid")
        if not include_errors:
            self.invokes_df = self.invokes_df[self.invokes_df["success"]]

        with open(self.results_json) as f:
            self.json_data = json.load(f)
        if self.has_benchmark_data():
            with open(self.benchmark_file) as f:
                self.benchmark_data = json.load(f)["data"]

        if self.has_benchmark_data():
            self.get_benchmark_mapped_names()

        status_df = pd.DataFrame.from_records(
            self.status_data,
            columns=[
                "timestamp",
                "gpu_util",
                "cpu_util",
                "mem",
                "instant",
                "num_containers",
                "num_running",
                "gpu_queue_len",
                "cpu_queue_len",
                "gpu_load",
                "gpu_load_avg",
                "gpu_tput",
                "cpu_load",
                "cpu_load_avg",
                "cpu_tput",
            ],
        )
        status_df["norm_time"] = status_df["timestamp"] - status_df["timestamp"].min()
        self.status_df = status_df

        batch_df = pd.DataFrame.from_records(
            self.batch_data, columns=["timestamp", "fqdn", "batch_len"]
        )
        batch_df["norm_time"] = batch_df["timestamp"] - batch_df["timestamp"].min()
        self.batch_df = batch_df

        queueing_df = pd.DataFrame.from_records(
            self.tid_to_queueing,
            columns=["tid", "queueing_sec", "insert", "remove"],
            index="tid",
        )
        self.invokes_df = self.invokes_df.join(queueing_df)
        self.invokes_df["queue_exec_sec"] = (
            self.invokes_df["queueing_sec"] + self.invokes_df["code_duration_sec"]
        )
        if len(self.est_e2e_time_data) > 0:
            est_time_df = pd.DataFrame.from_dict(
                self.est_e2e_time_data,
                columns=[
                    "cpu_est_sec",
                    "gpu_est_sec",
                    "insert_cpu_load",
                    "insert_gpu_load",
                ],
                orient="index",
            )
            self.invokes_df = self.invokes_df.join(est_time_df)

        if len(self.est_invoke_time_data) > 0:
            est_time_df = pd.DataFrame.from_dict(
                self.est_invoke_time_data,
                columns=[
                    "cpu_est_queue_sec",
                    "cpu_est_runtime_sec",
                    "gpu_est_queue_sec",
                    "gpu_est_runtime_sec",
                ],
                orient="index",
            )
            self.invokes_df = self.invokes_df.join(est_time_df)

        if len(self.cache_insertions) > 0:
            self.invokes_df["cache_insertions"] = self.invokes_df[
                "function_name"
            ].apply(lambda x: self.cache_insertions[x])

        if self.has_benchmark_data():
            self.compute_overheads()

        self.compute_traffic_classes()

        if len(self.est_e2e_time_data) > 0:
            gpus = self.invokes_df[self.invokes_df["compute"] == "gpu"]
            cpus = self.invokes_df[self.invokes_df["compute"] == "cpu"]
            gpu_times = gpus["gpu_est_sec"] - gpus["queue_exec_sec"]
            cpu_times = cpus["cpu_est_sec"] - cpus["queue_exec_sec"]
            times = pd.concat([cpu_times, gpu_times])
            self.invokes_df["est_sec_diff"] = times
