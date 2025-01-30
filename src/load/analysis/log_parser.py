import json
from typing import List, Tuple, Optional, Dict
from collections import defaultdict
import os, pickle
import numpy as np
import pandas as pd
from copy import deepcopy
from multiprocessing import Pool
from functools import reduce
from ..run.run_trace import (
    RunType,
    RunTarget,
    has_results,
    trace_base_name,
    trace_output,
)


def get_from_dict(dataDict, mapList, default):
    """Iterate nested dictionary"""
    try:
        return reduce(dict.get, mapList, dataDict)
    except TypeError:
        return default


def get_from_dict_no_none(dataDict, mapList, default):
    """Iterate nested dictionary"""
    try:
        ret = reduce(dict.get, mapList, dataDict)
        if ret is None:
            return default
        return ret
    except TypeError:
        return default


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


class BaseParser:
    """
    A base class to allow injection of custom log parsing code into a re-usable log parsing setup.
    Parsers should NOT hold onto data assuming it can be removed later. Python's `pickle` gets weird.
    All info should be put in `main_parser`

    `parser_map`: a dict of string -> function inside the type.
        The string corresponds to a message in the logs,
        then the corresponding function will be called on that log.
    """

    parser_map = {}

    def __init__(self, main_parser):
        """
        `main_parser`: is of type LogParser
        """
        self.main_parser = main_parser

    def before_parse(self):
        """
        Called after results files have been loaded, but before logs start parsing.
        To make add details to metadata_df or invokes_df dataframe as wanted.
        """
        pass

    def log_completed(self):
        """
        Called after the entire set of logs has been parsed and passed to this parser, to allow for data to be merged with main parser.
        """
        pass


class LogParser:
    """
    Class to help parse logs from the result of a single experiment.
    Relies on injected sub-parsers to handle post-processing.
    This class loads the experiment artifacts and then calls these parsers to do analysis on them to add more information.
    Sub-parsers must be all injected before starting to parse data.

    Currently works best with a single worker, and iffy on cluster support.
    TODO: improve this for multiple workers & controller support
    TODO: multiple workers inside simulation will need change to logging framework to allow for log distinction
    """

    def __init__(
        self,
        folder_path: str,
        input_csv: str,
        metadata_csv: str,
        benchmark_file: str = None,
        run_type: RunType = RunType.LIVE,
        target: RunTarget = RunTarget.WORKER,
        run_data: Optional[Dict] = None,
    ):
        """
        run_data: metadata about the experimental run
        """
        self.source = folder_path
        self.input_csv = input_csv
        self.metadata_csv = metadata_csv
        self.target = target
        self.run_type = run_type
        self.run_data = run_data

        if not has_results(folder_path, self.input_csv):
            raise Exception(f"Missing logs in '{folder_path}'")

        self.load_gen = os.path.join(folder_path, "load_gen.log")

        self.results_csv = os.path.join(
            folder_path, trace_output("csv", self.input_csv)
        )
        self.results_log = os.path.join(folder_path, "worker_worker1.log")
        if self.run_type.is_sim():
            self.results_log = os.path.join(folder_path, "load_gen.log")
        self.results_json = os.path.join(
            folder_path, trace_output("json", self.input_csv)
        )
        self.benchmark_file = benchmark_file
        self.benchmark_data = None
        self.parser_map = defaultdict(list)
        self.parsers = []
        self.load_parsers()

    def load_parsers(self):
        for parser in LogParser.registered_parser_types:
            parser_instance = parser(self)
            for message, func in parser.parser_map.items():
                self.parser_map[message].append((parser_instance, func))
            self.parsers.append(parser_instance)

    registered_parser_types = []

    @classmethod
    def register_parser(cls, parser: BaseParser):
        """
        Parsers are registered in the order they are loaded by Python.
        So some have graph dependencies on one another that must be respected.
        """
        cls.registered_parser_types.append(parser)

    def has_benchmark_data(self):
        return self.benchmark_file is not None

    def _parse_worker_log(self):
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

                log_msg = log["fields"]["message"]
                if log_msg in self.parser_map:
                    for instance, func in self.parser_map[log_msg]:
                        func(instance, log)

    def parse_logs(
        self,
        include_errors: bool = False,
        fail_if_errors: bool = False,
    ):
        self.metadata_df = pd.read_csv(self.metadata_csv, index_col="func_name")
        self.invokes_df = pd.read_csv(self.results_csv, index_col="tid")
        if fail_if_errors:
            if not self.invokes_df["success"].all():
                raise Exception(
                    f"Experiment had invocation failures '{self.results_log}'"
                )
        if not include_errors:
            self.invokes_df = self.invokes_df[self.invokes_df["success"]]

        with open(self.results_json) as f:
            self.json_data = json.load(f)
        if self.has_benchmark_data():
            with open(self.benchmark_file) as f:
                self.benchmark_data = json.load(f)["data"]

        for instance in self.parsers:
            instance.before_parse()

        self._parse_worker_log()

        for instance in self.parsers:
            instance.log_completed()


@LogParser.register_parser
class BenchmarkNameParser(BaseParser):
    """
    Map functions to the code names in the benchmark file, if it exists.
    """

    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)

    def get_benchmark_mapped_names(self):
        name_map_data = []
        found = set()
        with open(self.main_parser.load_gen) as f:
            for log in f.readlines():
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
        for index, row in self.main_parser.metadata_df.iterrows():
            if index not in found:
                # If item doesn't have log message in load gen file, it gave a specific image in metadata
                image_name = row.image_name.split("/")[-1].split(":")[0]
                name_map_data.append((index, image_name.split("-")[0]))

        name_map = pd.DataFrame.from_records(
            name_map_data, columns=["func_name", "benchmark_name"], index="func_name"
        )
        self.main_parser.metadata_df = self.main_parser.metadata_df.join(
            name_map,
        )

    def before_parse(self):
        if self.main_parser.has_benchmark_data():
            self.get_benchmark_mapped_names()


@LogParser.register_parser
class MetadataTrafficClassesParser(BaseParser):
    """
    Compute function traffic classes from IATs
    """

    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)

    def before_parse(self):
        data = defaultdict(dict)
        last = {}
        iats = defaultdict(list)
        rares = []
        func_iats = []
        max_iat = 0
        classes = np.arange(0.0, 1.1, 0.1)
        with open(self.main_parser.input_csv) as f:
            f.readline()  # dump headers
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
                        break
        class_df = pd.DataFrame.from_records(
            df_data, columns=["func_name", "avg_iat", "iat_class"], index="func_name"
        )
        self.main_parser.metadata_df = self.main_parser.metadata_df.join(
            class_df, how="outer"
        )


@LogParser.register_parser
class QueuingParser(BaseParser):
    """
    Compute per-invocation queuing times
    """

    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)
        self.tid_to_queueing = []

    def queue_exit(self, log):
        insert = pd.to_datetime(log["fields"]["insert_time"])
        remove = pd.to_datetime(log["fields"]["remove_time"])
        elapsed = remove - insert
        self.tid_to_queueing.append(
            (log["fields"]["tid"], elapsed.total_seconds(), insert, remove)
        )

    def log_completed(self):
        queueing_df = pd.DataFrame.from_records(
            self.tid_to_queueing,
            columns=["tid", "queueing_sec", "insert", "remove"],
            index="tid",
        )
        self.main_parser.invokes_df = self.main_parser.invokes_df.join(queueing_df)
        self.main_parser.invokes_df["queue_exec_sec"] = (
            self.main_parser.invokes_df["queueing_sec"]
            + self.main_parser.invokes_df["code_duration_sec"]
        )

    parser_map = {
        "Item starting to execute": queue_exit,
    }


@LogParser.register_parser
class FullJsonMergeParser(BaseParser):
    """
    Merge data about each invocation (compute, start time, finish time, container state)
    from full invocation json data file and add to invocations.
    """

    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)

    def get_start_and_finish(self, invoke):
        start = pd.to_datetime(invoke["invoke_start"])
        finish = start + pd.Timedelta(
            invoke["client_latency_us"] / 1_000_000.00, unit="s"
        )
        return start, finish

    def get_compute(self, invoke):
        if self.main_parser.target == RunTarget.WORKER:
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

    def get_container_state(self, invoke):
        if self.main_parser.target == RunTarget.WORKER:
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

    def before_parse(self):
        data = []
        for invoke in self.main_parser.json_data:
            tid = invoke["tid"]
            compute = self.get_compute(invoke)
            state = self.get_container_state(invoke)
            start, finish = self.get_start_and_finish(invoke)
            data.append((tid, compute, state, start, finish))
        full_df = pd.DataFrame.from_records(
            data, columns=["tid", "compute", "state", "start", "finish"], index="tid"
        )
        self.main_parser.invokes_df = self.main_parser.invokes_df.join(full_df)


@LogParser.register_parser
class EstTimeParser(BaseParser):
    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)
        self.est_e2e_time_data = {}
        self.est_invoke_time_data = defaultdict(lambda: np.zeros(4))

    def est_e2e_time(self, log):
        tid = log["fields"]["tid"]
        cpu = log["fields"]["cpu_est"]
        gpu = log["fields"]["gpu_est"]
        cpu_load = log["fields"].get("cpu_load", 0.0)
        gpu_load = log["fields"].get("gpu_load", 0.0)
        self.est_e2e_time_data[tid] = np.array([cpu, gpu, cpu_load, gpu_load])

    def gpu_queue_time_est(self, log):
        tid = log["fields"]["tid"]
        self.est_invoke_time_data[tid][2] = log["fields"]["qt"]
        self.est_invoke_time_data[tid][3] = log["fields"]["runtime"]

    def cpu_queue_time_est(self, log):
        tid = log["fields"]["tid"]
        self.est_invoke_time_data[tid][0] = log["fields"]["queue_time"]
        self.est_invoke_time_data[tid][1] = log["fields"]["runtime"]

    def log_completed(self):
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
            self.main_parser.invokes_df = self.main_parser.invokes_df.join(est_time_df)

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
            self.main_parser.invokes_df = self.main_parser.invokes_df.join(est_time_df)
        if len(self.est_e2e_time_data) > 0:
            gpus = self.main_parser.invokes_df[
                self.main_parser.invokes_df["compute"] == "gpu"
            ]
            cpus = self.main_parser.invokes_df[
                self.main_parser.invokes_df["compute"] == "cpu"
            ]
            gpu_times = gpus["gpu_est_sec"] - gpus["queue_exec_sec"]
            cpu_times = cpus["cpu_est_sec"] - cpus["queue_exec_sec"]
            times = pd.concat([cpu_times, gpu_times])
            self.main_parser.invokes_df["est_sec_diff"] = times

    parser_map = {
        "Est e2e time": est_e2e_time,
        "GPU estimated completion time of item": gpu_queue_time_est,
        "CPU estimated completion time of item": cpu_queue_time_est,
    }


@LogParser.register_parser
class StatusParser(BaseParser):
    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)
        self.status_data = []

    def parse_status(self, log):
        t = pd.to_datetime(log["timestamp"])
        status = json.loads(log["fields"]["status"])
        gpu_util = 0
        gpu_mem = 0
        gpu_instant = 0
        if len(status["gpu_utilization"]) > 0:
            gpu_util = sum(
                [gpu["utilization_gpu"] for gpu in status["gpu_utilization"]]
            ) / len(status["gpu_utilization"])
            gpu_instant = sum(
                [gpu["utilization_gpu"] for gpu in status["gpu_utilization"]]
            ) / len(status["gpu_utilization"])
            gpu_mem = sum(
                [
                    float(gpu["memory_used"]) / float(gpu["memory_total"])
                    for gpu in status["gpu_utilization"]
                ]
            ) / len(status["gpu_utilization"])
        cpu_util = get_from_dict_no_none(
            status, ["cpu_us"], 0.0
        ) + get_from_dict_no_none(status, ["cpu_sy"], 0.0)
        self.status_data.append(
            (
                t,
                gpu_util,
                cpu_util,
                gpu_mem * 100.0,
                gpu_instant,
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

    def log_completed(self):
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
        self.main_parser.status_df = status_df

    parser_map = {"current load status": parse_status}


@LogParser.register_parser
class BatchingParser(BaseParser):
    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)
        self.batch_data = []
        self.batch_name = None
        self.batch_size = 0
        self.batch_start = None
        self.has_batch_sizes = False

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
            invoke = self.main_parser.invokes_df[
                self.main_parser.invokes_df.index == tid
            ]
            if len(invoke) == 0:
                # invocation had an error somewhere
                return
            fname = invoke.iloc[0]["function_name"]
            ts = pd.to_datetime(log["timestamp"])
            if self.batch_name is None:
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

    def log_completed(self):
        batch_df = pd.DataFrame.from_records(
            self.batch_data, columns=["timestamp", "fqdn", "batch_len"]
        )
        batch_df["norm_time"] = batch_df["timestamp"] - batch_df["timestamp"].min()
        self.main_parser.batch_df = batch_df

    parser_map = {
        "Executing batch": parse_batching,
        "Item starting to execute": parse_batching_alternate,
    }


@LogParser.register_parser
class CacheInsertionsParser(BaseParser):
    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)
        self.cache_insertions = defaultdict(int)

    def cache_insert(self, log):
        self.cache_insertions[log["fields"]["fqdn"][:-6]] += 1

    def log_completed(self):
        if len(self.cache_insertions) > 0:
            self.main_parser.invokes_df["cache_insertions"] = (
                self.main_parser.invokes_df["function_name"].apply(
                    lambda x: self.cache_insertions[x]
                )
            )

    parser_map = {
        "Cache Insertion": cache_insert,
    }


@LogParser.register_parser
class OverheadParser(BaseParser):
    def __init__(self, main_parser: LogParser):
        super().__init__(main_parser)

    def compute_overheads(self):
        data = []
        for index, row in self.main_parser.metadata_df.iterrows():
            sub_df = self.main_parser.invokes_df[
                self.main_parser.invokes_df["function_name"] == index
            ]
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
                        self.main_parser.benchmark_data,
                    )
                )
                cold_exec_time = min(cold_exec_time, cold_comp_exec_time)
                warm_gpu_exec_time = np.mean(
                    get_bench_data(
                        row.benchmark_name,
                        compute.lower(),
                        "warm_results_sec",
                        self.main_parser.benchmark_data,
                    )
                )
                warm_exec_time = min(warm_exec_time, warm_gpu_exec_time)
                comp_warm_e2e_time_sec = (
                    np.mean(
                        get_bench_data(
                            row.benchmark_name,
                            compute.lower(),
                            "warm_worker_duration_us",
                            self.main_parser.benchmark_data,
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

                share = len(sub_df) / len(self.main_parser.invokes_df)
                exec_overhead_norm = exec_overhead * share
                e2e_overhead = e2e_sec - warm_e2e_time
                e2e_overhead_portion = norm_e2e * share
                e2e_norm_portion = (e2e_sec / warm_e2e_time) * share
                e2e_portion = e2e_sec * share

                queue_portion = invoke_row.queueing_sec * share

                data.append(
                    (
                        invoke_row[0],
                        e2e_overhead,
                        exec_overhead,
                        norm_e2e,
                        e2e_overhead_portion,
                        e2e_sec,
                        queue_portion,
                        exec_overhead_norm,
                        e2e_portion,
                        e2e_norm_portion,
                    )
                )
        overhead_df = pd.DataFrame.from_records(
            data,
            columns=[
                "tid",
                "e2e_overhead",
                "exec_overhead",
                "norm_e2e",
                "e2e_overhead_portion",
                "e2e_sec",
                "queue_portion",
                "exec_overhead_norm",
                "e2e_portion",
                "e2e_norm_portion",
            ],
            index="tid",
        )
        self.main_parser.invokes_df = self.main_parser.invokes_df.join(overhead_df)

    def log_completed(self):
        if self.main_parser.has_benchmark_data():
            self.compute_overheads()


def _load_single_data(
    folder_path: str,
    input_csv: str,
    metadata_csv: str,
    benchmark_file: str,
    run_type: RunType,
    target: RunTarget,
    include_errors: bool = False,
    fail_if_errors: bool = False,
    run_data: Optional[Dict] = None,
) -> LogParser:
    parser = LogParser(
        folder_path, input_csv, metadata_csv, benchmark_file, run_type, target, run_data
    )
    parser.parse_logs()
    # Pickle doesn't like the parsers, so just nuke them as they shouldn't be holding info anyway
    del parser.parser_map
    del parser.parsers

    return parser


def _recurse(
    path: str,
    folder_structure: List[str],
    trace_in: str,
    depth=0,
    data=None,
) -> List[Tuple[str, str]]:
    csv_files = []
    if data is None:
        data = dict()
    if not os.path.exists(path):
        return csv_files
    for sub_pth in os.listdir(path):
        pth = os.path.join(path, sub_pth)
        if os.path.isdir(pth):
            if depth >= len(folder_structure):
                return csv_files

            if has_results(pth, trace_in):
                recur = deepcopy(data)
                recur[folder_structure[depth]] = sub_pth
                csv_files.append((pth, recur))
            else:
                recur = deepcopy(data)
                recur[folder_structure[depth]] = sub_pth
                csv_files += _recurse(
                    pth,
                    folder_structure,
                    trace_in,
                    depth + 1,
                    recur,
                )
    return csv_files


def parse_data(
    start_folder: str,
    input_csv: str,
    metadata_csv: str,
    benchmark_file: str,
    folder_structure: List[str],
    run_type: RunType,
    target: RunTarget,
    filter_fn=None,
    include_errors=False,
    fail_if_errors: bool = False,
    num_procs: Optional[int] = None,
) -> List[LogParser]:
    """
    Bulk load experiment results from a structured folder.
    Uses a Python multiprocessing Pool to load them.
    `num_procs`: number of sub-processes to make in Pool.

    An example of using this function:
    ```
    def filter_fn(found_results):
        ret = []
        for pth, run_data in found_results:
            if int(run_data["cpu_cores"]) != 16:
                ret.append((pth, run_data))
        return ret

    folder_structure = ["cpu_queue", "cpu_cores"]
    parse_data(
        results_dir,
        input_csv,
        meta_csv,
        benchmark,
        folder_structure,
        RunType.SIM,
        RunTarget.WORKER,
        filter_fn=filter_fn,
    )
    ```
    """
    csv_files = _recurse(start_folder, folder_structure, input_csv)
    if filter_fn is not None:
        csv_files = filter_fn(csv_files)
    if len(csv_files) == 0:
        raise Exception(f"Could not get any results in '{start_folder}'")

    load_data = []
    for pth, run_data in csv_files:
        load_data.append(
            (
                pth,
                input_csv,
                metadata_csv,
                benchmark_file,
                run_type,
                target,
                include_errors,
                fail_if_errors,
                run_data,
            )
        )

    with Pool(num_procs) as p:
        parsed_data = p.starmap(_load_single_data, load_data)
    return parsed_data
