import json
from string import whitespace
from typing import List, Tuple, Optional, Dict, Set
from collections import defaultdict
import os, pickle
import numpy as np
import pandas
import pandas as pd
from datetime import datetime, timedelta
from copy import deepcopy
from multiprocessing import Pool
from functools import reduce
import tz
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


def timestamp_to_pddate(time):
    try:
        return pd.to_datetime(time)
    except:
        ptime = pd.to_datetime(time, format="%Y-%m-%d %H:%M:%S:%f+")
        ptime = ptime.replace(tzinfo=tz.tzutc())
        local = ptime.astimezone(tz.tzlocal())
        local = local.replace(tzinfo=None)
        return local


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
        `main_parser`: is of type WorkerLogParser
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
    Class to help parse logs from the result of a single CLUSTER experiment.
    If just ran on a worker, use `WorkerLogParser`!!!
    Relies on injected sub-parsers to handle post-processing.
    This class loads the experiment artifacts and then calls these parsers to do analysis on them to add more information.
    Sub-parsers must be all injected before starting to parse data.

    NOTE: If logs are missing for any reason in the simulation, or being mis-attributed to wrong workers, it is likely from 'Span' filtering.
    `start_simulation_tracing` in `logging.rs` makes layers for each component to write to unique files tracking information in an event's Spans.
    This filtering WILL break when crossing async functions if they are not tagged with
    See here for more information: https://docs.rs/tracing/latest/tracing/span/struct.Span.html#in-asynchronous-code
    """

    def __init__(
        self,
        folder_path: str,
        input_csv: str,
        metadata_csv: str,
        benchmark_file: str = None,
        run_type: RunType = RunType.LIVE,
        run_data: Optional[Dict] = None,
    ):
        """
        run_data: metadata about the experimental run
        """
        self.source = folder_path
        self.input_csv = input_csv
        self.metadata_csv = metadata_csv
        self.target = RunTarget.CONTROLLER
        self.run_type = run_type
        self.run_data = run_data

        if not has_results(folder_path, self.input_csv):
            raise Exception(f"Missing logs in '{folder_path}'")

        self.results_csv = os.path.join(
            folder_path, trace_output("csv", self.input_csv)
        )
        self.results_json = os.path.join(
            folder_path, trace_output("json", self.input_csv)
        )
        self.controller_parser = ControllerLogParser(
            folder_path, input_csv, metadata_csv, run_type, run_data
        )
        self.worker_parsers = []
        for file in os.listdir(folder_path):
            if file.startswith("worker") and file.endswith(".log"):
                worker_log = os.path.join(folder_path, file)
                self.worker_parsers.append(
                    WorkerLogParser(
                        self.source,
                        self.input_csv,
                        self.metadata_csv,
                        benchmark_file,
                        self.run_type,
                        self.target,
                        self.run_data,
                        worker_log,
                    )
                )

    def parse_logs(
        self,
        include_errors: bool = False,
        fail_if_errors: bool = False,
    ):
        self.controller_parser.parse_logs(include_errors, fail_if_errors)
        for worker in self.worker_parsers:
            invokes_set = self.controller_parser.get_tid_set(worker)
            worker.parse_logs(include_errors, fail_if_errors, invokes_set)
        self.controller_parser.worker_logs_completed(self)

class ControllerLogParser:
    """
    Class to help parse logs from a controller.

    TODO: implement global IAT / traffic class tracking at controller level. Currently each worker is calculating the global.
    """

    def __init__(
        self,
        folder_path: str,
        input_csv: str,
        metadata_csv: str,
        run_type: RunType = RunType.LIVE,
        run_data: Optional[Dict] = None,
    ):
        self.source = folder_path
        self.input_csv = input_csv
        self.results_csv = os.path.join(
            self.source, trace_output("csv", self.input_csv)
        )
        self.results_log = os.path.join(self.source, "controller.log")
        self.metadata_csv = metadata_csv
        self.run_type = run_type
        self.run_data = run_data
        self.worker_names = set()
        self.parser_map = defaultdict(list)
        self.parsers = []
        self.load_parsers()

    def get_tid_set(self, worker) -> Optional[Set[str]]:
        """
        :param worker: a WorkerLogParser object
        """
        if "worker" in self.invokes_df.columns:
            worker_name = os.path.basename(worker.results_log).split('.')[0]
            if worker_name not in self.worker_names:
                # worker_name = worker_name[len("worker"):]
                if worker_name not in self.worker_names:
                    raise Exception(f"Unable to figure out what the worker's name was from log '{worker.results_log}' and dispatched names '{self.worker_names}")
            subset = self.invokes_df[self.invokes_df["worker"] == worker_name]
            return set(subset.index)
        return None

    def _parse_controller_log(self):
        with open(self.results_log) as f:
            for log in f.readlines():
                try:
                    log = json.loads(log)
                except Exception as e:
                    print(self.results_log)
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

        for instance in self.parsers:
            instance.before_parse()

        self._parse_controller_log()

        for instance in self.parsers:
            instance.log_completed()

    def worker_logs_completed(self, parent: LogParser):
        """
        Called after all worker logs have been parsed to concatenate their `invokes_df` frames.
        Make it easy to get a global frame of them
        """
        invoke_dfs = []
        for worker in parent.worker_parsers:
            if len(worker.invokes_df)>0:
                invoke_dfs.append(worker.invokes_df)
            else:
                print(f"WARN: worker {worker.results_log} didn't have any invocations")
        invoke_dfs = pd.concat(invoke_dfs)
        invoke_dfs["worker"] = self.invokes_df["worker"]
        self.invokes_df = invoke_dfs

    def load_parsers(self):
        for parser in ControllerLogParser.registered_parser_types:
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


@ControllerLogParser.register_parser
class LoadBalanceParser(BaseParser):
    """
    Map invocations to the worker they ran on
    """

    def __init__(self, main_parser: ControllerLogParser):
        super().__init__(main_parser)
        self.dispatches = []

    def track_load_balancing(self, log):
        tid = log["fields"]["tid"]
        worker = log["fields"]["worker"]
        self.dispatches.append( (tid, worker) )

    def new_worker(self, log):
        self.main_parser.worker_names.add(log["fields"]["worker"])

    def log_completed(self):
        df = pandas.DataFrame.from_records(self.dispatches, columns=["tid", "worker"], index="tid")
        self.main_parser.invokes_df = self.main_parser.invokes_df.join(df)

    parser_map = {
        "invoking function on worker": track_load_balancing,
        "worker successfully registered": new_worker
    }

class WorkerLogParser:
    """
    Class to help parse logs from the result of a single experiment.
    Relies on injected sub-parsers to handle post-processing.
    This class loads the experiment artifacts and then calls these parsers to do analysis on them to add more information.
    Sub-parsers must be all injected before starting to parse data.
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
        worker_log: str = None,
    ):
        """
        run_data: metadata about the experimental run
        worker_log: the specific worker log this parser should work on. Leave as `None` to allow for auto-pickup when just a single worker was run
        folder_path: Where results are stored
        input_csv: the _name_ of the trace input file, must be located in `folder_path`
        metadata_csv: the _name_ of the trace metadata file, must be located in `folder_path`
        """
        self.source = folder_path
        self.input_csv = os.path.join(self.source, input_csv)
        self.metadata_csv = os.path.join(self.source, metadata_csv)
        self.run_type = run_type
        self.target = target
        self.run_data = run_data

        if not has_results(self.source, self.input_csv):
            raise Exception(f"Missing logs in '{self.source}'")

        self.load_gen = os.path.join(self.source, "load_gen.log")

        self.results_csv = os.path.join(
            self.source, trace_output("csv", self.input_csv)
        )
        self.results_log = worker_log
        if self.results_log is None:
            for file in os.listdir(self.source):
                if file.startswith("worker") and file.endswith(".log"):
                    self.results_log = os.path.join(self.source, file)
                    break

        self.results_json = os.path.join(
            self.source, trace_output("json", self.input_csv)
        )
        self.benchmark_file = benchmark_file
        self.benchmark_data = None
        self.parser_map = defaultdict(list)
        self.parsers = []
        self.load_parsers()

    def load_parsers(self):
        for parser in WorkerLogParser.registered_parser_types:
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

    def _load_rapl(self):
        path = os.path.join(self.source, "energy-rapl.log")
        try:
            rapl_df = pd.read_csv(path)
        except FileNotFoundError:
            return
        max_rapl_uj = rapl_df["rapl_uj"].max()
        try:
            # TODO: pass your own max rapl value in somehow
            with open(
                "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj",
            ) as f:
                max_rapl_uj = int(f.read())
        except:
            pass
        rapl_data = []
        for i in range(1, len(rapl_df["rapl_uj"])):
            left = int(rapl_df["rapl_uj"][i - 1])
            right = int(rapl_df["rapl_uj"][i])
            if right < left:
                uj = right + (max_rapl_uj - left)
            else:
                uj = right - left
            rapl_data.append(uj)

        rapl_data.append(rapl_data[-1])
        rapl_df["rapl_uj_diff"] = pd.Series(rapl_data, index=rapl_df.index)

        self.rapl_df = rapl_df

    def _load_perf(self):
        """
        Load a perf log into a dataframe
        The multiple reported metrics are each put into their own column
        """
        perf_log = os.path.join(self.source, "energy-perf.log")

        try:
            f = open(perf_log, "r")
            first_line = f.readline()
            start_date = first_line[len("# started on ") :].strip(whitespace)
            # 'Mon Aug 29 15:04:17 2022'
            start_date = datetime.strptime(start_date, "%a %b %d %H:%M:%S %Y")
        except FileNotFoundError:
            return

        # https://www.man7.org/linux/man-pages/man1/perf-stat.1.html#top_of_page
        # CSV FORMAT
        cols = [
            "timestamp",
            "perf_stat",
            "unit",
            "event_name",
            "counter_runtime",
            "pct_time_counter_running",
        ]
        df = pd.read_csv(
            perf_log, skiprows=2, names=cols, usecols=[i for i in range(len(cols))]
        )

        def time_to_ns(x) -> int:
            if np.isnan(x):
                raise Exception("Got a nan value instead of a real time!")
            new_date = start_date + timedelta(seconds=x)
            new_date = datetime.combine(new_date.date(), new_date.time(), None)
            # 2022-09-14 11:54:00.793313159
            return new_date.strftime("%Y-%m-%d %H:%M:%S.%f")

        df["timestamp"] = df["timestamp"].apply(lambda x: time_to_ns(x))

        df_energy_pkg = df[df["event_name"] == "power/energy-pkg/"].copy()
        df_energy_ram = df[df["event_name"] == "power/energy-ram/"].copy()
        df_instructions = df[df["event_name"] == "inst_retired.any"].copy()

        df_energy_pkg.index = pd.DatetimeIndex(df_energy_pkg["timestamp"])
        df_energy_pkg.rename(columns={"perf_stat": "energy_pkg"}, inplace=True)

        df_energy_ram.index = pd.DatetimeIndex(df_energy_ram["timestamp"])
        df_energy_ram.rename(columns={"perf_stat": "energy_ram"}, inplace=True)

        df_instructions.index = pd.DatetimeIndex(df_instructions["timestamp"])
        df_instructions.rename(
            columns={"perf_stat": "retired_instructions"}, inplace=True
        )

        df = df_energy_pkg.join(df_energy_ram["energy_ram"])
        df = df.join(df_instructions["retired_instructions"])
        self.perf_df = df

    def _load_ipmi(self):
        try:
            cols = ["timestamp", "ipmi"]
            ipmi_log = os.path.join(self.source, "energy-ipmi.log")
            df = pd.read_csv(ipmi_log, skiprows=1, names=cols)
            df["timestamp"] = df["timestamp"].apply(timestamp_to_pddate)
            self.ipmi_df = df
        except FileNotFoundError:
            return

    def _parse_worker_log(self):
        with open(self.results_log) as f:
            for log in f.readlines():
                try:
                    log = json.loads(log)
                except Exception as e:
                    print(self.results_log)
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
        invoke_tid_subset: Optional[Set[str]] = None,
    ):
        """
        :param include_errors: Include errors in completed parsing
        :param fail_if_errors: Raise an error if any invocation failed
        :param invoke_tid_subset: a set of TIDs to restrict this worker to parsing, because they were sent here by the controller
        """
        self.metadata_df = pd.read_csv(self.metadata_csv, index_col="func_name")
        self.invokes_df = pd.read_csv(self.results_csv, index_col="tid")
        if fail_if_errors:
            if not self.invokes_df["success"].all():
                raise Exception(
                    f"Experiment had invocation failures '{self.results_log}'"
                )
        if not include_errors:
            self.invokes_df = self.invokes_df[self.invokes_df["success"]]
        if invoke_tid_subset is not None:
            self.invokes_df = self.invokes_df[self.invokes_df.index.isin(invoke_tid_subset)]
        self._load_rapl()
        self._load_ipmi()
        self._load_perf()

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


@WorkerLogParser.register_parser
class BenchmarkNameParser(BaseParser):
    """
    Map functions to the code names in the benchmark file, if it exists.
    """

    def __init__(self, main_parser: WorkerLogParser):
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
                if len(found) == len(self.main_parser.metadata_df):
                    break
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


@WorkerLogParser.register_parser
class MetadataTrafficClassesParser(BaseParser):
    """
    Compute function traffic classes from IATs.

    TODO: when in cluster, compute only IAT & traffic class of invocations that hit this worker. Currently loads global
    """

    def __init__(self, main_parser: WorkerLogParser):
        super().__init__(main_parser)

    def before_parse(self):
        last = {}
        iats = defaultdict(list)
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


@WorkerLogParser.register_parser
class QueuingParser(BaseParser):
    """
    Compute per-invocation queuing times
    """

    def __init__(self, main_parser: WorkerLogParser):
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


@WorkerLogParser.register_parser
class FullJsonMergeParser(BaseParser):
    """
    Merge data about each invocation (compute, start time, finish time, container state)
    from full invocation json data file and add to invocations.
    """

    def __init__(self, main_parser: WorkerLogParser):
        super().__init__(main_parser)

    def get_start_and_finish(self, invoke):
        start = pd.to_datetime(invoke["invoke_start"])
        finish = start + pd.Timedelta(
            invoke["client_latency_us"] / 1_000_000.00, unit="s"
        )
        return start, finish

    def get_compute(self, invoke, tid):
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

    def get_container_state(self, invoke, tid):
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
            compute = self.get_compute(invoke, tid)
            state = self.get_container_state(invoke, tid)
            start, finish = self.get_start_and_finish(invoke)
            data.append((tid, compute, state, start, finish))
        full_df = pd.DataFrame.from_records(
            data,
            columns=["tid", "compute", "state", "invoke_sent", "invoke_return"],
            index="tid",
        )
        self.main_parser.invokes_df = self.main_parser.invokes_df.join(full_df)


@WorkerLogParser.register_parser
class EstTimeParser(BaseParser):
    def __init__(self, main_parser: WorkerLogParser):
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


@WorkerLogParser.register_parser
class StatusParser(BaseParser):
    def __init__(self, main_parser: WorkerLogParser):
        super().__init__(main_parser)
        self.queue_data = []
        self.cpu_data = []
        self.container_data = []
        self.gpu_data = []

    def parse_queue(self, log):
        t = pd.to_datetime(log["timestamp"])
        num_running = int(log["fields"]["num_running_funcs"])
        queue_info = json.loads(log["fields"]["queue_info"])["queues"]
        self.queue_data.append((
            t,
            num_running,
            get_from_dict(queue_info, ["GPU", "len"], 0),
            get_from_dict(queue_info, ["GPU", "load"], 0.0),
            get_from_dict(queue_info, ["GPU", "load_avg"], 0.0),
            get_from_dict(queue_info, ["GPU", "tput"], 0.0),
            get_from_dict(queue_info, ["CPU", "len"], 0),
            get_from_dict(queue_info, ["CPU", "load"], 0.0),
            get_from_dict(queue_info, ["CPU", "load_avg"], 0.0),
            get_from_dict(queue_info, ["CPU", "tput"], 0.0),
        ))

    def parse_cpu(self, log):
        t = pd.to_datetime(log["timestamp"])
        cpu_util = json.loads(log["fields"]["cpu_util"])
        if cpu_util["cpu_us"] is None:
            return
        self.cpu_data.append((
            t,
            cpu_util["cpu_us"]+cpu_util["cpu_sy"],
            cpu_util["cpu_us"],
            cpu_util["cpu_sy"],
            cpu_util["cpu_id"],
            cpu_util["cpu_wa"],
            cpu_util["load_avg_1minute"],
        ))

    def parse_container_man(self, log):
        t = pd.to_datetime(log["timestamp"])
        self.container_data.append((
            t,
            log["fields"]["used_mem"],
            log["fields"]["total_mem"],
            (float(log["fields"]["used_mem"]) / float(log["fields"]["total_mem"]))*100.0,
            log["fields"]["num_containers"],
        ))

    def parse_gpu(self, log):
        t = pd.to_datetime(log["timestamp"])
        gpu_util_log = json.loads(log["fields"]["gpu_util"])
        gpu_util = 0
        gpu_mem = 0
        gpu_instant = 0
        if len(gpu_util_log) > 0:
            gpu_util = sum(
                [gpu["utilization_gpu"] for gpu in gpu_util_log]
            ) / len(gpu_util_log)
            gpu_instant = sum(
                [gpu["utilization_gpu"] for gpu in gpu_util_log]
            ) / len(gpu_util_log)
            gpu_mem = sum(
                [
                    float(gpu["memory_used"]) / float(gpu["memory_total"])
                    for gpu in gpu_util_log
                ]
            ) / len(gpu_util_log)
        self.gpu_data.append((t, gpu_util, gpu_mem, gpu_instant))

    def log_completed(self):
        cpu_df = pd.DataFrame.from_records(
            self.cpu_data,
           columns=[
               "timestamp",
               "cpu_util",
               "cpu_us",
               "cpu_sy",
               "cpu_id",
               "cpu_wa",
               "load_avg_1minute",
           ])

        queue_df = pd.DataFrame.from_records(
            self.queue_data,
            columns=[
                "timestamp",
                "num_running",
                "gpu_len",
                "gpu_load",
                "gpu_load_avg",
                "gpu_tput",
                "cpu_len",
                "cpu_load",
                "cpu_load_avg",
                "cpu_tput",
            ])

        container_df = pd.DataFrame.from_records(
            self.container_data,
            columns=[
                "timestamp",
                "cpu_used_mem",
                "cpu_total_mem",
                "memory_util_pct",
                "num_containers",
            ])
        dfs = [queue_df,cpu_df,container_df]

        if len(self.gpu_data) > 0:
            gpu_df = pd.DataFrame.from_records(
                self.gpu_data,
                columns=[
                    "timestamp",
                    "gpu_util",
                    "gpu_mem",
                    "gpu_instant",
                ])
            dfs.append(gpu_df)
        status_df = pd.concat(dfs)
        status_df.index = status_df["timestamp"]
        # fix up empties because all our status data isn't coming in one drop, but "close enough"
        # all are on the same log schedule
        status_df.ffill(inplace=True)
        status_df.bfill(inplace=True)
        status_df["norm_time"] = status_df["timestamp"] - status_df["timestamp"].min()
        self.main_parser.status_df = status_df

    parser_map = {
        "current queue info": parse_queue,
        "CPU utilization": parse_cpu,
        "Container manager info": parse_container_man,
        "GPU status": parse_gpu,
    }


@WorkerLogParser.register_parser
class StartFinishParser(BaseParser):
    def __init__(self, main_parser: WorkerLogParser):
        super().__init__(main_parser)
        self.invoke_start = {}
        self.invoke_end = {}

    def invoke_started(self, log):
        ts = pd.to_datetime(log["timestamp"])
        tid = log["fields"]["tid"]
        self.invoke_start[tid] = ts

    def invoke_finished(self, log):
        ts = pd.to_datetime(log["timestamp"])
        tid = log["fields"]["tid"]
        self.invoke_end[tid] = ts

    def log_completed(self):
        data = []
        for tid in self.invoke_start.keys():
            data.append((tid, self.invoke_start[tid], self.invoke_end[tid]))
        df = pd.DataFrame.from_records(
            data, columns=["tid", "start", "end"], index="tid"
        )
        self.main_parser.invokes_df = self.main_parser.invokes_df.join(df)

    parser_map = {
        "Item starting to execute": invoke_started,
        "Invocation complete": invoke_finished,
    }


@WorkerLogParser.register_parser
class BatchingParser(BaseParser):
    def __init__(self, main_parser: WorkerLogParser):
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


@WorkerLogParser.register_parser
class CacheInsertionsParser(BaseParser):
    def __init__(self, main_parser: WorkerLogParser):
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


@WorkerLogParser.register_parser
class OverheadParser(BaseParser):
    def __init__(self, main_parser: WorkerLogParser):
        super().__init__(main_parser)

    def compute_overheads(self):
        data = []
        for index, row in self.main_parser.metadata_df.iterrows():
            sub_df = self.main_parser.invokes_df[
                self.main_parser.invokes_df["function_name"] == index
            ]
            min_exec = sub_df["code_duration_sec"].min()
            cold_exec_time = warm_exec_time = warm_e2e_time = 1000
            supported_computes = "CPU"
            if "compute" in row.index:
                supported_computes = row.compute
            for compute in supported_computes.split("|"):
                # normalizing will happen by the best time
                cold_comp_exec_time = np.mean(
                    get_bench_data(
                        row.benchmark_name,
                        compute,
                        "cold_results_sec",
                        self.main_parser.benchmark_data,
                    )
                )
                cold_exec_time = min(cold_exec_time, cold_comp_exec_time)
                warm_gpu_exec_time = np.mean(
                    get_bench_data(
                        row.benchmark_name,
                        compute,
                        "warm_results_sec",
                        self.main_parser.benchmark_data,
                    )
                )
                warm_exec_time = min(warm_exec_time, warm_gpu_exec_time)
                comp_warm_e2e_time_sec = (
                    np.mean(
                        get_bench_data(
                            row.benchmark_name,
                            compute,
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
) -> WorkerLogParser:
    parser = WorkerLogParser(
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
) -> List[WorkerLogParser]:
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
    NOTE: this assumes that all experiments to be loaded use the same `input_csv` and `metadata_csv` file names
    TODO: check up on thus ^^

    TODO: allow loading bulk cluster data
    """
    csv_files = _recurse(start_folder, folder_structure, input_csv)
    if filter_fn is not None:
        csv_files = filter_fn(csv_files)
    if len(csv_files) == 0:
        raise NoResultsException(f"Could not get any results in '{start_folder}'", None)

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


class NoResultsException(Exception):
    def __init__(self, message, errors):
        super().__init__(message, errors)
