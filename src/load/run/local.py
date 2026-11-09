"""Direct, single-host Iluvatar experiment execution.

This module contains the lifecycle that used to live in ``local_exp.sh`` so it
can be reused by experiment scripts as well as by the command-line wrapper.
It intentionally does not use Ansible; the existing :mod:`load.run.run_trace`
module remains the implementation for Ansible-backed runs.
"""

from __future__ import annotations

import csv
import json
import math
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional


class ExperimentError(RuntimeError):
    """An experiment could not be prepared or completed."""


def parse_value(value: str) -> Any:
    """Parse a command-line value as JSON, falling back to a string."""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def set_json_path(document: dict[str, Any], dotted_path: str, value: Any) -> None:
    """Set an object value addressed by a dot-separated path."""
    parts = dotted_path.split(".")
    if not dotted_path or any(not part for part in parts):
        raise ExperimentError(f"invalid empty JSON path: {dotted_path!r}")
    current: dict[str, Any] = document
    for part in parts[:-1]:
        child = current.get(part)
        if child is None:
            child = {}
            current[part] = child
        if not isinstance(child, dict):
            raise ExperimentError(
                f"cannot set {dotted_path!r}: {part!r} is not an object"
            )
        current = child
    current[parts[-1]] = value


def load_domains(path: Path) -> list[dict[str, Any]]:
    with path.open() as source:
        value = json.load(source)
    if isinstance(value, dict):
        value = value.get("groups")
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ExperimentError(
            f"domains file must contain a JSON array or an object with a groups array: {path}"
        )
    return value


def prepare_worker_config(
    source: Path,
    destination: Path,
    overrides: Iterable[str] = (),
    domains_file: Optional[Path] = None,
    timeslice_ms: Optional[int] = None,
    domain_priority: Optional[str] = None,
    domain_fifo: Optional[int] = None,
) -> dict[str, Any]:
    """Copy and patch a worker JSON config, returning the patched object."""
    with source.open() as config_file:
        config = json.load(config_file)

    for override in overrides:
        if "=" not in override:
            raise ExperimentError(
                f"invalid --set value (expected PATH=VALUE): {override}"
            )
        path, raw_value = override.split("=", 1)
        set_json_path(config, path, parse_value(raw_value))

    if domains_file is not None:
        set_json_path(
            config,
            "fineloadbalancing.preallocated_groups.groups",
            load_domains(domains_file),
        )

    try:
        groups = config["fineloadbalancing"]["preallocated_groups"]["groups"]
    except (KeyError, TypeError):
        groups = None
    if any(value is not None for value in (timeslice_ms, domain_priority, domain_fifo)):
        if not isinstance(groups, list):
            raise ExperimentError(
                "domain-wide options require fineloadbalancing.preallocated_groups.groups"
            )
        for group in groups:
            if timeslice_ms is not None:
                group["ts"] = timeslice_ms
            if domain_priority is not None:
                group["prio"] = domain_priority
            if domain_fifo is not None:
                group["fifo"] = domain_fifo

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=destination.parent, prefix=f".{destination.name}.", delete=False
        ) as output:
            temporary = Path(output.name)
            json.dump(config, output, indent=2)
            output.write("\n")
        temporary.replace(destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return config


def find_trace_files(
    trace_dir: Path,
    input_csv: Optional[Path] = None,
    metadata_csv: Optional[Path] = None,
) -> tuple[Path, Path]:
    """Resolve explicit trace files or the two supported directory layouts."""
    if input_csv is None:
        candidates = (trace_dir / "funcs-in.csv", trace_dir / "in.csv")
        input_csv = next((path for path in candidates if path.is_file()), candidates[-1])
    if metadata_csv is None:
        candidates = (trace_dir / "funcs-meta.csv", trace_dir / "meta.csv")
        metadata_csv = next((path for path in candidates if path.is_file()), candidates[-1])
    return input_csv, metadata_csv


def resolve_binary(
    ilu_home: Path, build: str, name: str, explicit: Optional[Path] = None
) -> Path:
    if explicit is not None:
        return explicit
    candidates = (
        ilu_home / "target" / build / name,
        ilu_home / "target" / "x86_64-unknown-linux-gnu" / build / name,
    )
    return next((path for path in candidates if path.is_file()), candidates[0])


@dataclass
class DirectRunConfig:
    ilu_home: Path
    worker_config: Path
    trace_input: Path
    trace_metadata: Path
    function_data: Optional[Path]
    results_dir: Path
    build: str = "debug"
    worker_bin: Optional[Path] = None
    load_gen_bin: Optional[Path] = None
    worker_cli_bin: Optional[Path] = None
    host: str = "127.0.0.1"
    port: Optional[int] = None
    load_type: str = "functions"
    prewarms: int = 0
    max_prewarms: int = 5000
    start_timeout: int = 60
    postprocess_script: Optional[Path] = None
    clean_after: bool = True
    overrides: list[str] = field(default_factory=list)
    domains_file: Optional[Path] = None
    timeslice_ms: Optional[int] = None
    domain_priority: Optional[str] = None
    domain_fifo: Optional[int] = None

    def resolve(self) -> "DirectRunConfig":
        self.ilu_home = self.ilu_home.resolve()
        self.worker_config = self.worker_config.resolve()
        self.trace_input = self.trace_input.resolve()
        self.trace_metadata = self.trace_metadata.resolve()
        self.results_dir = self.results_dir.resolve()
        if self.function_data is not None:
            self.function_data = self.function_data.resolve()
        if self.domains_file is not None:
            self.domains_file = self.domains_file.resolve()
        if self.postprocess_script is not None:
            self.postprocess_script = self.postprocess_script.resolve()
        self.worker_bin = resolve_binary(
            self.ilu_home, self.build, "iluvatar_worker", self.worker_bin
        ).resolve()
        self.load_gen_bin = resolve_binary(
            self.ilu_home, self.build, "iluvatar_load_gen", self.load_gen_bin
        ).resolve()
        self.worker_cli_bin = resolve_binary(
            self.ilu_home, self.build, "iluvatar_worker_cli", self.worker_cli_bin
        ).resolve()
        return self

    def validate(self) -> None:
        required = {
            "worker binary": self.worker_bin,
            "load-generator binary": self.load_gen_bin,
            "worker CLI binary": self.worker_cli_bin,
            "worker configuration": self.worker_config,
            "trace input": self.trace_input,
            "trace metadata": self.trace_metadata,
        }
        if self.load_type == "functions":
            required["function benchmark data"] = self.function_data
        elif self.load_type != "lookbusy":
            raise ExperimentError("load type must be 'functions' or 'lookbusy'")
        if self.domains_file is not None:
            required["domains file"] = self.domains_file
        if self.postprocess_script is not None:
            required["post-processing script"] = self.postprocess_script
        missing = [f"{label}: {path}" for label, path in required.items() if path is None or not path.is_file()]
        if missing:
            raise ExperimentError("missing required files:\n  " + "\n  ".join(missing))


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summarize_results(source: Path, summary_path: Path, failures_path: Path) -> None:
    """Write the small, dependency-free summary previously embedded in Bash."""
    with source.open(newline="") as source_file:
        rows = list(csv.DictReader(source_file))

    def succeeded(row: dict[str, str]) -> bool:
        return row.get("success", "").lower() == "true"

    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row.get("function_name", "unknown"), []).append(row)

    columns = [
        "function_name", "total", "successes", "failures", "cold_successes",
        "warm_successes", "mean_e2e_us", "p50_e2e_us", "p95_e2e_us",
        "p99_e2e_us", "mean_worker_duration_us",
    ]
    with summary_path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        for function_name, function_rows in sorted(grouped.items()):
            successful = [row for row in function_rows if succeeded(row)]
            e2e = [int(row["e2e_duration_us"]) for row in successful]
            worker = [int(row["worker_duration_us"]) for row in successful]
            writer.writerow({
                "function_name": function_name,
                "total": len(function_rows),
                "successes": len(successful),
                "failures": len(function_rows) - len(successful),
                "cold_successes": sum(row.get("was_cold", "").lower() == "true" for row in successful),
                "warm_successes": sum(row.get("was_cold", "").lower() != "true" for row in successful),
                "mean_e2e_us": round(statistics.fmean(e2e), 2) if e2e else 0,
                "p50_e2e_us": _percentile(e2e, 0.50),
                "p95_e2e_us": _percentile(e2e, 0.95),
                "p99_e2e_us": _percentile(e2e, 0.99),
                "mean_worker_duration_us": round(statistics.fmean(worker), 2) if worker else 0,
            })

    with failures_path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(row for row in rows if not succeeded(row))


class DirectExperiment:
    """Own the worker/load-generator lifecycle for one local experiment."""

    def __init__(self, config: DirectRunConfig):
        self.config = config.resolve()
        self.worker: Optional[subprocess.Popen[str]] = None
        self.worker_stopped = True
        self.generated_config = self.config.results_dir / "worker_config.json"
        self.orchestration_log = self.config.results_dir / "orchestration.log"

    def message(self, text: str) -> None:
        rendered = f"\n#####################################\n# {text}\n"
        print(rendered, end="")
        with self.orchestration_log.open("a") as log:
            log.write(rendered)

    def command(self, args: list[object], *, check: bool = True, log=None):
        rendered = [str(arg) for arg in args]
        return subprocess.run(rendered, check=check, stdout=log, stderr=log, text=True)

    def clean_worker(self) -> None:
        self.message("Cleaning worker state")
        with self.orchestration_log.open("a") as log:
            self.command(
                ["sudo", self.config.worker_bin, "-c", self.generated_config, "clean"],
                log=log,
            )

    def stop_worker(self) -> None:
        if self.worker_stopped or self.worker is None or self.worker.poll() is not None:
            self.worker_stopped = True
            return
        self.message("Stopping worker")
        self.command(["sudo", "kill", "-INT", self.worker.pid], check=False)
        try:
            self.worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.command(["sudo", "kill", "-TERM", self.worker.pid], check=False)
            try:
                self.worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.command(["sudo", "kill", "-KILL", self.worker.pid], check=False)
                self.worker.wait()
        self.worker_stopped = True

    def collect_worker_logs(self, worker_log_dir: Optional[str]) -> None:
        if not worker_log_dir:
            return
        source = Path(worker_log_dir)
        if self.command(["sudo", "test", "-d", source], check=False).returncode != 0:
            return
        destination = self.config.results_dir / "worker_logs"
        destination.mkdir(exist_ok=True)
        self.command(["sudo", "cp", "-a", f"{source}/.", f"{destination}/"])
        self.command(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", destination])

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + self.config.start_timeout
        while time.monotonic() < deadline:
            if self.worker is None or self.worker.poll() is not None:
                raise ExperimentError(
                    f"worker exited before becoming ready; see {self.config.results_dir / 'worker_stdout.log'}"
                )
            ready = self.command(
                [
                    self.config.worker_cli_bin,
                    "--host", self.config.host,
                    "--port", self.config.port,
                    "ping",
                ],
                check=False,
                log=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                return
            time.sleep(1)
        raise ExperimentError(
            f"worker did not become ready within {self.config.start_timeout} seconds"
        )

    def _run_load(self) -> int:
        load_command: list[object] = [
            self.config.load_gen_bin, "trace",
            "--input-csv", self.config.trace_input,
            "--metadata-csv", self.config.trace_metadata,
            "--port", self.config.port,
            "--host", self.config.host,
            "--out-folder", self.config.results_dir,
            "--target", "worker",
            "--setup", "live",
            "--load-type", self.config.load_type,
            "--prewarms", self.config.prewarms,
            "--max-prewarms", self.config.max_prewarms,
        ]
        if self.config.load_type == "functions":
            load_command.extend(["--function-data", self.config.function_data])
        with (self.config.results_dir / "load_gen.log").open("w") as log:
            process = subprocess.Popen(
                [str(item) for item in load_command],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert process.stdout is not None
            for line in process.stdout:
                sys.stdout.write(line)
                log.write(line)
            return process.wait()

    def _postprocess(self) -> None:
        self.message("Post-processing results")
        if self.config.postprocess_script is not None:
            self.command([self.config.postprocess_script, self.config.results_dir])
            return
        outputs = sorted(
            path for path in self.config.results_dir.glob("output-*.csv")
            if "failed" not in path.name
        )
        if not outputs:
            raise ExperimentError(
                f"load generator produced no output CSV in {self.config.results_dir}"
            )
        summarize_results(
            outputs[0],
            self.config.results_dir / "summary.csv",
            self.config.results_dir / "failed_invocations.csv",
        )

    def run(self) -> int:
        self.config.validate()
        if subprocess.run(
            ["pgrep", "-f", "[i]luvatar_worker"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode == 0:
            raise ExperimentError("an iluvatar_worker process is already running")

        self.config.results_dir.mkdir(parents=True, exist_ok=True)
        config = prepare_worker_config(
            self.config.worker_config,
            self.generated_config,
            self.config.overrides,
            self.config.domains_file,
            self.config.timeslice_ms,
            self.config.domain_priority,
            self.config.domain_fifo,
        )
        if self.config.port is None:
            try:
                self.config.port = int(config["port"])
            except (KeyError, TypeError, ValueError) as error:
                raise ExperimentError("worker config does not contain a valid port") from error

        shutil.copy2(self.config.trace_input, self.config.results_dir / "trace_input.csv")
        shutil.copy2(self.config.trace_metadata, self.config.results_dir / "trace_metadata.csv")
        if self.config.function_data is not None:
            shutil.copy2(self.config.function_data, self.config.results_dir / "function_data.json")

        worker_log_dir = config.get("logging", {}).get("directory")
        self.command(["sudo", "-v"])
        self.clean_worker()
        load_status = 1
        logs_collected = False
        worker_stdout = None
        try:
            self.message("Starting worker")
            worker_stdout = (self.config.results_dir / "worker_stdout.log").open("w")
            self.worker = subprocess.Popen(
                ["sudo", str(self.config.worker_bin), "-c", str(self.generated_config)],
                stdout=worker_stdout,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.worker_stopped = False
            self._wait_until_ready()
            self.message("Running local trace")
            (self.config.results_dir / "trace_start_time_including_prewarm.txt").write_text(
                f"{int(time.time())}\n"
            )
            load_status = self._run_load()
            (self.config.results_dir / "trace_end_time.txt").write_text(
                f"{int(time.time())}\n"
            )
            self.stop_worker()
            worker_stdout.close()
            worker_stdout = None
            self.collect_worker_logs(worker_log_dir)
            logs_collected = True
            if self.config.clean_after:
                self.clean_worker()
                self.config.clean_after = False
            self._postprocess()
            self.message(f"Experiment complete: {self.config.results_dir}")
            return load_status
        finally:
            self.stop_worker()
            if worker_stdout is not None:
                worker_stdout.close()
            if not logs_collected:
                self.collect_worker_logs(worker_log_dir)
            if self.config.clean_after:
                try:
                    self.clean_worker()
                except Exception as error:  # cleanup must not mask the primary failure
                    print(f"warning: worker cleanup failed: {error}", file=sys.stderr)


def run_direct(config: DirectRunConfig) -> int:
    """Run one direct local experiment."""
    return DirectExperiment(config).run()


@dataclass
class AnsibleRunConfig:
    """Inputs for one local-host run managed by the existing Ansible runner."""

    ilu_home: Path
    worker_config: Path
    trace_input: Path
    trace_metadata: Path
    benchmark_file: Path
    results_dir: Path
    build: str = "debug"
    prewarms: int = 0
    force: bool = False


def _merge_config(defaults: Any, requested: Any) -> Any:
    """Overlay requested config values, retaining defaults for null/missing data."""
    if requested is None:
        return defaults
    if isinstance(defaults, dict) and isinstance(requested, dict):
        return {
            key: _merge_config(defaults.get(key), value)
            for key, value in requested.items()
        } | {
            key: value for key, value in defaults.items() if key not in requested
        }
    return requested


def _default_network_interface() -> str:
    route = subprocess.run(
        ["ip", "route", "show", "default"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    try:
        return route[route.index("dev") + 1]
    except (ValueError, IndexError) as error:
        raise ExperimentError("could not determine the default network interface") from error


def run_ansible_local(config: AnsibleRunConfig) -> None:
    """Run a local trace through ``run_live`` using an explicit worker config."""
    if shutil.which("ansible-playbook") is None:
        raise ExperimentError(
            "ansible-playbook is not installed; install "
            f"{config.ilu_home.resolve() / 'ansible/reqs.txt'}"
        )
    paths = {
        "worker configuration": config.worker_config.resolve(),
        "trace input": config.trace_input.resolve(),
        "trace metadata": config.trace_metadata.resolve(),
        "function benchmark data": config.benchmark_file.resolve(),
    }
    ilu_home = config.ilu_home.resolve()
    ansible_dir = ilu_home / "ansible"
    binary_dir = ilu_home / "target" / config.build
    default_config = (
        ilu_home
        / "target"
        / "x86_64-unknown-linux-gnu"
        / config.build
        / "worker.json"
    )
    worker_bin = binary_dir / "iluvatar_worker"
    load_gen_bin = binary_dir / "iluvatar_load_gen"
    hosts = ansible_dir / "environments" / "local" / "hosts.ini"
    addresses = ansible_dir / "group_vars" / "host_addresses.yml"
    paths.update({
        "worker binary": worker_bin,
        "load-generator binary": load_gen_bin,
        "Ansible inventory": hosts,
        "Ansible host addresses": addresses,
        "build worker configuration": default_config,
    })
    missing = [f"{label}: {path}" for label, path in paths.items() if not path.is_file()]
    if missing:
        raise ExperimentError("missing required files:\n  " + "\n  ".join(missing))

    with default_config.open() as source:
        worker_defaults = json.load(source)
    with paths["worker configuration"].open() as source:
        requested_config = json.load(source)
    worker_config = _merge_config(worker_defaults, requested_config)

    results_dir = config.results_dir.resolve()
    results_dir.mkdir(parents=True, exist_ok=True)
    merged_config = results_dir / "worker_config.json"
    merged_config.write_text(json.dumps(worker_config, indent=2) + "\n")
    shutil.copy2(paths["worker configuration"], results_dir / "requested_worker_config.json")
    try:
        port = int(worker_config["port"])
        worker_log_dir = Path(worker_config["logging"]["directory"])
    except (KeyError, TypeError, ValueError) as error:
        raise ExperimentError(
            "worker config must contain port and logging.directory"
        ) from error
    worker_log_dir.mkdir(parents=True, exist_ok=True)
    hardware_interface = _default_network_interface()

    from .ansible import RunTarget
    from .multiproc import LOCALHOST_Q
    from .run_trace import BuildTarget, run_live

    try:
        build_level = BuildTarget(config.build)
    except ValueError as error:
        raise ExperimentError(f"unsupported build profile: {config.build}") from error

    run_live(
        str(paths["trace input"]),
        str(paths["trace metadata"]),
        str(results_dir),
        LOCALHOST_Q,
        ilu_home=str(ilu_home),
        ansible_dir=str(ansible_dir),
        ansible_hosts_addrs=f"@{addresses}",
        ansible_args=[
            "-e", f"worker_bin_src={binary_dir}",
            "-e", f"worker_config_src={merged_config}",
            "-e", f"worker_port={port}",
            "-e", f"hardware_interface={hardware_interface}",
        ],
        benchmark_file=str(paths["function benchmark data"]),
        build_level=build_level,
        force=config.force,
        host="127.0.0.1",
        load_gen_bin=str(load_gen_bin),
        prewarm=config.prewarms,
        target=RunTarget.WORKER,
        worker_config_file=str(merged_config),
        worker_log_dir=str(worker_log_dir),
        worker_port=port,
    )
