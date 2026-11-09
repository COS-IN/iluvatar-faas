"""Command-line entry point for repeatable Iluvatar experiments."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from .ansible import RunTarget
from .local import DirectRunConfig, ExperimentError, find_trace_files, parse_value, run_direct
from .multiproc import make_host_queue
from .run_trace import BuildTarget, run_live


MODULE_PATH = Path(__file__).resolve()
REPO_ROOT = MODULE_PATH.parents[3]
DEFAULT_ILU_HOME = REPO_ROOT / "src" / "Ilúvatar"
DEFAULT_WORKSPACE = REPO_ROOT.parent


def existing_default(*paths: Path) -> Path:
    return next((path for path in paths if path.exists()), paths[0])


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backend", choices=("direct", "ansible"), default="direct")
    parser.add_argument("--ilu-home", type=Path, default=Path(os.getenv("ILU_HOME", DEFAULT_ILU_HOME)))
    parser.add_argument("--build", choices=tuple(item.value for item in BuildTarget), default="debug")
    parser.add_argument("--trace-dir", type=Path)
    parser.add_argument("--input-csv", type=Path)
    parser.add_argument("--metadata-csv", type=Path)
    parser.add_argument("--function-data", type=Path)
    parser.add_argument("--results-dir", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--load-type", choices=("functions", "lookbusy"), default="functions")
    parser.add_argument("--prewarms", type=int, default=0)
    parser.add_argument("--max-prewarms", type=int, default=5000)
    parser.add_argument("--force", action="store_true", help="rerun even when complete results exist (Ansible backend)")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Run one Iluvatar trace locally or through the existing Ansible lifecycle."
    )
    add_common_arguments(result)
    result.add_argument("--worker-config", type=Path)
    result.add_argument("--worker-bin", type=Path)
    result.add_argument("--load-gen-bin", type=Path)
    result.add_argument("--worker-cli-bin", type=Path)
    result.add_argument("--start-timeout", type=int, default=60)
    result.add_argument("--postprocess-script", type=Path)
    result.add_argument("--no-clean-after", action="store_true")
    result.add_argument("--cpu-policy")
    result.add_argument("--cpu-queue-policy")
    result.add_argument("--cpu-count", type=int)
    result.add_argument("--concurrent-creation", type=int)
    result.add_argument("--concurrency-limit", type=int)
    result.add_argument("--domains-file", type=Path)
    result.add_argument("--timeslice-ms", type=int)
    result.add_argument("--domain-priority")
    result.add_argument("--domain-fifo", type=int)
    result.add_argument("--guardrails-tightness", type=int)
    result.add_argument("--guardrails-log-base-c", type=int)
    result.add_argument("--iat-bucket-size", type=int)
    result.add_argument(
        "--set", dest="overrides", action="append", default=[], metavar="PATH=VALUE",
        help="override any worker JSON path; may be repeated",
    )

    ansible = result.add_argument_group("Ansible backend")
    ansible.add_argument("--ansible-dir", type=Path)
    ansible.add_argument("--ansible-environment", default="local")
    ansible.add_argument("--host-addresses", type=Path)
    ansible.add_argument("--private-key", type=Path)
    ansible.add_argument(
        "--ansible-args", default="",
        help="additional ansible-playbook arguments, parsed with shell quoting",
    )
    ansible.add_argument(
        "--option", action="append", default=[], metavar="NAME=VALUE",
        help="override a load.run.run_trace option for the Ansible backend",
    )
    ansible.add_argument("--target", choices=("worker", "controller"), default="worker")
    ansible.add_argument("--worker-log-dir", type=Path)
    ansible.add_argument("--snapshotter")
    return result


def apply_named_overrides(args: argparse.Namespace) -> list[str]:
    overrides = list(args.overrides)
    mappings = (
        ("cpu_policy", "fineloadbalancing.dispatchpolicy"),
        ("cpu_queue_policy", "invocation.queue_policies.CPU"),
        ("cpu_count", "container_resources.cpu_resource.count"),
        ("concurrent_creation", "container_resources.concurrent_creation"),
        ("concurrency_limit", "fineloadbalancing.concur_limit"),
        ("guardrails_tightness", "fineloadbalancing.guardrails_tightness"),
        ("guardrails_log_base_c", "fineloadbalancing.guardrails_log_base_c"),
        ("iat_bucket_size", "fineloadbalancing.iat_bucket_size"),
    )
    for attribute, path in mappings:
        value = getattr(args, attribute)
        if value is not None:
            overrides.append(f"{path}={json.dumps(value)}")
    return overrides


def resolve_inputs(args: argparse.Namespace) -> None:
    args.ilu_home = args.ilu_home.resolve()
    args.trace_dir = (args.trace_dir or args.ilu_home / "docs/examples/basic-trace").resolve()
    args.input_csv, args.metadata_csv = find_trace_files(
        args.trace_dir, args.input_csv, args.metadata_csv
    )
    if args.function_data is None:
        args.function_data = args.ilu_home / "docs/examples/benchmark/worker_function_benchmarks.json"
    if args.results_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        args.results_dir = DEFAULT_WORKSPACE / "results" / f"local-{timestamp}"


def direct_config(args: argparse.Namespace) -> DirectRunConfig:
    worker_config = args.worker_config or existing_default(
        DEFAULT_WORKSPACE / "configurations/v-021.json",
        DEFAULT_WORKSPACE / "configurations/local-linpack-syscall.json",
        args.ilu_home / "target" / args.build / "worker.json",
        args.ilu_home / "target/x86_64-unknown-linux-gnu" / args.build / "worker.json",
    )
    return DirectRunConfig(
        ilu_home=args.ilu_home,
        build=args.build,
        worker_config=worker_config,
        worker_bin=args.worker_bin,
        load_gen_bin=args.load_gen_bin,
        worker_cli_bin=args.worker_cli_bin,
        trace_input=args.input_csv,
        trace_metadata=args.metadata_csv,
        function_data=args.function_data if args.load_type == "functions" else None,
        results_dir=args.results_dir,
        host=args.host,
        port=args.port,
        load_type=args.load_type,
        prewarms=args.prewarms,
        max_prewarms=args.max_prewarms,
        start_timeout=args.start_timeout,
        postprocess_script=args.postprocess_script,
        clean_after=not args.no_clean_after,
        overrides=apply_named_overrides(args),
        domains_file=args.domains_file,
        timeslice_ms=args.timeslice_ms,
        domain_priority=args.domain_priority,
        domain_fifo=args.domain_fifo,
    )


def ansible_options(args: argparse.Namespace) -> dict[str, Any]:
    ansible_dir = (args.ansible_dir or args.ilu_home / "ansible").resolve()
    host_addresses = (args.host_addresses or ansible_dir / "group_vars/host_addresses.yml").resolve()
    options: dict[str, Any] = {
        "ilu_home": str(args.ilu_home),
        "ansible_dir": str(ansible_dir),
        "ansible_hosts_addrs": f"@{host_addresses}",
        "benchmark_file": str(args.function_data),
        "build_level": BuildTarget(args.build),
        "target": RunTarget(args.target),
        "load_type": args.load_type,
        "prewarm": args.prewarms,
        "worker_port": args.port or 8070,
        "force": args.force,
        "private_ssh_key": str(args.private_key.resolve()) if args.private_key else None,
        "ansible_args": shlex.split(args.ansible_args),
    }
    direct_mappings = {
        "cpu_queue_policy": "cpu_queue_policy",
        "cpu_count": "cores",
        "concurrent_creation": "concurrent_creation",
    }
    for attribute, option in direct_mappings.items():
        value = getattr(args, attribute)
        if value is not None:
            options[option] = value
    if args.worker_log_dir is not None:
        options["worker_log_dir"] = str(args.worker_log_dir.resolve())
    if args.snapshotter is not None:
        options["snapshotter"] = args.snapshotter
    for override in args.option:
        if "=" not in override:
            raise ExperimentError(f"invalid --option (expected NAME=VALUE): {override}")
        name, raw_value = override.split("=", 1)
        options[name.replace("-", "_")] = parse_value(raw_value)
    unsupported = [
        name for name in (
            "cpu_policy", "concurrency_limit", "domains_file", "timeslice_ms",
            "domain_priority", "domain_fifo", "guardrails_tightness",
            "guardrails_log_base_c", "iat_bucket_size",
        ) if getattr(args, name) is not None
    ]
    if args.overrides:
        unsupported.append("set")
    if unsupported:
        rendered = ", ".join(f"--{name.replace('_', '-')}" for name in unsupported)
        raise ExperimentError(
            f"worker JSON options are direct-backend options ({rendered}); "
            "use --option NAME=VALUE for Ansible environment settings"
        )
    return options


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        resolve_inputs(args)
        if args.backend == "direct":
            return run_direct(direct_config(args))
        options = ansible_options(args)
        queue = make_host_queue([(args.ansible_environment, args.host)])
        run_live(
            str(args.input_csv.resolve()),
            str(args.metadata_csv.resolve()),
            str(args.results_dir.resolve()),
            queue,
            **options,
        )
        return 0
    except KeyboardInterrupt:
        print("interrupted; worker cleanup attempted", file=sys.stderr)
        return 130
    except (
        ExperimentError,
        FileNotFoundError,
        json.JSONDecodeError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
