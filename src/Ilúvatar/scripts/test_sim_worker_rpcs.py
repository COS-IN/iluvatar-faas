#!/usr/bin/env python3
"""End-to-end RPC smoke test against a `--sim` iluvatar_worker process.

This script:
1. launches `iluvatar_worker --sim`
2. waits for gRPC readiness
3. runs ping/register/prewarm/invoke/invoke_async/invoke_async_check/clean
4. shuts down the worker process
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worker-config",
        default="iluvatar_worker/src/worker.json",
        help="Path to worker config file passed to iluvatar_worker --config",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Worker host for RPC smoke test")
    parser.add_argument("--port", type=int, default=8079, help="Worker gRPC port for RPC smoke test")
    parser.add_argument(
        "--startup-timeout-sec",
        type=float,
        default=45.0,
        help="How long to wait for the sim worker to accept TCP connections",
    )
    parser.add_argument(
        "--shutdown-timeout-sec",
        type=float,
        default=10.0,
        help="How long to wait for graceful worker shutdown before force-killing",
    )
    parser.add_argument(
        "--rpc-timeout-sec",
        type=float,
        default=60.0,
        help="Timeout used for each unary RPC",
    )
    parser.add_argument(
        "--async-poll-timeout-sec",
        type=float,
        default=60.0,
        help="How long to wait for invoke_async_check success",
    )
    parser.add_argument("--function-name", default="sim-rpc-smoke", help="Function name to register/invoke")
    parser.add_argument("--function-version", default=None, help="Function version to register/invoke")
    parser.add_argument(
        "--image",
        default="docker.io/alfuerst/hello-iluvatar-action:latest",
        help="Image name passed during register",
    )
    parser.add_argument(
        "--invoke-json",
        default=None,
        help=(
            "Optional simulation invoke JSON object. "
            "Defaults to a CPU payload with warm/cold durations."
        ),
    )
    return parser.parse_args()


def repo_root(script_path: Path) -> Path:
    return script_path.resolve().parents[1]


def wait_for_port(host: str, port: int, timeout_sec: float) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def terminate_worker(worker_proc: subprocess.Popen[str], timeout_sec: float) -> None:
    if worker_proc.poll() is not None:
        return
    worker_proc.send_signal(signal.SIGINT)
    try:
        worker_proc.wait(timeout=timeout_sec)
        return
    except subprocess.TimeoutExpired:
        pass
    worker_proc.terminate()
    try:
        worker_proc.wait(timeout=2.0)
        return
    except subprocess.TimeoutExpired:
        pass
    worker_proc.kill()
    worker_proc.wait(timeout=2.0)


def default_sim_invoke_json() -> str:
    payload = {
        "CPU": {
            "warm_dur_ms": 5,
            "cold_dur_ms": 25,
        }
    }
    return json.dumps(payload)


def tid(label: str) -> str:
    return f"{label}-{uuid.uuid4()}"


def load_python_stubs(root: Path):
    try:
        import grpc
        from grpc_tools import protoc
    except ImportError as exc:
        raise SystemExit(
            "Missing Python gRPC dependencies. Install them with:\n"
            "  python3 -m pip install grpcio grpcio-tools protobuf"
        ) from exc

    include_dir = root / "iluvatar_rpc" / "src"
    proto = include_dir / "rpc" / "iluvatar_rpc.proto"
    temp_dir = tempfile.TemporaryDirectory(prefix="iluvatar-worker-rpc-")
    out_dir = Path(temp_dir.name)
    (out_dir / "rpc").mkdir(parents=True, exist_ok=True)
    (out_dir / "rpc" / "__init__.py").touch()

    rc = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{include_dir}",
            f"--python_out={out_dir}",
            f"--grpc_python_out={out_dir}",
            str(proto),
        ]
    )
    if rc != 0:
        raise SystemExit(f"grpc_tools.protoc failed while compiling {proto}")

    import sys

    sys.path.insert(0, str(out_dir))
    pb2 = importlib.import_module("rpc.iluvatar_rpc_pb2")
    pb2_grpc = importlib.import_module("rpc.iluvatar_rpc_pb2_grpc")
    return grpc, pb2, pb2_grpc, temp_dir


def poll_async_result(stub, pb2, cookie: str, rpc_timeout_sec: float, async_timeout_sec: float):
    deadline = time.time() + async_timeout_sec
    last_payload = ""
    while time.time() < deadline:
        response = stub.invoke_async_check(
            pb2.InvokeAsyncLookupRequest(
                lookup_cookie=cookie,
                transaction_id=tid("invoke-async-check"),
            ),
            timeout=rpc_timeout_sec,
        )
        last_payload = response.json_result
        if response.success:
            return response
        if '"Status": "Invocation not completed"' not in response.json_result:
            raise AssertionError(
                f"invoke_async_check terminal failure: {response.json_result}"
            )
        time.sleep(0.2)
    raise TimeoutError(
        f"invoke_async_check timed out after {async_timeout_sec}s; last={last_payload}"
    )


def run_rpc_flow(args: argparse.Namespace, root: Path, invoke_json: str) -> int:
    grpc, pb2, pb2_grpc, temp_dir = load_python_stubs(root)
    _keep_tmpdir_alive = temp_dir

    target = f"{args.host}:{args.port}"
    channel = grpc.insecure_channel(target)
    grpc.channel_ready_future(channel).result(timeout=args.rpc_timeout_sec)
    stub = pb2_grpc.IluvatarWorkerStub(channel)

    function_version = args.function_version or f"sim-{time.strftime('%Y%m%d-%H%M%S')}"

    ping = stub.ping(
        pb2.PingRequest(message="Ping", transaction_id=tid("ping")),
        timeout=args.rpc_timeout_sec,
    )
    if ping.message != "Pong":
        raise AssertionError(f"unexpected ping response: {ping.message!r}")
    print("[PASS] ping")

    health = stub.health(
        pb2.HealthRequest(transaction_id=tid("health")),
        timeout=args.rpc_timeout_sec,
    )
    if health.status not in (0, 1):
        raise AssertionError(f"unexpected health status: {health.status}")
    print("[PASS] health")

    register = stub.register(
        pb2.RegisterRequest(
            function_name=args.function_name,
            function_version=function_version,
            image_name=args.image,
            memory=512,
            cpus=1,
            parallel_invokes=1,
            transaction_id=tid("register"),
            language=pb2.NOLANG,
            compute=0b00000001,
            isolate=0b00000001,
            container_server=0,
            resource_timings_json="{}",
            system_function=False,
        ),
        timeout=args.rpc_timeout_sec,
    )
    if not register.success:
        raise AssertionError(f"register failed: {register.error}")
    print("[PASS] register")

    prewarm = stub.prewarm(
        pb2.PrewarmRequest(
            function_name=args.function_name,
            function_version=function_version,
            transaction_id=tid("prewarm"),
            compute=0b00000001,
        ),
        timeout=args.rpc_timeout_sec,
    )
    if not prewarm.success:
        raise AssertionError(f"prewarm failed: {prewarm.message}")
    print("[PASS] prewarm")

    invoke = stub.invoke(
        pb2.InvokeRequest(
            function_name=args.function_name,
            function_version=function_version,
            json_args=invoke_json,
            transaction_id=tid("invoke"),
        ),
        timeout=args.rpc_timeout_sec,
    )
    if not invoke.success:
        raise AssertionError(f"invoke failed: {invoke.json_result}")
    print("[PASS] invoke")

    invoke_async = stub.invoke_async(
        pb2.InvokeAsyncRequest(
            function_name=args.function_name,
            function_version=function_version,
            json_args=invoke_json,
            transaction_id=tid("invoke-async"),
        ),
        timeout=args.rpc_timeout_sec,
    )
    if not invoke_async.success or not invoke_async.lookup_cookie:
        raise AssertionError(f"invoke_async failed: success={invoke_async.success}")
    print("[PASS] invoke-async")

    async_checked = poll_async_result(
        stub,
        pb2,
        invoke_async.lookup_cookie,
        args.rpc_timeout_sec,
        args.async_poll_timeout_sec,
    )
    if not async_checked.success:
        raise AssertionError(f"invoke_async_check failed: {async_checked.json_result}")
    print("[PASS] invoke-async-check")

    stub.clean(
        pb2.CleanRequest(transaction_id=tid("clean")),
        timeout=args.rpc_timeout_sec,
    )
    print("[PASS] clean")
    return 0


def main() -> int:
    args = parse_args()
    root = repo_root(Path(__file__))
    worker_config = (
        (root / args.worker_config).resolve() if not os.path.isabs(args.worker_config) else Path(args.worker_config)
    )
    if not worker_config.exists():
        raise SystemExit(f"Worker config not found: {worker_config}")

    invoke_json = args.invoke_json or default_sim_invoke_json()
    try:
        decoded = json.loads(invoke_json)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--invoke-json must be valid JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise SystemExit("--invoke-json must decode to a JSON object")

    with tempfile.TemporaryDirectory(prefix="iluvatar-sim-worker-logs-") as log_dir:
        with open(worker_config, encoding="utf-8") as f:
            worker_cfg_json = json.load(f)
        logging_cfg = worker_cfg_json.get("logging", {})
        logging_cfg["directory"] = log_dir
        worker_cfg_json["logging"] = logging_cfg

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="iluvatar-sim-worker-config-",
            delete=False,
        ) as cfg_file:
            json.dump(worker_cfg_json, cfg_file, indent=2)
            cfg_file.flush()
            sim_config_path = Path(cfg_file.name)

        worker_cmd = [
            "cargo",
            "run",
            "--bin",
            "iluvatar_worker",
            "--",
            "--sim",
            "--config",
            str(sim_config_path),
        ]
        print(f"Launching simulated worker: {' '.join(worker_cmd)}")
        worker_proc = subprocess.Popen(worker_cmd, cwd=root)

        try:
            if not wait_for_port(args.host, args.port, args.startup_timeout_sec):
                raise SystemExit(
                    f"Timed out waiting for simulated worker at {args.host}:{args.port} "
                    f"after {args.startup_timeout_sec}s"
                )
            return run_rpc_flow(args, root, invoke_json)
        finally:
            terminate_worker(worker_proc, args.shutdown_timeout_sec)
            sim_config_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
