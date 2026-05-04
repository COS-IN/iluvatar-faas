#!/usr/bin/env python3
"""Smoke-test the Iluvatar worker gRPC API.

This script generates Python gRPC stubs from the repo's canonical proto at
runtime, so it stays aligned with the Rust worker service definition.

Dependencies:
  python3 -m pip install grpcio grpcio-tools protobuf

Example:
  python3 src/Ilúvatar/scripts/test_worker_rpcs.py \
      --host localhost \
      --port 8079
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_IMAGE = "docker.io/alfuerst/hello-iluvatar-action:latest"

COMPUTE_BITS = {
    "cpu": 0b00000001,
    "gpu": 0b00000010,
    "cpu|gpu": 0b00000011,
}

ISOLATION_BITS = {
    "containerd": 0b00000001,
    "docker": 0b00000010,
    "containerd|docker": 0b00000011,
}

CONTAINER_SERVER = {
    "http": 0,
    "unix": 1,
}


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost", help="Worker host")
    parser.add_argument("--port", type=int, default=8079, help="Worker gRPC port")
    parser.add_argument("--function-name", default="rpc-smoke", help="Function name to register for the test")
    parser.add_argument(
        "--function-version",
        default=None,
        help="Function version to register for the test. Defaults to a unique timestamped value.",
    )
    parser.add_argument(
        "--image",
        default=DEFAULT_IMAGE,
        help="Container image to register. Defaults to the documented hello image.",
    )
    parser.add_argument("--memory-mb", type=int, default=512, help="Memory passed to register")
    parser.add_argument("--cpus", type=int, default=1, help="CPU count passed to register")
    parser.add_argument(
        "--parallel-invokes",
        type=int,
        default=1,
        help="parallel_invokes passed to register. The worker currently only accepts 1.",
    )
    parser.add_argument(
        "--compute",
        choices=sorted(COMPUTE_BITS),
        default="cpu",
        help="Compute bits passed to register/prewarm",
    )
    parser.add_argument(
        "--isolation",
        choices=sorted(ISOLATION_BITS),
        default="containerd",
        help="Isolation bits passed to register",
    )
    parser.add_argument(
        "--container-server",
        choices=sorted(CONTAINER_SERVER),
        default="http",
        help="Container server passed to register",
    )
    parser.add_argument(
        "--invoke-json",
        default='{"name": "rpc-smoke"}',
        help="Raw JSON object passed to invoke/invoke_async",
    )
    parser.add_argument("--rpc-timeout-sec", type=float, default=60.0, help="Timeout for each unary RPC")
    parser.add_argument(
        "--async-poll-interval-sec",
        type=float,
        default=0.25,
        help="Polling interval for invoke_async_check",
    )
    parser.add_argument(
        "--async-poll-timeout-sec",
        type=float,
        default=60.0,
        help="How long to wait for invoke_async_check to complete",
    )
    parser.add_argument(
        "--strict-estimate-shape",
        action="store_true",
        help="Fail if est_invoke_time does not key results by requested FQDNs",
    )
    return parser.parse_args()


def default_version() -> str:
    return f"{time.strftime('py-%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"


def repo_root(script_path: Path) -> Path:
    return script_path.resolve().parents[1]


def proto_paths(script_path: Path) -> tuple[Path, Path]:
    root = repo_root(script_path)
    include_dir = root / "iluvatar_rpc" / "src"
    proto = include_dir / "rpc" / "iluvatar_rpc.proto"
    return include_dir, proto


def load_python_stubs(script_path: Path) -> tuple[Any, Any, Any, Any, tempfile.TemporaryDirectory[str]]:
    try:
        import grpc
        from google.protobuf.json_format import MessageToDict
        from grpc_tools import protoc
    except ImportError as exc:
        raise SystemExit(
            "Missing Python gRPC dependencies. Install them with:\n"
            "  python3 -m pip install grpcio grpcio-tools protobuf"
        ) from exc

    include_dir, proto = proto_paths(script_path)
    temp_dir = tempfile.TemporaryDirectory(prefix="iluvatar-worker-rpc-")
    out_dir = Path(temp_dir.name)
    (out_dir / "rpc").mkdir(parents=True, exist_ok=True)
    (out_dir / "rpc" / "__init__.py").touch()

    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{include_dir}",
            f"--python_out={out_dir}",
            f"--grpc_python_out={out_dir}",
            str(proto),
        ]
    )
    if result != 0:
        raise SystemExit(f"grpc_tools.protoc failed while compiling {proto}")

    sys.path.insert(0, str(out_dir))
    pb2 = importlib.import_module("rpc.iluvatar_rpc_pb2")
    pb2_grpc = importlib.import_module("rpc.iluvatar_rpc_pb2_grpc")
    return grpc, MessageToDict, pb2, pb2_grpc, temp_dir


def as_dict(message: Any, message_to_dict: Any) -> dict[str, Any]:
    return message_to_dict(
        message,
        preserving_proto_field_name=True,
        use_integers_for_enums=True,
    )


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def render_message(message: Any, message_to_dict: Any) -> str:
    return compact_json(as_dict(message, message_to_dict))


def tid(label: str) -> str:
    return f"{label}-{uuid.uuid4()}"


def fqdn(function_name: str, function_version: str) -> str:
    return f"{function_name}-{function_version}"


def validate_invoke_json(raw_json: str) -> str:
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--invoke-json must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SystemExit("--invoke-json must decode to a JSON object")
    return json.dumps(parsed)


def find_registered(functions: Iterable[Any], function_name: str, function_version: str) -> bool:
    for func in functions:
        if func.function_name == function_name and func.function_version == function_version:
            return True
    return False


def poll_async_result(stub: Any, pb2: Any, cookie: str, args: argparse.Namespace, invoke_json: str) -> Any:
    deadline = time.time() + args.async_poll_timeout_sec
    last_payload = None
    while time.time() < deadline:
        response = stub.invoke_async_check(
            pb2.InvokeAsyncLookupRequest(
                lookup_cookie=cookie,
                transaction_id=tid("invoke-async-check"),
            ),
            timeout=args.rpc_timeout_sec,
        )
        last_payload = response.json_result
        if response.success:
            return response
        if '"Status": "Invocation not completed"' not in response.json_result:
            raise AssertionError(
                f"invoke_async_check returned a terminal failure for args {invoke_json}: {response.json_result}"
            )
        time.sleep(args.async_poll_interval_sec)
    raise AssertionError(f"invoke_async_check timed out after {args.async_poll_timeout_sec}s; last payload: {last_payload}")


def check_estimate_shape(
    requested_fqdns: list[str],
    est_times: dict[str, float],
    strict: bool,
) -> tuple[bool, str]:
    requested_set = set(requested_fqdns)
    response_keys = set(est_times)
    matched_keys = sorted(requested_set & response_keys)
    missing_keys = sorted(requested_set - response_keys)
    detail = (
        f"requested={requested_fqdns} "
        f"response_keys={sorted(response_keys)} "
        f"matched={matched_keys} "
        f"missing={missing_keys} "
        f"values={compact_json(est_times)}"
    )
    if strict and missing_keys:
        return False, detail
    return True, detail


def main() -> int:
    args = parse_args()
    version = args.function_version or default_version()
    invoke_json = validate_invoke_json(args.invoke_json)
    function_fqdn = fqdn(args.function_name, version)
    grpc, message_to_dict, pb2, pb2_grpc, temp_dir = load_python_stubs(Path(__file__))
    _keep_temp_dir_alive = temp_dir

    results: list[CheckResult] = []
    target = f"{args.host}:{args.port}"

    try:
        channel = grpc.insecure_channel(target)
        grpc.channel_ready_future(channel).result(timeout=args.rpc_timeout_sec)
        stub = pb2_grpc.IluvatarWorkerStub(channel)
    except Exception as exc:
        print(f"[FAIL] connect: {target} ({exc})")
        return 1

    def run_check(name: str, fn: Any) -> Any:
        try:
            value = fn()
            if isinstance(value, str):
                detail = value
            elif hasattr(value, "DESCRIPTOR"):
                detail = render_message(value, message_to_dict)
            else:
                detail = compact_json(value)
            results.append(CheckResult(name=name, ok=True, detail=detail))
            return value
        except Exception as exc:
            results.append(CheckResult(name=name, ok=False, detail=str(exc)))
            return None

    ping = run_check(
        "ping",
        lambda: render_message(
            stub.ping(pb2.PingRequest(message="Ping", transaction_id=tid("ping")), timeout=args.rpc_timeout_sec),
            message_to_dict,
        ),
    )
    if ping is None:
        return finish(results)

    health = run_check(
        "health",
        lambda: render_message(
            stub.health(pb2.HealthRequest(transaction_id=tid("health")), timeout=args.rpc_timeout_sec),
            message_to_dict,
        ),
    )
    if health is None:
        return finish(results)

    status_before = run_check(
        "status-before",
        lambda: render_message(
            stub.status(pb2.StatusRequest(transaction_id=tid("status-before")), timeout=args.rpc_timeout_sec),
            message_to_dict,
        ),
    )
    if status_before is None:
        return finish(results)

    list_before = run_check(
        "list-before",
        lambda: stub.list_registered_funcs(
            pb2.ListFunctionRequest(transaction_id=tid("list-before")),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if list_before is None:
        return finish(results)

    register_response = run_check(
        "register",
        lambda: stub.register(
            pb2.RegisterRequest(
                function_name=args.function_name,
                function_version=version,
                image_name=args.image,
                memory=args.memory_mb,
                cpus=args.cpus,
                parallel_invokes=args.parallel_invokes,
                transaction_id=tid("register"),
                language=pb2.NOLANG,
                compute=COMPUTE_BITS[args.compute],
                isolate=ISOLATION_BITS[args.isolation],
                container_server=CONTAINER_SERVER[args.container_server],
                resource_timings_json="{}",
                system_function=False,
            ),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if register_response is None:
        return finish(results)
    if not register_response.success:
        results[-1] = CheckResult(
            name="register",
            ok=False,
            detail=render_message(register_response, message_to_dict),
        )
        return finish(results)

    list_after = run_check(
        "list-after",
        lambda: stub.list_registered_funcs(
            pb2.ListFunctionRequest(transaction_id=tid("list-after")),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if list_after is None:
        return finish(results)
    if not find_registered(list_after.functions, args.function_name, version):
        results[-1] = CheckResult(
            name="list-after",
            ok=False,
            detail=f"{function_fqdn} was not present in list_registered_funcs after register",
        )
        return finish(results)

    est_single = run_check(
        "est-invoke-time-single",
        lambda: stub.est_invoke_time(
            pb2.EstInvokeRequest(
                fqdns=[function_fqdn],
                transaction_id=tid("est-single"),
            ),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if est_single is None:
        return finish(results)
    if not est_single.est_times:
        results[-1] = CheckResult(
            name="est-invoke-time-single",
            ok=False,
            detail="est_invoke_time returned no values for a single registered FQDN",
        )
        return finish(results)
    ok, detail = check_estimate_shape(
        [function_fqdn],
        dict(est_single.est_times),
        args.strict_estimate_shape,
    )
    results[-1] = CheckResult(name="est-invoke-time-single", ok=ok, detail=detail)

    batch_estimate_fqdns = [function_fqdn]
    other_fqdns = [
        f"{func.function_name}-{func.function_version}"
        for func in list_before.functions
        if f"{func.function_name}-{func.function_version}" != function_fqdn
    ]
    if other_fqdns:
        batch_estimate_fqdns.append(other_fqdns[0])
    else:
        batch_estimate_fqdns.append(f"{function_fqdn}-missing")

    est_batch = run_check(
        "est-invoke-time-batch",
        lambda: stub.est_invoke_time(
            pb2.EstInvokeRequest(
                fqdns=batch_estimate_fqdns,
                transaction_id=tid("est-batch"),
            ),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if est_batch is None:
        return finish(results)
    if not est_batch.est_times:
        results[-1] = CheckResult(
            name="est-invoke-time-batch",
            ok=False,
            detail="est_invoke_time returned no values for a batch request",
        )
        return finish(results)
    ok, detail = check_estimate_shape(
        batch_estimate_fqdns,
        dict(est_batch.est_times),
        args.strict_estimate_shape,
    )
    results[-1] = CheckResult(name="est-invoke-time-batch", ok=ok, detail=detail)

    prewarm_response = run_check(
        "prewarm",
        lambda: stub.prewarm(
            pb2.PrewarmRequest(
                function_name=args.function_name,
                function_version=version,
                transaction_id=tid("prewarm"),
                compute=COMPUTE_BITS[args.compute],
            ),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if prewarm_response is None:
        return finish(results)
    if not prewarm_response.success:
        results[-1] = CheckResult(
            name="prewarm",
            ok=False,
            detail=render_message(prewarm_response, message_to_dict),
        )
        return finish(results)

    invoke_response = run_check(
        "invoke",
        lambda: stub.invoke(
            pb2.InvokeRequest(
                function_name=args.function_name,
                function_version=version,
                json_args=invoke_json,
                transaction_id=tid("invoke"),
            ),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if invoke_response is None:
        return finish(results)
    if not invoke_response.success:
        results[-1] = CheckResult(
            name="invoke",
            ok=False,
            detail=render_message(invoke_response, message_to_dict),
        )
        return finish(results)

    invoke_async_response = run_check(
        "invoke-async",
        lambda: stub.invoke_async(
            pb2.InvokeAsyncRequest(
                function_name=args.function_name,
                function_version=version,
                json_args=invoke_json,
                transaction_id=tid("invoke-async"),
            ),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if invoke_async_response is None:
        return finish(results)
    if not invoke_async_response.success:
        results[-1] = CheckResult(
            name="invoke-async",
            ok=False,
            detail=render_message(invoke_async_response, message_to_dict),
        )
        return finish(results)
    if not invoke_async_response.lookup_cookie:
        results[-1] = CheckResult(
            name="invoke-async",
            ok=False,
            detail="invoke_async returned an empty lookup_cookie",
        )
        return finish(results)

    async_checked = run_check(
        "invoke-async-check",
        lambda: poll_async_result(stub, pb2, invoke_async_response.lookup_cookie, args, invoke_json),
    )
    if async_checked is None:
        return finish(results)
    if not async_checked.success:
        results[-1] = CheckResult(
            name="invoke-async-check",
            ok=False,
            detail=render_message(async_checked, message_to_dict),
        )
        return finish(results)

    clean_response = run_check(
        "clean",
        lambda: stub.clean(
            pb2.CleanRequest(transaction_id=tid("clean")),
            timeout=args.rpc_timeout_sec,
        ),
    )
    if clean_response is None:
        return finish(results)

    run_check(
        "status-after",
        lambda: render_message(
            stub.status(pb2.StatusRequest(transaction_id=tid("status-after")), timeout=args.rpc_timeout_sec),
            message_to_dict,
        ),
    )

    return finish(results)


def finish(results: list[CheckResult]) -> int:
    failures = 0
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        print(f"[{status}] {result.name}: {result.detail}")
        if not result.ok:
            failures += 1
    print(f"Summary: {len(results) - failures}/{len(results)} checks passed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
