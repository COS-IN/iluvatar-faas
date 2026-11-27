#!/usr/bin/env python3
"""Join PostedPrice (SMC) dispatch-time prices to observed invocation latencies.

Reads the worker's JSON-lines log and the load-generator output CSV from one
experiment directory. All joins use invocation tid; missing worker events stay
blank rather than being treated as zero.
"""

from __future__ import annotations

import argparse
from bisect import bisect_left
import csv
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


PRICE_FIELDS = [
    "l1_rho", "bias_cores", "pred_pelt_cores",
    "effective_rho", "pelt_util", "sigma_d", "p_d",
    "private_cost", "externality_cost", "state_cost",
    "total_price", "benefit_B_i", "toll_to_benefit", "net_gain",
]
LATENCY_FIELDS = [
    "function_exec_ms", "worker_duration_ms", "client_e2e_ms",
    "worker_e2e_ms", "dispatcher_queue_ms", "pricing_queue_ms",
    "container_acquire_ms", "exec_ms", "post_overhead_ms",
]
CORRELATION_FEATURES = [
    "total_price", "toll_to_benefit", "net_gain", "benefit_B_i",
    "private_cost", "externality_cost", "state_cost",
    "l1_rho", "bias_cores", "pred_pelt_cores",
    "effective_rho", "pelt_util", "sigma_d", "p_d",
]
CORRELATION_TARGETS = [
    "function_exec_ms", "worker_duration_ms", "client_e2e_ms",
    "worker_e2e_ms", "dispatcher_queue_ms", "pricing_queue_ms",
    "container_acquire_ms", "post_overhead_ms",
]


def number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.strip().lower() in {"none", "null", "nan"}:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    return str(value).strip().lower() in {"true", "1", "yes"}


def timestamp_seconds(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def logged_number(value: Any) -> float | None:
    """Read numbers from both ordinary JSON fields and Rust Option debug values."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in {"None", "null", ""}:
        return None
    direct = number(value)
    if direct is not None:
        return direct
    match = re.search(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?", str(value))
    return number(match.group(0)) if match else None


def read_worker_log(path: Path) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, float],
    dict[str, int],
]:
    selected_prices: dict[str, dict[str, Any]] = {}
    express_dispatches: dict[str, dict[str, Any]] = {}
    candidates: list[dict[str, Any]] = []
    timings: dict[str, dict[str, Any]] = {}
    worker_e2e: dict[str, dict[str, Any]] = {}
    queues: dict[str, dict[str, Any]] = {}
    pelt_samples: dict[str, dict[str, Any]] = {}
    pelt_telemetry: list[dict[str, Any]] = []
    boot_starts: dict[str, float] = {}
    boot_ends: dict[str, float] = {}
    boot_domains: dict[str, int] = {}
    dispatch_times: dict[str, float] = {}
    assigned_domains: dict[str, int] = {}

    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                record = json.loads(line)
                fields = record.get("fields", {})
                message = fields.get("message", "")
                tid = fields.get("tid") or record.get("span", {}).get("tid")
                if not tid:
                    continue

                if message == "[congestion_pricing] price candidate":
                    row = dict(fields)
                    row["log_timestamp"] = record.get("timestamp")
                    row["_line_number"] = line_number
                    candidates.append(row)
                    if bool_value(fields.get("selected")):
                        selected_prices.setdefault(tid, row)
                elif message == "[posted_price] express dispatch":
                    row = dict(fields)
                    row["log_timestamp"] = record.get("timestamp")
                    express_dispatches.setdefault(tid, row)
                elif message == "[congestion_pricing] invocation timing":
                    timings[tid] = dict(fields)
                elif message == "[finesched][core-metrics] sampled assigned domain":
                    sample_ts = timestamp_seconds(record.get("timestamp"))
                    pelt_util = logged_number(fields.get("domain_pelt_like_utilization_percent"))
                    runnable_load = logged_number(fields.get("runnable_load"))
                    domain_id = logged_number(fields.get("domain_id"))
                    pelt_samples[tid] = {
                        "domain_id": domain_id,
                        "selected_pelt_util": pelt_util / 100.0 if pelt_util is not None else None,
                        "selected_pelt_runnable": runnable_load,
                    }
                    if sample_ts is not None and domain_id is not None:
                        pelt_telemetry.append({
                            "timestamp_s": sample_ts,
                            "domain_id": int(domain_id),
                            "pelt_util": pelt_util / 100.0 if pelt_util is not None else None,
                            "runnable_load": runnable_load,
                        })
                elif message == "Trying to cold start a new container":
                    ts = timestamp_seconds(record.get("timestamp"))
                    if ts is not None:
                        boot_starts.setdefault(tid, ts)
                elif message == "Container cold start completed":
                    ts = timestamp_seconds(record.get("timestamp"))
                    if ts is not None:
                        boot_ends[tid] = ts
                elif message == "Handling invocation request":
                    ts = timestamp_seconds(record.get("timestamp"))
                    if ts is not None:
                        dispatch_times[tid] = ts
                elif message in (
                    "[finesched] domain assigned to function request",
                    "[finesched] domain assigned to function cgroup",
                ):
                    domain = logged_number(fields.get("domain_id"))
                    if domain is not None:
                        assigned_domains[tid] = int(domain)
                elif message == "Invocation complete":
                    e2e_sec = number(fields.get("e2etime"))
                    if e2e_sec is not None:
                        worker_e2e[tid] = {
                            "worker_e2e_ms": e2e_sec * 1000.0,
                            "worker_complete_timestamp": record.get("timestamp"),
                        }
                elif message == "Item starting to execute":
                    insert = fields.get("insert_time")
                    remove = fields.get("remove_time")
                    if insert and remove:
                        try:
                            wait_ms = (datetime.fromisoformat(remove) - datetime.fromisoformat(insert)).total_seconds() * 1000.0
                            queues[tid] = {
                                "dispatcher_queue_ms": max(0.0, wait_ms),
                                "queue_insert_time": insert,
                                "queue_remove_time": remove,
                            }
                        except ValueError:
                            pass
            except (json.JSONDecodeError, AttributeError, TypeError):
                continue

    extras: dict[str, dict[str, Any]] = {}
    for tid in set(timings) | set(worker_e2e) | set(queues):
        extras[tid] = {}
        extras[tid].update(timings.get(tid, {}))
        extras[tid].update(worker_e2e.get(tid, {}))
        extras[tid].update(queues.get(tid, {}))

    boot_intervals = []
    for tid, start in boot_starts.items():
        end = boot_ends.get(tid)
        if end is not None and end >= start:
            boot_intervals.append({
                "tid": tid,
                "start_s": start,
                "end_s": end,
                "cold_init_ms": (end - start) * 1000.0,
                "domain": boot_domains.get(tid, assigned_domains.get(tid)),
            })
    return (
        selected_prices,
        express_dispatches,
        candidates,
        extras,
        pelt_samples,
        pelt_telemetry,
        boot_intervals,
        dispatch_times,
        assigned_domains,
    )


def locate_files(run_dir: Path, worker_arg: str | None, csv_arg: str | None) -> tuple[Path, Path]:
    if worker_arg:
        worker = Path(worker_arg)
    else:
        workers = [run_dir / name for name in ("worker1.log", "worker_worker1.log", "worker.log") if (run_dir / name).is_file()]
        if not workers:
            workers = sorted(run_dir.glob("worker*.log"))
        if not workers:
            raise SystemExit(f"No worker log found under {run_dir}")
        worker = workers[0]

    if csv_arg:
        invocations = Path(csv_arg)
    else:
        candidates = sorted(p for p in run_dir.glob("output-*.csv") if "full" not in p.name.lower())
        if len(candidates) != 1:
            raise SystemExit(f"Expected one output-*.csv in {run_dir}, found {len(candidates)}; pass --invocations")
        invocations = candidates[0]
    if not worker.is_file():
        raise SystemExit(f"Worker log does not exist: {worker}")
    if not invocations.is_file():
        raise SystemExit(f"Invocation CSV does not exist: {invocations}")
    return worker, invocations


def load_invocations(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8-sig") as stream:
        for index, raw in enumerate(csv.DictReader(stream)):
            tid = raw.get("tid")
            if not tid:
                continue
            row: dict[str, Any] = {
                "trace_row": index,
                "tid": tid,
                "function_name": raw.get("function_name", ""),
                "success": bool_value(raw.get("success")),
                "was_cold": bool_value(raw.get("was_cold")),
            }
            for src, dst, factor in [
                ("worker_duration_us", "worker_duration_ms", 0.001),
                ("e2e_duration_us", "client_e2e_ms", 0.001),
                ("code_duration_sec", "function_exec_ms", 1000.0),
            ]:
                value = number(raw.get(src))
                row[dst] = None if value is None else value * factor
            row["csv_assignment_timestamp"] = raw.get("assignment_timestamp", "")
            rows.append(row)
    return rows


def load_warm_runtime_metadata(path: Path | None) -> dict[str, float]:
    if path is None or not path.is_file():
        return {}
    runtimes: dict[str, float] = {}
    with path.open(newline="", encoding="utf-8-sig") as stream:
        for raw in csv.DictReader(stream):
            name = raw.get("func_name") or raw.get("function_name")
            duration = number(raw.get("warm_dur_ms"))
            if name and duration is not None and duration > 0:
                runtimes[name] = duration
    return runtimes


def load_run_plot_config(run_dir: Path) -> tuple[float | None, float | None, int]:
    """Return optional legacy rho_knee, cores/domain, and domain_count from saved worker config."""
    configs = []
    for name in ("experiment-config.json", "worker_config.json", "requested_worker_config.json"):
        path = run_dir / name
        if path.is_file():
            try:
                configs.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue

    rho_knee = None
    for config in configs:
        inv = config.get("invocation", {})
        cp = inv.get("posted_price") or inv.get("congestion_pricing") or {}
        rho_knee = number(cp.get("rho_knee"))
        if rho_knee is not None:
            break

    cpu_count = domain_count = None
    for config in reversed(configs):
        cpus = config.get("container_resources", {}).get("cpu_resource", {}).get("count")
        domains = config.get("fineloadbalancing", {}).get("preallocated_groups", {}).get("domain_count")
        cpu_count = number(cpus) if cpus is not None else cpu_count
        domain_count = number(domains) if domains is not None else domain_count
    cores_per_domain = cpu_count / domain_count if cpu_count and domain_count and domain_count > 0 else None
    n_domains = int(domain_count) if domain_count and domain_count > 0 else None
    return rho_knee, cores_per_domain, n_domains


def normalize_price_fields(row: dict[str, Any]) -> None:
    """Normalize PostedPrice and legacy field aliases into canonical SMC field names."""
    eff_rho = number(row.get("effective_rho"))
    if eff_rho is None:
        eff_rho = number(row.get("rho"))
    row["effective_rho"] = eff_rho
    row["rho"] = eff_rho
    row["l1_rho"] = number(row.get("l1_rho")) if row.get("l1_rho") is not None else eff_rho
    row["bias_cores"] = number(row.get("bias_cores"))
    row["pred_pelt_cores"] = number(row.get("pred_pelt_cores"))
    row["sigma_d"] = number(row.get("sigma_d"))
    row["p_d"] = number(row.get("p_d"))

    total_price = number(row.get("total_price"))
    if total_price is None:
        total_price = number(row.get("total_marginal_toll"))
    row["total_price"] = total_price
    row["total_marginal_toll"] = total_price

    benefit_b_i = number(row.get("benefit_B_i"))
    if benefit_b_i is None:
        benefit_b_i = number(row.get("benefit"))
    row["benefit_B_i"] = benefit_b_i
    row["benefit"] = benefit_b_i

    actual_dom = number(row.get("actual_domain"))
    if actual_dom is None:
        actual_dom = number(row.get("final_domain"))
    row["actual_domain"] = int(actual_dom) if actual_dom is not None else None


def enrich_candidate_records(
    candidates: list[dict[str, Any]],
    telemetry: list[dict[str, Any]],
    warm_runtimes: dict[str, float],
    function_by_tid: dict[str, str],
    priority_weight_by_tid: dict[str, float],
    cores_per_domain: float | None,
    tolerance_ms: float = 50.0,
) -> int:
    """Attach dispatch-time PELT (direct logged `pelt_util` first, nearest sample fallback)."""
    telemetry_by_domain: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for sample in telemetry:
        telemetry_by_domain[int(sample["domain_id"])].append(sample)
    timestamps_by_domain: dict[int, list[float]] = {}
    for domain, samples in telemetry_by_domain.items():
        samples.sort(key=lambda row: row["timestamp_s"])
        timestamps_by_domain[domain] = [row["timestamp_s"] for row in samples]

    matched = 0
    for candidate in candidates:
        normalize_price_fields(candidate)
        tid = candidate.get("tid")
        domain = number(candidate.get("candidate_domain", candidate.get("domain_id")))
        timestamp = timestamp_seconds(candidate.get("log_timestamp"))
        candidate["domain_id"] = int(domain) if domain is not None else None
        candidate["is_selected"] = bool_value(candidate.get("selected"))

        runtime_est = number(candidate.get("estimated_runtime_ms"))
        function_name = function_by_tid.get(tid or "", "")
        if runtime_est is None:
            runtime_est = warm_runtimes.get(function_name)
        candidate["function_name"] = function_name or None
        candidate["runtime_est_ms"] = runtime_est

        benefit_b_i = candidate.get("benefit_B_i")
        if benefit_b_i is None and runtime_est is not None:
            priority_weight = number(candidate.get("priority_weight")) or priority_weight_by_tid.get(tid or "")
            if priority_weight is not None:
                benefit_b_i = priority_weight * runtime_est
        candidate["benefit_B_i"] = benefit_b_i
        candidate["estimated_benefit"] = benefit_b_i

        # Direct dispatch-time PELT fields logged by PostedPriceCoordinator take precedence.
        direct_pelt_util = logged_number(candidate.get("pelt_util"))
        direct_pelt_runnable = logged_number(candidate.get("pelt_runnable"))
        sample_age_ms: float | None = 0.0 if direct_pelt_util is not None else None

        if direct_pelt_util is None and domain is not None and timestamp is not None:
            dom_int = int(domain)
            domain_samples = telemetry_by_domain.get(dom_int, [])
            domain_times = timestamps_by_domain.get(dom_int, [])
            pos = bisect_left(domain_times, timestamp)
            nearby = [i for i in (pos - 1, pos) if 0 <= i < len(domain_samples)]
            if nearby:
                nearest = min(nearby, key=lambda i: abs(domain_times[i] - timestamp))
                age_ms = abs(domain_times[nearest] - timestamp) * 1000.0
                if age_ms <= tolerance_ms:
                    direct_pelt_util = domain_samples[nearest].get("pelt_util")
                    direct_pelt_runnable = domain_samples[nearest].get("runnable_load")
                    sample_age_ms = age_ms

        candidate["pelt_util"] = direct_pelt_util
        candidate["pelt_runnable"] = direct_pelt_runnable
        candidate["candidate_domain_pelt_util"] = direct_pelt_util
        candidate["candidate_domain_pelt_runnable"] = direct_pelt_runnable
        candidate["candidate_domain_norm_runnable"] = (
            direct_pelt_runnable / cores_per_domain
            if direct_pelt_runnable is not None and cores_per_domain
            else None
        )
        candidate["candidate_pelt_sample_age_ms"] = sample_age_ms

        private_cost = number(candidate.get("private_cost"))
        externality_cost = number(candidate.get("externality_cost"))
        total_price = candidate.get("total_price")

        unit_price = number(candidate.get("unit_congestion_price"))
        if unit_price is None and runtime_est and runtime_est > 0:
            if private_cost is not None and externality_cost is not None:
                unit_price = (private_cost + externality_cost) / runtime_est
            else:
                warm_toll = number(candidate.get("warm_toll"))
                queue_toll = number(candidate.get("queue_backlog_toll", candidate.get("queue_toll"))) or 0.0
                if warm_toll is not None:
                    unit_price = (warm_toll + queue_toll) / runtime_est
        candidate["unit_congestion_price"] = unit_price if direct_pelt_util is not None else None

        toll_to_benefit = number(candidate.get("toll_to_benefit"))
        if toll_to_benefit is None and benefit_b_i and benefit_b_i > 0 and total_price is not None:
            toll_to_benefit = total_price / benefit_b_i
        candidate["toll_to_benefit"] = toll_to_benefit if direct_pelt_util is not None else None

        if direct_pelt_util is not None:
            matched += 1
    return matched


def add_derived_metrics(
    rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    express_dispatches: dict[str, dict[str, Any]],
    pelt_samples: dict[str, dict[str, Any]],
    warm_runtimes: dict[str, float],
    boot_intervals: list[dict[str, Any]],
    dispatch_times: dict[str, float],
    assigned_domains: dict[str, int],
) -> None:
    candidates_by_tid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        tid = candidate.get("tid")
        if tid:
            candidates_by_tid[tid].append(candidate)

    intervals_by_tid = {boot["tid"]: boot for boot in boot_intervals}
    queries: list[tuple[int, float, int | None]] = []
    for index, row in enumerate(rows):
        normalize_price_fields(row)
        tid = row["tid"]
        event_candidates = candidates_by_tid.get(tid, [])
        rhos = [v for c in event_candidates if (v := number(c.get("effective_rho", c.get("rho")))) is not None]
        row["domain_rho_spread"] = max(rhos) - min(rhos) if rhos else None
        selected_rho = row.get("effective_rho")
        if row.get("state_kind") in ("WarmLocal", "WarmRemote") and selected_rho is not None and rhos:
            row["warm_affinity_gap"] = selected_rho - min(rhos)
        else:
            row["warm_affinity_gap"] = None

        runtime_est = number(row.get("estimated_runtime_ms"))
        if runtime_est is None and tid in express_dispatches:
            runtime_est = number(express_dispatches[tid].get("estimated_runtime_ms"))
        if runtime_est is None:
            runtime_est = warm_runtimes.get(row.get("function_name", ""))
        row["runtime_est_ms"] = runtime_est

        benefit = row.get("benefit_B_i")
        exec_ms = number(row.get("exec_ms"))
        queue_ms = number(row.get("pricing_queue_ms"))
        worker_e2e_ms = number(row.get("worker_e2e_ms"))
        priority_w = number(row.get("priority_weight"))
        if priority_w is None and tid in express_dispatches:
            priority_w = number(express_dispatches[tid].get("priority_weight"))
        if priority_w is None and benefit is not None and runtime_est:
            priority_w = benefit / runtime_est
        row["priority_weight"] = priority_w
        row["exec_stretch"] = exec_ms / runtime_est if exec_ms is not None and runtime_est else None
        row["norm_queue_wait"] = queue_ms / runtime_est if queue_ms is not None and runtime_est else None
        row["dispatch_overhead_ms"] = (
            worker_e2e_ms - queue_ms - exec_ms
            if worker_e2e_ms is not None and queue_ms is not None and exec_ms is not None
            else None
        )

        express_dom = number(express_dispatches.get(tid, {}).get("actual_domain"))
        coordinator_actual = row.get("actual_domain")
        if coordinator_actual is None and express_dom is not None:
            coordinator_actual = int(express_dom)
            row["actual_domain"] = coordinator_actual
        finesched_dom = assigned_domains.get(tid)
        row["finesched_domain"] = finesched_dom
        selected_domain = coordinator_actual if coordinator_actual is not None else finesched_dom
        row["selected_domain"] = int(selected_domain) if selected_domain is not None else None

        direct_pelt = logged_number(row.get("pelt_util"))
        direct_runnable = logged_number(row.get("pelt_runnable"))
        pelt = pelt_samples.get(tid, {})
        pelt_domain = number(pelt.get("domain_id"))
        if selected_domain is not None and pelt_domain is not None and int(pelt_domain) != int(selected_domain):
            pelt = {}
        row["selected_pelt_util"] = direct_pelt if direct_pelt is not None else number(pelt.get("selected_pelt_util"))
        row["selected_pelt_runnable"] = (
            direct_runnable if direct_runnable is not None else number(pelt.get("selected_pelt_runnable"))
        )
        row["pelt_util"] = row["selected_pelt_util"]
        runnable, util = row["selected_pelt_runnable"], row["selected_pelt_util"]
        row["pelt_runnable_gap"] = runnable - util if runnable is not None and util is not None else None

        boot = intervals_by_tid.get(tid)
        row["cold_init_ms"] = boot.get("cold_init_ms") if boot else None
        timestamp = boot.get("start_s") if boot else None
        if timestamp is None:
            timestamp = timestamp_seconds(row.get("log_timestamp"))
        if timestamp is None:
            timestamp = dispatch_times.get(tid)
        if timestamp is not None:
            queries.append((index, timestamp, row["selected_domain"]))

    counts = concurrent_boot_counts(boot_intervals, queries)
    for index, row in enumerate(rows):
        row["concurrent_boots_host"], row["concurrent_boots_domain"] = counts.get(index, (None, None))


def compute_attribution_summary(
    rows: list[dict[str, Any]],
    selected_prices: dict[str, dict[str, Any]],
    express_dispatches: dict[str, dict[str, Any]],
    assigned_domains: dict[str, int],
    n_domains: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    priced_by_dom: Counter[int] = Counter()
    express_by_dom: Counter[int] = Counter()
    finesched_by_dom: Counter[int] = Counter()
    matched_by_dom: Counter[int] = Counter()
    mismatched_by_dom: Counter[int] = Counter()
    spills_by_dom: Counter[int] = Counter()

    checked = 0
    matched = 0
    mismatched = 0
    spill_total = 0

    for row in rows:
        tid = row["tid"]
        fdom = assigned_domains.get(tid)
        if fdom is not None:
            finesched_by_dom[fdom] += 1

        if tid in selected_prices:
            srow = selected_prices[tid]
            adom = number(srow.get("actual_domain", srow.get("final_domain")))
            if adom is not None:
                adom_i = int(adom)
                priced_by_dom[adom_i] += 1
                if bool_value(srow.get("spilled")):
                    spill_total += 1
                    spills_by_dom[adom_i] += 1
                if fdom is not None:
                    checked += 1
                    if adom_i == fdom:
                        matched += 1
                        matched_by_dom[adom_i] += 1
                    else:
                        mismatched += 1
                        mismatched_by_dom[adom_i] += 1
        elif tid in express_dispatches:
            erow = express_dispatches[tid]
            adom = number(erow.get("actual_domain"))
            if adom is not None:
                adom_i = int(adom)
                express_by_dom[adom_i] += 1
                if bool_value(erow.get("spilled")):
                    spill_total += 1
                    spills_by_dom[adom_i] += 1
                if fdom is not None:
                    checked += 1
                    if adom_i == fdom:
                        matched += 1
                        matched_by_dom[adom_i] += 1
                    else:
                        mismatched += 1
                        mismatched_by_dom[adom_i] += 1

    total_coord = sum(priced_by_dom.values()) + sum(express_by_dom.values())
    total_finesched = sum(finesched_by_dom.values())
    domain_ids = sorted(set(range(n_domains)) | set(priced_by_dom) | set(express_by_dom) | set(finesched_by_dom))

    table: list[dict[str, Any]] = []
    for d in domain_ids:
        coord_d = priced_by_dom[d] + express_by_dom[d]
        table.append({
            "domain": f"d{d}",
            "priced_dispatches": priced_by_dom[d],
            "express_dispatches": express_by_dom[d],
            "total_posted_price_dispatches": coord_d,
            "posted_price_share_pct": (100.0 * coord_d / total_coord) if total_coord else None,
            "finesched_assigned_dispatches": finesched_by_dom[d],
            "finesched_share_pct": (100.0 * finesched_by_dom[d] / total_finesched) if total_finesched else None,
            "spilled_into_domain": spills_by_dom[d],
            "matched_1to1": matched_by_dom[d],
            "mismatched_1to1": mismatched_by_dom[d],
        })

    summary = {
        "total_posted_price_dispatches": total_coord,
        "total_finesched_dispatches": total_finesched,
        "attribution_checked_n": checked,
        "attribution_matched_n": matched,
        "attribution_mismatch_n": mismatched,
        "attribution_match_fraction": (matched / checked) if checked > 0 else None,
        "spill_count": spill_total,
        "domain_dispatch_counts": {
            f"d{d}": priced_by_dom[d] + express_by_dom[d] for d in domain_ids
        },
    }
    return table, summary


def concurrent_boot_counts(
    intervals: list[dict[str, Any]], queries: list[tuple[int, float, int | None]]
) -> dict[int, tuple[int, int | None]]:
    """Sweep half-open boot intervals and dispatch queries in O(N log N)."""
    timeline: list[tuple[float, int, int, int | None, int | None]] = []
    for interval in intervals:
        domain = interval.get("domain")
        timeline.append((interval["end_s"], 0, -1, domain, None))
        timeline.append((interval["start_s"], 1, 1, domain, None))
    for index, timestamp, domain in queries:
        timeline.append((timestamp, 2, 0, domain, index))
    timeline.sort(key=lambda item: (item[0], item[1]))

    host_active = 0
    domain_active: dict[int, int] = defaultdict(int)
    result: dict[int, tuple[int, int | None]] = {}
    for _, order, delta, domain, query_index in timeline:
        if order != 2:
            host_active += delta
            if domain is not None:
                domain_active[domain] += delta
                if domain_active[domain] <= 0:
                    domain_active.pop(domain, None)
        elif query_index is not None:
            result[query_index] = (
                host_active,
                domain_active.get(domain, 0) if domain is not None else None,
            )
    return result


def rank(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and values[order[j]] == values[order[i]]:
            j += 1
        average_rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[order[k]] = average_rank
        i = j
    return ranks


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    mx, my = statistics.mean(xs), statistics.mean(ys)
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    denom = math.sqrt(sum(x * x for x in dx) * sum(y * y for y in dy))
    return None if denom == 0 else sum(x * y for x, y in zip(dx, dy)) / denom


def correlation_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cohorts: list[tuple[str, list[dict[str, Any]]]] = [("all", rows)]
    cohorts.extend(
        (f"function:{f}", [r for r in rows if r.get("function_class") == f])
        for f in sorted({r.get("function_class") for r in rows if r.get("function_class")})
    )
    for cold, label in ((True, "cold"), (False, "warm")):
        cohorts.append((label, [r for r in rows if r.get("was_cold") is cold]))

    out: list[dict[str, Any]] = []
    for cohort, cohort_rows in cohorts:
        for feature in CORRELATION_FEATURES:
            for target in CORRELATION_TARGETS:
                pairs = [(number(r.get(feature)), number(r.get(target))) for r in cohort_rows]
                pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
                xs, ys = [p[0] for p in pairs], [p[1] for p in pairs]
                out.append({
                    "cohort": cohort,
                    "price_feature": feature,
                    "latency_metric": target,
                    "n": len(pairs),
                    "pearson_r": pearson(xs, ys),
                    "spearman_r": pearson(rank(xs), rank(ys)) if len(xs) >= 3 else None,
                })
    return out


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * p
    lo, hi = math.floor(position), math.ceil(position)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def median(values: list[float]) -> float | None:
    return percentile(values, 0.5)


def spearman_for(rows: list[dict[str, Any]], x_name: str, y_name: str) -> tuple[int, float | None]:
    pairs = [(number(row.get(x_name)), number(row.get(y_name))) for row in rows]
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    if len(pairs) < 3:
        return len(pairs), None
    return len(pairs), pearson(rank([x for x, _ in pairs]), rank([y for _, y in pairs]))


def pearson_for(rows: list[dict[str, Any]], x_name: str, y_name: str) -> tuple[int, float | None]:
    pairs = [(number(row.get(x_name)), number(row.get(y_name))) for row in rows]
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    return len(pairs), pearson([x for x, _ in pairs], [y for _, y in pairs])


def canonical_class(row: dict[str, Any]) -> str:
    name = str(row.get("function_class") or row.get("function_name") or "unknown").lower()
    if name.startswith("graph-bfs") or name.startswith("503.graph-bfs"):
        return "503.graph-bfs"
    if name.startswith("gzip"):
        return "gzip_compression"
    if name.startswith("pyaes"):
        return "pyaes"
    if name.startswith("hello"):
        return "hello"
    return name


def priority_queue_table(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    classes = ("hello", "503.graph-bfs", "gzip_compression", "pyaes")
    result = []
    for function_class in classes:
        group = [row for row in rows if canonical_class(row) == function_class]
        queue_pairs = [
            row for row in group
            if number(row.get("priority_weight")) is not None and number(row.get("pricing_queue_ms")) is not None
        ]
        aw_n, aw_corr = spearman_for(group, "active_weight", "pricing_queue_ms")
        dispatch = [v for row in group if (v := number(row.get("dispatch_overhead_ms"))) is not None]
        norm_queue = [v for row in group if (v := number(row.get("norm_queue_wait"))) is not None]
        priority = [v for row in group if (v := number(row.get("priority_weight"))) is not None]
        result.append({
            "function_class": function_class,
            "n": len(queue_pairs),
            "median_priority_weight": median(priority),
            "spearman_priority_vs_pricing_queue": spearman_for(group, "priority_weight", "pricing_queue_ms")[1],
            "spearman_active_weight_vs_pricing_queue": aw_corr,
            "active_weight_pair_n": aw_n,
            "median_norm_queue_wait": median(norm_queue),
            "p95_dispatch_overhead_ms": percentile(dispatch, 0.95),
        })
    return result


def concurrent_boot_table(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cold = [
        row for row in rows
        if row.get("was_cold") is True
        and number(row.get("cold_init_ms")) is not None
        and number(row.get("concurrent_boots_host")) is not None
    ]
    buckets = (
        ("1", lambda n: n == 1),
        ("2", lambda n: n == 2),
        ("3", lambda n: n == 3),
        ("4", lambda n: n == 4),
        ("5+", lambda n: n >= 5),
    )
    single = [number(row.get("cold_init_ms")) for row in cold if number(row.get("concurrent_boots_host")) == 1]
    single_median = median([v for v in single if v is not None])
    result = []
    for label, accepts in buckets:
        group = [row for row in cold if accepts(int(number(row.get("concurrent_boots_host")) or 0))]
        init = [v for row in group if (v := number(row.get("cold_init_ms"))) is not None]
        runnable = [v for row in group if (v := number(row.get("selected_pelt_runnable"))) is not None]
        gaps = [v for row in group if (v := number(row.get("pelt_runnable_gap"))) is not None]
        init_median = median(init)
        result.append({
            "concurrent_boots_host": label,
            "n": len(group),
            "median_cold_init_ms": init_median,
            "p95_cold_init_ms": percentile(init, 0.95),
            "init_stretch": init_median / single_median if init_median is not None and single_median else None,
            "mean_selected_pelt_runnable": statistics.mean(runnable) if runnable else None,
            "mean_pelt_runnable_gap": statistics.mean(gaps) if gaps else None,
            "pelt_sample_n": len(runnable),
        })
    return result


def domain_alignment_table(rows: list[dict[str, Any]], n_domains: int) -> list[dict[str, Any]]:
    priced = [row for row in rows if bool_value(row.get("selected")) is True and number(row.get("effective_rho")) is not None]
    cohorts: list[tuple[str, list[dict[str, Any]]]] = [
        (f"d{domain}", [r for r in priced if number(r.get("selected_domain")) == domain])
        for domain in range(n_domains)
    ]
    cohorts.append(("overall", priced))
    result = []
    for domain_name, group in cohorts:
        pelt_pairs = [row for row in group if number(row.get("selected_pelt_util")) is not None]
        biases = [number(row["effective_rho"]) - number(row["selected_pelt_util"]) for row in pelt_pairs]
        spread = [v for row in group if (v := number(row.get("domain_rho_spread"))) is not None]
        affinity = [v for row in group if (v := number(row.get("warm_affinity_gap"))) is not None]
        rho_stretch_n, rho_stretch = spearman_for(group, "effective_rho", "exec_stretch")
        util_stretch_n, util_stretch = spearman_for(group, "selected_pelt_util", "exec_stretch")
        gap_stretch_n, gap_stretch = spearman_for(group, "pelt_runnable_gap", "exec_stretch")
        _, rho_util_pearson = pearson_for(group, "effective_rho", "selected_pelt_util")
        _, rho_util_spearman = spearman_for(group, "effective_rho", "selected_pelt_util")
        result.append({
            "domain": domain_name,
            "n": len(group),
            "pelt_pair_n": len(pelt_pairs),
            "pearson_selected_rho_vs_pelt_util": rho_util_pearson,
            "spearman_selected_rho_vs_pelt_util": rho_util_spearman,
            "mean_bias_selected_rho_minus_pelt_util": statistics.mean(biases) if biases else None,
            "spearman_selected_rho_vs_exec_stretch": rho_stretch,
            "rho_stretch_pair_n": rho_stretch_n,
            "spearman_selected_pelt_util_vs_exec_stretch": util_stretch,
            "pelt_stretch_pair_n": util_stretch_n,
            "spearman_pelt_runnable_gap_vs_exec_stretch": gap_stretch,
            "gap_stretch_pair_n": gap_stretch_n,
            "median_domain_rho_spread": median(spread),
            "p95_domain_rho_spread": percentile(spread, 0.95),
            "median_warm_affinity_gap": median(affinity),
            "p95_warm_affinity_gap": percentile(affinity, 0.95),
        })
    return result


def grouped_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[row.get("function_class") or "unknown"].append(row)
    metrics = [
        "total_price", "toll_to_benefit", "net_gain", "benefit_B_i",
        "effective_rho", "sigma_d", "p_d",
        "client_e2e_ms", "worker_e2e_ms", "function_exec_ms",
    ]
    summary: list[dict[str, Any]] = []
    for name, group in sorted(groups.items()):
        item: dict[str, Any] = {"function_class": name, "invocations_with_price": len(group)}
        for metric in metrics:
            values = [v for r in group if (v := number(r.get(metric))) is not None]
            item[f"{metric}_n"] = len(values)
            item[f"{metric}_mean"] = statistics.mean(values) if values else None
            item[f"{metric}_p50"] = percentile(values, 0.50)
            item[f"{metric}_p95"] = percentile(values, 0.95)
        summary.append(item)
    return summary


def coverage_by_function(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[row.get("function_class") or "unknown"].append(row)
    result = []
    for function_class, group in sorted(groups.items()):
        priced = sum(row.get("selected") in (True, "True", "true", 1, "1") for row in group)
        result.append({
            "function_class": function_class,
            "csv_invocations": len(group),
            "priced_invocations": priced,
            "unpriced_invocations": len(group) - priced,
            "price_coverage_fraction": priced / len(group) if group else None,
        })
    return result


def timeseries_rows(rows: list[dict[str, Any]], bin_seconds: float) -> list[dict[str, Any]]:
    bins: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    width_ms = bin_seconds * 1000.0
    for row in rows:
        decision_ms = number(row.get("decision_time_ms"))
        if decision_ms is None:
            continue
        key = (row.get("function_class") or "unknown", int(decision_ms // width_ms))
        bins[key].append(row)
    metrics = [
        "total_price", "toll_to_benefit", "net_gain", "benefit_B_i",
        "effective_rho", "client_e2e_ms", "worker_e2e_ms", "function_exec_ms",
    ]
    out: list[dict[str, Any]] = []
    for (function_class, bin_id), group in sorted(bins.items(), key=lambda p: (p[0][1], p[0][0])):
        row: dict[str, Any] = {
            "function_class": function_class,
            "bin_start_seconds": bin_id * bin_seconds,
            "bin_end_seconds": (bin_id + 1) * bin_seconds,
            "invocations": len(group),
        }
        for metric in metrics:
            values = [v for r in group if (v := number(r.get(metric))) is not None]
            row[f"{metric}_mean"] = statistics.mean(values) if values else None
            row[f"{metric}_p50"] = percentile(values, 0.50)
        out.append(row)
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fmt(value: Any) -> str:
    n = number(value)
    return "n/a" if n is None else f"{n:.4g}"


def write_report(
    path: Path,
    run_dir: Path,
    rows: list[dict[str, Any]],
    all_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
    attribution_rows: list[dict[str, Any]],
    correlations: list[dict[str, Any]],
    coverage: dict[str, Any],
    bin_seconds: float,
    n_domains: int,
) -> None:
    lines = [
        f"# PostedPrice (SMC) price & attribution analysis: {run_dir.name}",
        "",
        "Candidate-domain prices are emitted at **dispatch time** and joined to invocation CSV rows by exact `tid`.",
        "The selected per-invocation row corresponds to `actual_domain` returned by `acquire_cores`. All candidate evaluations are saved in `price_candidates.csv`.",
        "",
        "## Coverage",
        "",
        f"- Invocation CSV rows: {coverage['csv_invocations']}",
        f"- Price candidate records: {coverage['price_candidate_records']}",
        f"- Invocations with dispatch price: {coverage['priced_invocations']}",
        f"- Express-lane (`hello`) dispatches: {coverage['express_dispatches']}",
        f"- Price rows joined to CSV: {coverage['joined_invocations']}",
        f"- Selected price rows with client E2E: {coverage['with_client_e2e']}",
        f"- Selected price rows with worker E2E: {coverage['with_worker_e2e']}",
        f"- Selected price rows with four timing fields: {coverage['with_timing_breakdown']}",
        f"- Candidate records matched with dispatch-time PELT: {coverage['candidate_price_pelt_matches']}",
        f"- Matched cold boot start/completion pairs: {coverage['matched_cold_boots']}",
        "",
        "## Per-domain dispatch counts and 1:1 attribution check",
        "",
        f"- Checked dispatches (`PostedPrice` `actual_domain` vs FineSched `domain_id`): **{coverage['attribution_checked_n']}**",
        f"- Exact 1:1 matches: **{coverage['attribution_matched_n']}** ({fmt((coverage.get('attribution_match_fraction') or 0.0) * 100.0)}%)",
        f"- Attribution mismatches: **{coverage['attribution_mismatch_n']}**",
        f"- Atomic capacity spills (`actual_domain != ranked_domain`): **{coverage['spill_count']}**",
        "",
        "| Domain | Priced dispatches | Express dispatches | Total PostedPrice | Share % | FineSched assigned | FineSched % | Spilled into | 1:1 matched | 1:1 mismatched |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in attribution_rows:
        lines.append(
            f"| {row['domain']} | {row['priced_dispatches']} | {row['express_dispatches']} | "
            f"{row['total_posted_price_dispatches']} | {fmt(row['posted_price_share_pct'])}% | "
            f"{row['finesched_assigned_dispatches']} | {fmt(row['finesched_share_pct'])}% | "
            f"{row['spilled_into_domain']} | {row['matched_1to1']} | {row['mismatched_1to1']} |"
        )

    lines += [
        "",
        "### Price coverage by function",
        "",
        "| Function class | CSV invocations | Priced | Express / unpriced | Coverage |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in coverage_rows:
        lines.append(
            f"| {row['function_class']} | {row['csv_invocations']} | {row['priced_invocations']} | "
            f"{row['unpriced_invocations']} | "
            f"{fmt(number(row['price_coverage_fraction']) * 100 if row['price_coverage_fraction'] is not None else None)}% |"
        )

    lines += [
        "",
        "## Per-function snapshot",
        "",
        "| Function class | n priced | toll/benefit mean | total price mean | net gain mean | client E2E p50 ms | worker E2E p50 ms | function exec p50 ms |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for group in grouped_summary(rows):
        lines.append(
            f"| {group['function_class']} | {group['invocations_with_price']} | "
            f"{fmt(group.get('toll_to_benefit_mean'))} | {fmt(group.get('total_price_mean'))} | "
            f"{fmt(group.get('net_gain_mean'))} | {fmt(group.get('client_e2e_ms_p50'))} | "
            f"{fmt(group.get('worker_e2e_ms_p50'))} | {fmt(group.get('function_exec_ms_p50'))} |"
        )

    lines += [
        "",
        "## Strongest descriptive correlations",
        "",
        "| Cohort | Price feature | Latency | n | Pearson r | Spearman r |",
        "|---|---|---|---:|---:|---:|",
    ]
    ranked = [r for r in correlations if r["cohort"] == "all" and r["n"] >= 10 and r["spearman_r"] is not None]
    ranked.sort(key=lambda r: abs(r["spearman_r"]), reverse=True)
    for r in ranked[:20]:
        lines.append(
            f"| all | {r['price_feature']} | {r['latency_metric']} | {r['n']} | "
            f"{fmt(r['pearson_r'])} | {fmt(r['spearman_r'])} |"
        )

    lines += [
        "",
        "## Table A — Within-class priority differentiation and queue isolation",
        "",
        "| Function class | n | Median priority weight | Spearman(priority, pricing queue) | Spearman(active weight, pricing queue) | Active-weight pairs | Median normalized queue wait | p95 dispatch overhead (ms) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in priority_queue_table(all_rows):
        lines.append(
            f"| {row['function_class']} | {row['n']} | {fmt(row['median_priority_weight'])} | "
            f"{fmt(row['spearman_priority_vs_pricing_queue'])} | "
            f"{fmt(row['spearman_active_weight_vs_pricing_queue'])} | "
            f"{row['active_weight_pair_n']} | {fmt(row['median_norm_queue_wait'])} | "
            f"{fmt(row['p95_dispatch_overhead_ms'])} |"
        )

    lines += [
        "",
        "## Table B — Concurrent cold-boot contention curve",
        "",
        "| Concurrent host boots | n | Median cold init (ms) | p95 cold init (ms) | Init stretch | Mean selected PELT runnable | Mean PELT runnable gap | PELT sample n |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in concurrent_boot_table(all_rows):
        lines.append(
            f"| {row['concurrent_boots_host']} | {row['n']} | {fmt(row['median_cold_init_ms'])} | "
            f"{fmt(row['p95_cold_init_ms'])} | {fmt(row['init_stretch'])} | "
            f"{fmt(row['mean_selected_pelt_runnable'])} | {fmt(row['mean_pelt_runnable_gap'])} | "
            f"{row['pelt_sample_n']} |"
        )

    lines += [
        "",
        "## Table C — Effective rho and PELT alignment by domain",
        "",
        "| Domain | n priced | PELT pairs | Pearson(rho, PELT util) | Spearman(rho, PELT util) | Mean bias (rho - PELT util) | Spearman(rho, stretch) | n | Spearman(PELT util, stretch) | n | Spearman(runnable gap, stretch) | n | Median / p95 rho spread | Median / p95 warm affinity gap |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in domain_alignment_table(rows, n_domains):
        spread = f"{fmt(row['median_domain_rho_spread'])} / {fmt(row['p95_domain_rho_spread'])}"
        affinity = f"{fmt(row['median_warm_affinity_gap'])} / {fmt(row['p95_warm_affinity_gap'])}"
        lines.append(
            f"| {row['domain']} | {row['n']} | {row['pelt_pair_n']} | {fmt(row['pearson_selected_rho_vs_pelt_util'])} | "
            f"{fmt(row['spearman_selected_rho_vs_pelt_util'])} | {fmt(row['mean_bias_selected_rho_minus_pelt_util'])} | "
            f"{fmt(row['spearman_selected_rho_vs_exec_stretch'])} | {row['rho_stretch_pair_n']} | "
            f"{fmt(row['spearman_selected_pelt_util_vs_exec_stretch'])} | {row['pelt_stretch_pair_n']} | "
            f"{fmt(row['spearman_pelt_runnable_gap_vs_exec_stretch'])} | {row['gap_stretch_pair_n']} | {spread} | {affinity} |"
        )

    lines += [
        "",
        "## Figures",
        "",
        f"- `effective_rho_vs_pelt.png`: predicted `effective_rho` vs. kernel `pelt_util` (Spearman r={fmt(coverage.get('candidate_rho_pelt_spearman'))}, MAE={fmt(coverage.get('candidate_rho_pelt_mae'))}, RMSE={fmt(coverage.get('candidate_rho_pelt_rmse'))}).",
        f"- `unit_congestion_price_vs_pelt.png`: linear-scale dimensionless `toll_to_benefit = total_price / benefit_B_i` ({coverage.get('candidate_toll_benefit_points', 0):,} points) and pure domain unit price (`sigma_d` and `p_d` without multiplying by `w_0`) vs. dispatch-time `pelt_util`.",
        f"- `toll_to_benefit_vs_pelt.png`: standalone linear-scale dimensionless `toll_to_benefit` vs. `pelt_util`.",
        f"- `pelt_marginal_price_by_domain.png`: time-ordered `pelt_util` and `total_price` per domain.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def binned_median_band(
    rows: list[dict[str, Any]], x_field: str, y_field: str, bins: int = 24
) -> tuple[list[float], list[float], list[float], list[float]]:
    pairs = [(number(row.get(x_field)), number(row.get(y_field))) for row in rows]
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    if not pairs:
        return [], [], [], []
    x_min, x_max = min(x for x, _ in pairs), max(x for x, _ in pairs)
    if x_min == x_max:
        ys = [y for _, y in pairs]
        return [x_min], [median(ys) or 0.0], [percentile(ys, 0.25) or 0.0], [percentile(ys, 0.75) or 0.0]
    grouped: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for x, y in pairs:
        bin_id = min(bins - 1, int((x - x_min) / (x_max - x_min) * bins))
        grouped[bin_id].append((x, y))
    min_bin_count = max(25, len(pairs) // 400)
    merged_groups: list[list[tuple[float, float]]] = []
    pending: list[tuple[float, float]] = []
    for _, group in sorted(grouped.items()):
        pending.extend(group)
        if len(pending) >= min_bin_count:
            merged_groups.append(pending)
            pending = []
    if pending:
        if merged_groups:
            merged_groups[-1].extend(pending)
        else:
            merged_groups.append(pending)
    centers, medians, lows, highs = [], [], [], []
    for group in merged_groups:
        y_values = [y for _, y in group]
        centers.append(statistics.mean(x for x, _ in group))
        medians.append(median(y_values) or 0.0)
        lows.append(percentile(y_values, 0.25) or 0.0)
        highs.append(percentile(y_values, 0.75) or 0.0)
    return centers, medians, lows, highs


def plot_timeseries(path: Path, rows: list[dict[str, Any]]) -> list[str]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    metrics = [
        ("toll_to_benefit_mean", "Mean toll / benefit"),
        ("total_price_mean", "Mean total price (welfare units)"),
        ("effective_rho_mean", "Mean effective domain rho"),
        ("client_e2e_ms_mean", "Mean client E2E (ms)"),
    ]
    classes = sorted({r["function_class"] for r in rows})
    created: list[str] = []
    for function_class in classes:
        class_rows = [r for r in rows if r["function_class"] == function_class]
        fig, axes = plt.subplots(len(metrics), 1, figsize=(11, 9), sharex=True, constrained_layout=True)
        for ax, (metric, title) in zip(axes, metrics):
            group = [r for r in class_rows if number(r.get(metric)) is not None]
            group.sort(key=lambda r: number(r["bin_start_seconds"]) or 0.0)
            if group:
                ax.plot(
                    [r["bin_start_seconds"] for r in group],
                    [r[metric] for r in group],
                    marker=".",
                    linewidth=1,
                    label=f"PostedPrice ({function_class})",
                )
            ax.set_ylabel(title)
            ax.grid(True, alpha=0.25)
        axes[-1].set_xlabel("Seconds since worker start")
        fig.suptitle(f"PostedPrice prices and observed E2E: {function_class}")
        plot_path = path.with_name(f"{path.stem}-{function_class}{path.suffix}")
        fig.savefig(plot_path, dpi=150)
        plt.close(fig)
        created.append(plot_path.name)
    return created


def plot_pelt_marginal_price_by_domain(
    path: Path, rows: list[dict[str, Any]], n_domains: int
) -> list[str]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    n_dom = max(1, int(n_domains))
    ncols = 1 if n_dom == 1 else 2
    nrows = math.ceil(n_dom / ncols)
    decision_times = [ts for row in rows if (ts := timestamp_seconds(row.get("log_timestamp"))) is not None]
    time_origin = min(decision_times) if decision_times else 0.0
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(6.2 * ncols, 4.3 * nrows), sharex=True, sharey=True, squeeze=False
    )
    toll_axes: list[tuple[int, int, Any]] = []
    for idx, axis in enumerate(axes.flat):
        if idx >= n_dom:
            axis.set_visible(False)
            continue
        domain = idx
        r_idx, c_idx = divmod(idx, ncols)
        group = []
        for row in rows:
            timestamp = timestamp_seconds(row.get("log_timestamp"))
            if (
                timestamp is not None
                and number(row.get("selected_domain")) == domain
                and number(row.get("selected_pelt_util")) is not None
                and number(row.get("total_price")) is not None
            ):
                group.append((timestamp, row))
        group.sort(key=lambda pair: pair[0])
        elapsed = [timestamp - time_origin for timestamp, _ in group]
        pelt = [number(row.get("selected_pelt_util")) for _, row in group]
        toll = [number(row.get("total_price")) for _, row in group]
        axis.plot(elapsed, pelt, color="tab:blue", linewidth=1.1, label="PELT utilization")
        toll_axis = axis.twinx()
        toll_axes.append((r_idx, c_idx, toll_axis))
        toll_axis.plot(elapsed, toll, color="tab:orange", linewidth=1.1, alpha=0.85, label="PostedPrice total_price")
        is_rightmost_in_row = (c_idx == ncols - 1) or (idx == n_dom - 1)
        if not is_rightmost_in_row:
            toll_axis.tick_params(axis="y", right=False, labelright=False)
        if toll:
            p99_t = percentile([v for v in toll if v is not None], 0.99) or 1.0
            toll_axis.set_ylim(0.0, max(1.0, p99_t * 1.15))
        corr = spearman_for([row for _, row in group], "selected_pelt_util", "total_price")[1]
        axis.set_title(f"d{domain}: n={len(group)}, Spearman={fmt(corr)}")
        axis.set_ylim(bottom=0.0)
        axis.grid(True, alpha=0.25)
    for c_idx in range(ncols):
        last_r = nrows - 1
        if last_r * ncols + c_idx >= n_dom and last_r > 0:
            last_r -= 1
        axes[last_r, c_idx].set_xlabel("Dispatch time since run start (s)")
        axes[last_r, c_idx].tick_params(axis="x", labelbottom=True)
    for r_idx in range(nrows):
        if r_idx * ncols < n_dom:
            axes[r_idx, 0].set_ylabel("Actual-domain PELT utilization (0–1)", color="tab:blue")
    for r_idx, c_idx, toll_axis in toll_axes:
        idx = r_idx * ncols + c_idx
        if (c_idx == ncols - 1) or (idx == n_dom - 1):
            toll_axis.set_ylabel("PostedPrice total price (welfare units)", color="tab:orange")
    fig.legend(
        [plt.Line2D([], [], color="tab:blue"), plt.Line2D([], [], color="tab:orange")],
        ["PELT utilization", "PostedPrice total price"],
        loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.95),
    )
    fig.suptitle("PostedPrice: Actual-domain PELT utilization and dispatch price", y=0.99)
    fig.subplots_adjust(top=0.87, bottom=0.11, left=0.08, right=0.92, hspace=0.25, wspace=0.12)
    plot_path = path.with_name("pelt_marginal_price_by_domain.png")
    fig.savefig(plot_path, dpi=160)
    plt.close(fig)
    return [plot_path.name]


def plot_toll_to_benefit_vs_pelt(
    path: Path, candidates: list[dict[str, Any]], rho_knee: float | None
) -> list[str]:
    """Plot linear-scale dimensionless toll/benefit vs. dispatch-time PELT."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    x_field = "candidate_domain_pelt_util"
    fig, axis = plt.subplots(1, 1, figsize=(8.5, 5.5))
    styles = (
        (False, "tab:blue", "PostedPrice (rejected)"),
        (True, "tab:orange", "PostedPrice (actual)"),
    )
    plotted = [
        row for row in candidates
        if number(row.get(x_field)) is not None and number(row.get("toll_to_benefit")) is not None
    ]
    ratios = [v for row in plotted if (v := number(row.get("toll_to_benefit"))) is not None]
    for selected, color, label in styles:
        group = [row for row in plotted if bool_value(row.get("is_selected")) is selected]
        axis.scatter(
            [number(row.get(x_field)) for row in group],
            [number(row.get("toll_to_benefit")) for row in group],
            s=22 if selected else 9,
            alpha=0.78 if selected else 0.18,
            color=color,
            marker="D" if selected else ".",
            linewidths=0.0,
            label=label,
            zorder=3 if selected else 1,
        )
    centers, medians, lows, highs = binned_median_band(plotted, x_field, "toll_to_benefit")
    if centers:
        axis.fill_between(centers, lows, highs, color="black", alpha=0.14, label="25th–75th pct", zorder=2)
        axis.plot(centers, medians, color="black", linewidth=2.0, label="PostedPrice median", zorder=4)
    if rho_knee is not None:
        axis.axvline(rho_knee, color="black", linestyle="--", linewidth=1.2, alpha=0.7, label="rho_knee")
    axis.axhline(1.0, color="gray", linestyle=":", linewidth=1.2, label="Break-even (P_i = B_i)")
    corr = spearman_for(plotted, x_field, "toll_to_benefit")[1]
    p99 = percentile(ratios, 0.99)
    y_max = max(1.5, p99 * 1.1) if p99 is not None else 1.5
    clipped = sum(value > y_max for value in ratios)
    axis.set_ylim(0.0, y_max)
    axis.set_title(f"n={len(plotted):,}, Spearman r={fmt(corr)}" + (f", clipped={clipped:,}" if clipped else ""))
    axis.set_xlabel("Candidate-domain dispatch-time PELT utilization")
    axis.set_ylabel("Dimensionless toll-to-benefit (P_i / B_i)")
    axis.grid(True, alpha=0.25)
    handles, labels = axis.get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.99))
    fig.suptitle("PostedPrice: Dimensionless toll-to-benefit vs. kernel PELT utilization", y=1.05)
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return [path.name]


def compute_rho_pelt_metrics(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    pairs = [
        (p, r)
        for row in candidates
        if (p := number(row.get("candidate_domain_pelt_util"))) is not None
        and (r := number(row.get("effective_rho"))) is not None
    ]
    if not pairs:
        return {
            "candidate_rho_pelt_n": 0,
            "candidate_rho_pelt_spearman": None,
            "candidate_rho_pelt_pearson": None,
            "candidate_rho_pelt_mae": None,
            "candidate_rho_pelt_rmse": None,
        }
    xs = [p for p, _ in pairs]
    ys = [r for _, r in pairs]
    abs_errs = [abs(y - x) for x, y in pairs]
    sq_errs = [(y - x) ** 2 for x, y in pairs]
    spearman = pearson(rank(xs), rank(ys)) if len(pairs) >= 3 else None
    return {
        "candidate_rho_pelt_n": len(pairs),
        "candidate_rho_pelt_spearman": spearman,
        "candidate_rho_pelt_pearson": pearson(xs, ys),
        "candidate_rho_pelt_mae": statistics.mean(abs_errs),
        "candidate_rho_pelt_rmse": math.sqrt(statistics.mean(sq_errs)),
    }


def plot_effective_rho_vs_pelt(path: Path, candidates: list[dict[str, Any]]) -> list[str]:
    """Plot predicted `effective_rho` vs. kernel `pelt_util` with Spearman r, MAE, and RMSE."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    x_field = "candidate_domain_pelt_util"
    plotted = [
        row for row in candidates
        if number(row.get(x_field)) is not None and number(row.get("effective_rho")) is not None
    ]
    if not plotted:
        return []

    metrics = compute_rho_pelt_metrics(plotted)
    fig, axis = plt.subplots(1, 1, figsize=(8.5, 5.8))
    styles = (
        (False, "tab:blue", "PostedPrice (rejected)"),
        (True, "tab:orange", "PostedPrice (actual)"),
    )
    for selected, color, label in styles:
        group = [row for row in plotted if bool_value(row.get("is_selected")) is selected]
        axis.scatter(
            [number(row.get(x_field)) for row in group],
            [number(row.get("effective_rho")) for row in group],
            s=22 if selected else 9,
            alpha=0.75 if selected else 0.16,
            color=color,
            marker="D" if selected else ".",
            linewidths=0.0,
            label=label,
            zorder=3 if selected else 1,
        )

    centers, medians, lows, highs = binned_median_band(plotted, x_field, "effective_rho")
    if centers:
        axis.fill_between(centers, lows, highs, color="black", alpha=0.14, label="25th–75th pct", zorder=2)
        axis.plot(centers, medians, color="black", linewidth=2.1, label="effective_rho median", zorder=5)

    has_distinct_l1 = any(
        abs((number(r.get("l1_rho")) or 0.0) - (number(r.get("effective_rho")) or 0.0)) > 1e-6
        for r in plotted
        if number(r.get("l1_rho")) is not None
    )
    if has_distinct_l1:
        l1_c, l1_m, _, _ = binned_median_band(plotted, x_field, "l1_rho")
        if l1_c:
            axis.plot(l1_c, l1_m, color="tab:green", linestyle="--", linewidth=1.6, label="l1_rho median", zorder=4)

    xs = [number(row.get(x_field)) for row in plotted]
    ys = [number(row.get("effective_rho")) for row in plotted]
    x_max = max([v for v in xs if v is not None] or [1.0])
    p99_y = percentile([v for v in ys if v is not None], 0.99) or 1.0
    lim = max(1.05, x_max * 1.05, p99_y * 1.1)
    axis.plot([0.0, min(1.2, lim)], [0.0, min(1.2, lim)], color="gray", linestyle=":", linewidth=1.3, label="1:1 parity (rho = PELT)")
    axis.set_xlim(0.0, max(1.02, x_max * 1.05))
    axis.set_ylim(0.0, lim)
    axis.set_title(
        f"n={metrics['candidate_rho_pelt_n']:,}, Spearman r={fmt(metrics['candidate_rho_pelt_spearman'])}, "
        f"MAE={fmt(metrics['candidate_rho_pelt_mae'])}, RMSE={fmt(metrics['candidate_rho_pelt_rmse'])}"
    )
    axis.set_xlabel("Candidate-domain dispatch-time PELT utilization")
    axis.set_ylabel("Predicted effective_rho")
    axis.grid(True, alpha=0.25)
    handles, labels = axis.get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.99))
    fig.suptitle("PostedPrice: Predicted effective_rho vs. kernel PELT utilization", y=1.05)
    fig.tight_layout(rect=(0, 0, 1, 0.89))
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return [path.name]


def plot_unit_congestion_price_vs_pelt(
    path: Path, candidates: list[dict[str, Any]], rho_knee: float | None
) -> list[str]:
    """Plot linear-scale dimensionless toll_to_benefit (left) and pure domain unit price sigma_d & p_d (right) vs. pelt_util."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    x_field = "candidate_domain_pelt_util"
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(13.8, 5.7))
    styles = (
        (False, "tab:blue", "PostedPrice (rejected)"),
        (True, "tab:orange", "PostedPrice (actual)"),
    )

    # Left panel: dimensionless toll_to_benefit = total_price / benefit_B_i
    left_rows = [
        row for row in candidates
        if number(row.get(x_field)) is not None and number(row.get("toll_to_benefit")) is not None
    ]
    left_vals = [v for row in left_rows if (v := number(row.get("toll_to_benefit"))) is not None and v >= 0]
    for selected, color, label in styles:
        group = [row for row in left_rows if bool_value(row.get("is_selected")) is selected]
        ax_left.scatter(
            [number(row.get(x_field)) for row in group],
            [number(row.get("toll_to_benefit")) for row in group],
            s=22 if selected else 9,
            alpha=0.78 if selected else 0.18,
            color=color,
            marker="D" if selected else ".",
            linewidths=0.0,
            label=label,
            zorder=3 if selected else 1,
        )
    centers, medians, lows, highs = binned_median_band(left_rows, x_field, "toll_to_benefit")
    if centers:
        ax_left.fill_between(centers, lows, highs, color="black", alpha=0.14, label="25th–75th pct", zorder=2)
        ax_left.plot(centers, medians, color="black", linewidth=2.0, label="PostedPrice median", zorder=4)
    ax_left.axhline(1.0, color="gray", linestyle=":", linewidth=1.2, label="Break-even (1.0)")
    if rho_knee is not None:
        ax_left.axvline(rho_knee, color="black", linestyle="--", linewidth=1.2, alpha=0.7, label="rho_knee")
    corr_left = spearman_for(left_rows, x_field, "toll_to_benefit")[1]
    p99_left = percentile(left_vals, 0.99)
    y_max_left = max(1.5, p99_left * 1.1) if p99_left is not None else 1.5
    clipped_left = sum(v > y_max_left for v in left_vals)
    ax_left.set_ylim(0.0, y_max_left)
    ax_left.set_title(
        f"n={len(left_rows):,}, Spearman r={fmt(corr_left)}"
        + (f", clipped={clipped_left:,}" if clipped_left else "")
    )
    ax_left.set_xlabel("Candidate-domain dispatch-time PELT utilization")
    ax_left.set_ylabel("Dimensionless toll-to-benefit (P_i / B_i)")
    ax_left.grid(True, alpha=0.25)

    # Right panel: pure domain unit price (`sigma_d` and `p_d`, free of shopper `w_0`)
    right_rows = [
        row for row in candidates
        if number(row.get(x_field)) is not None
        and number(row.get("sigma_d")) is not None
        and number(row.get("p_d")) is not None
    ]
    sigma_vals = [v for row in right_rows if (v := number(row.get("sigma_d"))) is not None and v >= 0]
    pd_vals = [v for row in right_rows if (v := number(row.get("p_d"))) is not None and v >= 0]

    ax_right.scatter(
        [number(row.get(x_field)) for row in right_rows],
        [number(row.get("sigma_d")) for row in right_rows],
        s=10,
        alpha=0.20,
        color="tab:blue",
        marker=".",
        linewidths=0.0,
        label="sigma_d (samples)",
        zorder=1,
    )
    sig_c, sig_m, sig_lo, sig_hi = binned_median_band(right_rows, x_field, "sigma_d")
    if sig_c:
        ax_right.fill_between(sig_c, sig_lo, sig_hi, color="tab:blue", alpha=0.16, zorder=2)
        ax_right.plot(sig_c, sig_m, color="tab:blue", linewidth=2.2, label="sigma_d median", zorder=4)
    p99_sig = percentile(sigma_vals, 0.99)
    y_max_sig = max(0.2, p99_sig * 1.15) if p99_sig is not None and p99_sig > 0 else 1.0
    ax_right.set_ylim(0.0, y_max_sig)
    ax_right.set_xlabel("Candidate-domain dispatch-time PELT utilization")
    ax_right.set_ylabel("Pure unit self-slowdown sigma_d", color="tab:blue")
    ax_right.tick_params(axis="y", labelcolor="tab:blue")
    ax_right.grid(True, alpha=0.25)

    ax_pd = ax_right.twinx()
    ax_pd.scatter(
        [number(row.get(x_field)) for row in right_rows],
        [number(row.get("p_d")) for row in right_rows],
        s=10,
        alpha=0.18,
        color="tab:red",
        marker=".",
        linewidths=0.0,
        label="p_d (samples)",
        zorder=1,
    )
    pd_c, pd_m, pd_lo, pd_hi = binned_median_band(right_rows, x_field, "p_d")
    if pd_c:
        ax_pd.fill_between(pd_c, pd_lo, pd_hi, color="tab:red", alpha=0.12, zorder=2)
        ax_pd.plot(pd_c, pd_m, color="tab:red", linestyle="--", linewidth=2.1, label="p_d median", zorder=5)
    p99_pd = percentile(pd_vals, 0.99)
    y_max_pd = max(0.05, p99_pd * 1.15) if p99_pd is not None and p99_pd > 0 else 1.0
    ax_pd.set_ylim(0.0, y_max_pd)
    ax_pd.set_ylabel("Pure marginal externality rate p_d (weight/ms)", color="tab:red")
    ax_pd.tick_params(axis="y", labelcolor="tab:red")

    corr_sig = spearman_for(right_rows, x_field, "sigma_d")[1]
    corr_pd = spearman_for(right_rows, x_field, "p_d")[1]
    ax_right.set_title(f"Pure domain price: r(sigma_d)={fmt(corr_sig)}, r(p_d)={fmt(corr_pd)}")
    h_r1, l_r1 = ax_right.get_legend_handles_labels()
    h_r2, l_r2 = ax_pd.get_legend_handles_labels()
    if h_r1 or h_r2:
        ax_pd.legend(h_r1 + h_r2, l_r1 + l_r2, loc="upper left", frameon=True, framealpha=0.85, fontsize=8.5)

    handles, labels = ax_left.get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=5, frameon=False, bbox_to_anchor=(0.5, 0.99))
    fig.suptitle("PostedPrice: Linear-scale congestion price & pure domain unit price vs. PELT", y=1.03)
    fig.tight_layout(rect=(0, 0, 1, 0.89))
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return [path.name]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="Experiment result directory")
    parser.add_argument("--worker-log", help="Worker JSONL log (auto-detected by default)")
    parser.add_argument("--invocations", help="Load-generator output CSV (auto-detected by default)")
    parser.add_argument("--metadata", help="Trace metadata CSV with static warm_dur_ms fallback")
    parser.add_argument("--output-dir", type=Path, help="Output directory (default: sibling RUN_NAME-congestion-price-analysis)")
    parser.add_argument("--bin-seconds", type=float, default=5.0, help="Time-series bin width (default: 5)")
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    if args.bin_seconds <= 0:
        parser.error("--bin-seconds must be positive")
    worker, invocation_csv = locate_files(run_dir, args.worker_log, args.invocations)
    output_dir = (args.output_dir or run_dir.parent / f"{run_dir.name}-congestion-price-analysis").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    (
        prices,
        express_dispatches,
        candidates,
        worker_extras,
        pelt_samples,
        pelt_telemetry,
        boot_intervals,
        dispatch_times,
        assigned_domains,
    ) = read_worker_log(worker)
    invocation_rows = load_invocations(invocation_csv)
    if args.metadata:
        metadata_path = Path(args.metadata)
    else:
        metadata_candidates = [run_dir / "trace_metadata.csv"]
        metadata_candidates.extend(sorted(run_dir.glob("*meta*.csv")))
        metadata_candidates.extend(sorted(run_dir.glob("*metadata*.csv")))
        metadata_path = next((candidate for candidate in metadata_candidates if candidate.is_file()), run_dir / "trace_metadata.csv")
    warm_runtimes = load_warm_runtime_metadata(metadata_path if metadata_path.is_file() else None)
    rho_knee, cores_per_domain, cfg_n_domains = load_run_plot_config(run_dir)
    if cfg_n_domains is not None:
        n_domains = cfg_n_domains
    else:
        observed_doms = [
            int(d)
            for d in (
                [number(r.get("candidate_domain")) for r in candidates]
                + [number(r.get("actual_domain", r.get("final_domain"))) for r in prices.values()]
                + list(assigned_domains.values())
            )
            if d is not None and d >= 0
        ]
        n_domains = (max(observed_doms) + 1) if observed_doms else 1
    function_by_tid = {row["tid"]: row.get("function_name", "") for row in invocation_rows}

    for interval in boot_intervals:
        selected = prices.get(interval["tid"], {})
        selected_domain = number(selected.get("actual_domain", selected.get("final_domain")))
        if selected_domain is not None:
            interval["domain"] = int(selected_domain)
        elif interval.get("domain") is None:
            interval["domain"] = assigned_domains.get(interval["tid"])

    joined: list[dict[str, Any]] = []
    for invocation in invocation_rows:
        tid = invocation["tid"]
        price = prices.get(tid)
        extra = worker_extras.get(tid, {})
        row = dict(invocation)
        row.update({key: price.get(key) for key in price} if price else {})
        row.update(extra)
        row["function_class"] = (
            (price or {}).get("function_class")
            or express_dispatches.get(tid, {}).get("function_class")
            or invocation.get("function_name", "unknown").split("-")[0].lower()
        )
        for src in ("pricing_queue_ms", "container_acquire_ms", "exec_ms", "post_overhead_ms"):
            row[src] = number(extra.get(src))
        joined.append(row)

    add_derived_metrics(
        joined, candidates, express_dispatches, pelt_samples,
        warm_runtimes, boot_intervals, dispatch_times, assigned_domains,
    )
    priority_weight_by_tid = {
        row["tid"]: value
        for row in joined
        if (value := number(row.get("priority_weight"))) is not None
    }
    candidate_price_pelt_matches = enrich_candidate_records(
        candidates, pelt_telemetry, warm_runtimes, function_by_tid,
        priority_weight_by_tid, cores_per_domain,
    )

    attribution_rows, attribution_summary = compute_attribution_summary(
        joined, prices, express_dispatches, assigned_domains, n_domains
    )

    joined_price_rows = [r for r in joined if r["tid"] in prices]
    correlations = correlation_rows(joined_price_rows)
    by_function = grouped_summary(joined_price_rows)
    coverage_functions = coverage_by_function(joined)
    time_rows = timeseries_rows(joined_price_rows, args.bin_seconds)

    write_csv(output_dir / "price_candidates.csv", candidates)
    write_csv(output_dir / "prices_per_invocation.csv", joined)
    write_csv(output_dir / "domain_dispatch_attribution.csv", attribution_rows)
    write_csv(output_dir / "price_correlations.csv", correlations)
    write_csv(output_dir / "price_by_function.csv", by_function)
    write_csv(output_dir / "price_coverage_by_function.csv", coverage_functions)
    write_csv(output_dir / "price_timeseries.csv", time_rows)

    rho_pelt_metrics = compute_rho_pelt_metrics(candidates)
    timing_fields = ("pricing_queue_ms", "container_acquire_ms", "exec_ms", "post_overhead_ms")
    coverage = {
        "csv_invocations": len(invocation_rows),
        "price_candidate_records": len(candidates),
        "priced_invocations": len(prices),
        "express_dispatches": len(express_dispatches),
        "joined_invocations": len(joined_price_rows),
        "with_client_e2e": sum(number(r.get("client_e2e_ms")) is not None for r in joined_price_rows),
        "with_worker_e2e": sum(number(r.get("worker_e2e_ms")) is not None for r in joined_price_rows),
        "with_timing_breakdown": sum(all(number(r.get(f)) is not None for f in timing_fields) for r in joined_price_rows),
        "with_runtime_estimate": sum(number(r.get("runtime_est_ms")) is not None for r in joined),
        "with_selected_pelt": sum(number(r.get("selected_pelt_util")) is not None for r in joined_price_rows),
        "matched_cold_boots": len(boot_intervals),
        "candidate_price_pelt_matches": candidate_price_pelt_matches,
        "candidate_toll_benefit_points": sum(number(row.get("toll_to_benefit")) is not None for row in candidates),
        "candidate_unit_price_points": sum(number(row.get("sigma_d")) is not None for row in candidates),
        **rho_pelt_metrics,
        **attribution_summary,
    }
    write_report(
        output_dir / "analysis.md",
        run_dir,
        joined_price_rows,
        joined,
        coverage_functions,
        attribution_rows,
        correlations,
        coverage,
        args.bin_seconds,
        n_domains,
    )
    plots = plot_timeseries(output_dir / "price_latency_timeseries.png", time_rows)
    plots.extend(plot_pelt_marginal_price_by_domain(output_dir / "price_latency_timeseries.png", joined_price_rows, n_domains))
    plots.extend(plot_effective_rho_vs_pelt(output_dir / "effective_rho_vs_pelt.png", candidates))
    plots.extend(plot_unit_congestion_price_vs_pelt(output_dir / "unit_congestion_price_vs_pelt.png", candidates, rho_knee))
    plots.extend(plot_toll_to_benefit_vs_pelt(output_dir / "toll_to_benefit_vs_pelt.png", candidates, rho_knee))
    (output_dir / "coverage.json").write_text(json.dumps({**coverage, "plots_created": plots}, indent=2) + "\n", encoding="utf-8")
    print(f"Analysis: {output_dir}")
    print(json.dumps({**coverage, "plots_created": plots}, indent=2))


if __name__ == "__main__":
    main()
