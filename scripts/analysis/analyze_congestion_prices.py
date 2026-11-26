#!/usr/bin/env python3
"""Join CongestionPricing decision prices to observed invocation latencies.

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
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


PRICE_FIELDS = [
    "rho", "r_min_ms", "sigma_warm", "delta_sigma_warm", "benefit",
    "warm_toll", "cold_toll", "queue_backlog_toll", "creation_credit",
    "io_overlap_credit", "total_marginal_toll", "net_gain",
]
LATENCY_FIELDS = [
    "function_exec_ms", "worker_duration_ms", "client_e2e_ms",
    "worker_e2e_ms", "dispatcher_queue_ms", "pricing_queue_ms",
    "container_acquire_ms", "exec_ms", "post_overhead_ms",
]
CORRELATION_FEATURES = [
    "total_marginal_toll", "net_gain", "benefit", "warm_toll",
    "cold_toll", "queue_backlog_toll", "creation_credit",
    "io_overlap_credit", "rho", "sigma_warm", "delta_sigma_warm",
]
CORRELATION_TARGETS = [
    "function_exec_ms", "worker_duration_ms", "client_e2e_ms",
    "worker_e2e_ms", "dispatcher_queue_ms", "pricing_queue_ms",
    "container_acquire_ms", "post_overhead_ms",
]


def number(value: Any) -> float | None:
    if value is None or value == "":
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
    direct = number(value)
    if direct is not None:
        return direct
    if value is None:
        return None
    match = re.search(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?", str(value))
    return number(match.group(0)) if match else None


def read_worker_log(path: Path) -> tuple[
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
                        # One pricing gate should select one candidate. Keep
                        # the first selected record if a malformed log repeats it.
                        selected_prices.setdefault(tid, row)
                elif message == "[congestion_pricing] invocation timing":
                    timings[tid] = dict(fields)
                elif message == "[finesched][core-metrics] sampled assigned domain":
                    sample_ts = timestamp_seconds(record.get("timestamp"))
                    pelt_util = logged_number(fields.get("domain_pelt_like_utilization_percent"))
                    runnable_load = logged_number(fields.get("runnable_load"))
                    domain_id = logged_number(fields.get("domain_id"))
                    pelt_samples[tid] = {
                        "domain_id": domain_id,
                        "selected_pelt_util": pelt_util,
                        "selected_pelt_runnable": runnable_load,
                    }
                    if pelt_samples[tid]["selected_pelt_util"] is not None:
                        pelt_samples[tid]["selected_pelt_util"] /= 100.0
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
                elif message in ("[finesched] domain assigned to function request", "[finesched] domain assigned to function cgroup"):
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
                        from datetime import datetime
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

    # Merge worker timing fields without losing complete/queue fields.
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
    return selected_prices, candidates, extras, pelt_samples, pelt_telemetry, boot_intervals, dispatch_times, assigned_domains


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


def load_run_plot_config(run_dir: Path) -> tuple[float | None, float | None]:
    """Return rho_knee and cores/domain from the run's saved worker config."""
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
        cp = config.get("invocation", {}).get("congestion_pricing", {})
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
    return rho_knee, cores_per_domain


def enrich_candidate_records(
    candidates: list[dict[str, Any]],
    telemetry: list[dict[str, Any]],
    warm_runtimes: dict[str, float],
    function_by_tid: dict[str, str],
    cores_per_domain: float | None,
    tolerance_ms: float = 50.0,
) -> int:
    """Nearest same-domain PELT join, equivalent to merge_asof(nearest)."""
    telemetry_by_domain: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for sample in telemetry:
        telemetry_by_domain[int(sample["domain_id"])].append(sample)
    timestamps_by_domain: dict[int, list[float]] = {}
    for domain, samples in telemetry_by_domain.items():
        samples.sort(key=lambda row: row["timestamp_s"])
        timestamps_by_domain[domain] = [row["timestamp_s"] for row in samples]

    matched = 0
    for candidate in candidates:
        tid = candidate.get("tid")
        domain = number(candidate.get("candidate_domain", candidate.get("domain_id")))
        timestamp = timestamp_seconds(candidate.get("log_timestamp"))
        candidate["domain_id"] = int(domain) if domain is not None else None
        candidate["is_selected"] = bool_value(candidate.get("selected"))
        candidate["queue_toll"] = number(candidate.get("queue_backlog_toll"))
        runtime_est = number(candidate.get("estimated_runtime_ms"))
        function_name = function_by_tid.get(tid or "", "")
        if runtime_est is None:
            runtime_est = warm_runtimes.get(function_name)
        candidate["function_name"] = function_name or None
        candidate["runtime_est_ms"] = runtime_est

        sample = None
        sample_age_ms = None
        if domain is not None and timestamp is not None:
            domain = int(domain)
            domain_samples = telemetry_by_domain.get(domain, [])
            domain_times = timestamps_by_domain.get(domain, [])
            pos = bisect_left(domain_times, timestamp)
            nearby = [i for i in (pos - 1, pos) if 0 <= i < len(domain_samples)]
            if nearby:
                nearest = min(nearby, key=lambda i: abs(domain_times[i] - timestamp))
                age_ms = abs(domain_times[nearest] - timestamp) * 1000.0
                if age_ms <= tolerance_ms:
                    sample = domain_samples[nearest]
                    sample_age_ms = age_ms

        candidate["candidate_domain_pelt_util"] = sample.get("pelt_util") if sample else None
        candidate["candidate_domain_pelt_runnable"] = sample.get("runnable_load") if sample else None
        candidate["candidate_domain_norm_runnable"] = (
            sample["runnable_load"] / cores_per_domain
            if sample and sample.get("runnable_load") is not None and cores_per_domain
            else None
        )
        candidate["candidate_pelt_sample_age_ms"] = sample_age_ms
        warm_toll = number(candidate.get("warm_toll"))
        queue_toll = number(candidate.get("queue_backlog_toll"))
        candidate["unit_congestion_price"] = (
            (warm_toll + queue_toll) / runtime_est
            if runtime_est is not None and runtime_est > 0 and warm_toll is not None and queue_toll is not None and sample
            else None
        )
        if sample is not None:
            matched += 1
    return matched


def add_derived_metrics(
    rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
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
        tid = row["tid"]
        event_candidates = candidates_by_tid.get(tid, [])
        rhos = [v for candidate in event_candidates if (v := number(candidate.get("rho"))) is not None]
        row["domain_rho_spread"] = max(rhos) - min(rhos) if rhos else None
        selected_rho = number(row.get("rho"))
        if row.get("wave_state") == "WarmIdle" and selected_rho is not None and rhos:
            row["warm_affinity_gap"] = selected_rho - min(rhos)
        else:
            row["warm_affinity_gap"] = None

        runtime_est = number(row.get("estimated_runtime_ms"))
        if runtime_est is None:
            runtime_est = warm_runtimes.get(row.get("function_name", ""))
        row["runtime_est_ms"] = runtime_est
        benefit = number(row.get("benefit"))
        exec_ms = number(row.get("exec_ms"))
        queue_ms = number(row.get("pricing_queue_ms"))
        worker_e2e_ms = number(row.get("worker_e2e_ms"))
        row["priority_weight"] = benefit / runtime_est if benefit is not None and runtime_est else None
        row["exec_stretch"] = exec_ms / runtime_est if exec_ms is not None and runtime_est else None
        row["norm_queue_wait"] = queue_ms / runtime_est if queue_ms is not None and runtime_est else None
        row["dispatch_overhead_ms"] = (
            worker_e2e_ms - queue_ms - exec_ms
            if worker_e2e_ms is not None and queue_ms is not None and exec_ms is not None
            else None
        )

        # Older decision records omit this queue state. It can be reconstructed
        # from the logged toll only when the marginal-stretch coefficient is
        # nonzero; the result is the toll's capped effective queue weight.
        queue_weight = number(row.get("domain_queue_weight"))
        if queue_weight is None:
            queue_weight = number(row.get("queue_weight_sum"))
        source = "logged" if queue_weight is not None else "unavailable"
        if queue_weight is None:
            delta_sigma = number(row.get("delta_sigma_warm"))
            queue_toll = number(row.get("queue_backlog_toll"))
            r_min = number(row.get("r_min_ms"))
            if runtime_est is not None and r_min is not None and delta_sigma is not None and queue_toll is not None:
                denominator = min(runtime_est, r_min) * delta_sigma
                if denominator > 0:
                    queue_weight = queue_toll / denominator
                    source = "derived_from_queue_backlog_toll"
        row["domain_queue_weight"] = queue_weight
        row["domain_queue_weight_source"] = source

        selected_domain = number(row.get("final_domain"))
        if selected_domain is None:
            selected_domain = assigned_domains.get(tid)
        row["selected_domain"] = int(selected_domain) if selected_domain is not None else None
        pelt = pelt_samples.get(tid, {})
        pelt_domain = number(pelt.get("domain_id"))
        if selected_domain is not None and pelt_domain is not None and int(pelt_domain) != int(selected_domain):
            pelt = {}
        row["selected_pelt_util"] = number(pelt.get("selected_pelt_util"))
        row["selected_pelt_runnable"] = number(pelt.get("selected_pelt_runnable"))
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
    cohorts.extend((f"function:{f}", [r for r in rows if r.get("function_class") == f]) for f in sorted({r.get("function_class") for r in rows if r.get("function_class")}))
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
    # Price rows are intentional: unpriced express-lane hello calls have no
    # admission decision and therefore cannot participate in price correlations.
    classes = ("hello", "503.graph-bfs", "gzip_compression", "pyaes")
    result = []
    for function_class in classes:
        group = [row for row in rows if canonical_class(row) == function_class]
        queue_pairs = [row for row in group if number(row.get("priority_weight")) is not None and number(row.get("pricing_queue_ms")) is not None]
        qweight_n, qweight_corr = spearman_for(group, "domain_queue_weight", "pricing_queue_ms")
        dispatch = [v for row in group if (v := number(row.get("dispatch_overhead_ms"))) is not None]
        norm_queue = [v for row in group if (v := number(row.get("norm_queue_wait"))) is not None]
        priority = [v for row in group if (v := number(row.get("priority_weight"))) is not None]
        result.append({
            "function_class": function_class,
            "n": len(queue_pairs),
            "median_priority_weight": median(priority),
            "spearman_priority_vs_pricing_queue": spearman_for(group, "priority_weight", "pricing_queue_ms")[1],
            "spearman_domain_queue_weight_vs_pricing_queue": qweight_corr,
            "domain_queue_weight_pair_n": qweight_n,
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
    buckets = (("1", lambda n: n == 1), ("2", lambda n: n == 2), ("3", lambda n: n == 3), ("4", lambda n: n == 4), ("5+", lambda n: n >= 5))
    single = [number(row.get("cold_init_ms")) for row in cold if number(row.get("concurrent_boots_host")) == 1]
    single_median = median([v for v in single if v is not None])
    result = []
    for label, accepts in buckets:
        group = [row for row in cold if accepts(int(number(row.get("concurrent_boots_host")) or 0))]
        init = [number(row.get("cold_init_ms")) for row in group]
        init = [v for v in init if v is not None]
        runnable = [number(row.get("selected_pelt_runnable")) for row in group]
        runnable = [v for v in runnable if v is not None]
        gaps = [number(row.get("pelt_runnable_gap")) for row in group]
        gaps = [v for v in gaps if v is not None]
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


def domain_alignment_table(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    priced = [row for row in rows if bool_value(row.get("selected")) is True and number(row.get("rho")) is not None]
    cohorts: list[tuple[str, list[dict[str, Any]]]] = [(f"d{domain}", [r for r in priced if number(r.get("selected_domain")) == domain]) for domain in range(4)]
    cohorts.append(("overall", priced))
    result = []
    for domain_name, group in cohorts:
        pelt_pairs = [row for row in group if number(row.get("selected_pelt_util")) is not None]
        biases = [number(row["rho"]) - number(row["selected_pelt_util"]) for row in pelt_pairs]
        spread = [v for row in group if (v := number(row.get("domain_rho_spread"))) is not None]
        affinity = [v for row in group if (v := number(row.get("warm_affinity_gap"))) is not None]
        rho_stretch_n, rho_stretch = spearman_for(group, "rho", "exec_stretch")
        util_stretch_n, util_stretch = spearman_for(group, "selected_pelt_util", "exec_stretch")
        gap_stretch_n, gap_stretch = spearman_for(group, "pelt_runnable_gap", "exec_stretch")
        rho_util_n, rho_util_pearson = pearson_for(group, "rho", "selected_pelt_util")
        _, rho_util_spearman = spearman_for(group, "rho", "selected_pelt_util")
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
    metrics = ["total_marginal_toll", "net_gain", "benefit", "rho", "creation_credit", "client_e2e_ms", "worker_e2e_ms", "function_exec_ms"]
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
    metrics = ["total_marginal_toll", "net_gain", "benefit", "rho", "creation_credit", "client_e2e_ms", "worker_e2e_ms", "function_exec_ms"]
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


def write_report(path: Path, run_dir: Path, rows: list[dict[str, Any]], all_rows: list[dict[str, Any]], coverage_rows: list[dict[str, Any]], correlations: list[dict[str, Any]], coverage: dict[str, Any], bin_seconds: float) -> None:
    lines = [
        f"# CongestionPricing price analysis: {run_dir.name}",
        "",
        "Prices are joined to invocation CSV rows by exact `tid`; unobserved fields remain missing.",
        "The per-invocation price is the selected candidate domain. Candidate-level alternatives are in `price_candidates.csv`.",
        "",
        "## Coverage",
        "",
        f"- Invocation CSV rows: {coverage['csv_invocations']}",
        f"- Price candidate records: {coverage['price_candidate_records']}",
        f"- Invocations with selected price: {coverage['priced_invocations']}",
        f"- Price rows joined to CSV: {coverage['joined_invocations']}",
        f"- Selected price rows with client E2E: {coverage['with_client_e2e']}",
        f"- Selected price rows with worker E2E: {coverage['with_worker_e2e']}",
        f"- Selected price rows with four timing fields: {coverage['with_timing_breakdown']}",
        f"- Joined invocations with estimated or metadata runtime: {coverage['with_runtime_estimate']}",
        f"- Selected price rows with exact-tid PELT-like sample: {coverage['with_selected_pelt']}",
        f"- Matched cold boot start/completion pairs: {coverage['matched_cold_boots']}",
        "",
        "### Price coverage by function",
        "",
        "| Function class | CSV invocations | Priced | Unpriced | Coverage |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in coverage_rows:
        lines.append(f"| {row['function_class']} | {row['csv_invocations']} | {row['priced_invocations']} | {row['unpriced_invocations']} | {fmt(number(row['price_coverage_fraction']) * 100 if row['price_coverage_fraction'] is not None else None)}% |")
    lines += [
        "",
        "## Per-function snapshot",
        "",
        "See `price_by_function.csv` for means and p50/p95. Prices are in the coordinator's welfare units; latency and rho fields retain their named units.",
        "",
        "| Function class | n priced | net gain mean | toll mean | client E2E p50 ms | worker E2E p50 ms | function exec p50 ms |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for group in grouped_summary(rows):
        lines.append(
            f"| {group['function_class']} | {group['invocations_with_price']} | "
            f"{fmt(group.get('net_gain_mean'))} | {fmt(group.get('total_marginal_toll_mean'))} | "
            f"{fmt(group.get('client_e2e_ms_p50'))} | {fmt(group.get('worker_e2e_ms_p50'))} | "
            f"{fmt(group.get('function_exec_ms_p50'))} |"
        )
    lines += [
        "",
        "## Strongest descriptive correlations",
        "",
        "Pearson and Spearman correlations are descriptive associations, not causal effects. E2E includes queueing and startup; worker E2E is sourced from `Invocation complete`, client E2E from the load-generator CSV.",
        "",
        "| Cohort | Price feature | Latency | n | Pearson r | Spearman r |",
        "|---|---|---|---:|---:|---:|",
    ]
    ranked = [r for r in correlations if r["cohort"] == "all" and r["n"] >= 10 and r["spearman_r"] is not None]
    ranked.sort(key=lambda r: abs(r["spearman_r"]), reverse=True)
    for r in ranked[:20]:
        lines.append(f"| all | {r['price_feature']} | {r['latency_metric']} | {r['n']} | {fmt(r['pearson_r'])} | {fmt(r['spearman_r'])} |")
    lines += [
        "",
        "## Within-function correlations",
        "",
        "For each function class and outcome, this shows the feature with the largest absolute Spearman correlation (minimum n=50). Stratifying by function reduces, but does not eliminate, workload and time confounding.",
        "",
        "| Function class | Latency | Feature | n | Pearson r | Spearman r |",
        "|---|---|---|---:|---:|---:|",
    ]
    for function_class in sorted({r.get("function_class") for r in rows if r.get("function_class")}):
        cohort = f"function:{function_class}"
        for target in ("client_e2e_ms", "worker_e2e_ms", "function_exec_ms"):
            matches = [r for r in correlations if r["cohort"] == cohort and r["latency_metric"] == target and r["n"] >= 50 and r["spearman_r"] is not None]
            if matches:
                best = max(matches, key=lambda r: abs(float(r["spearman_r"])))
                lines.append(f"| {function_class} | {target} | {best['price_feature']} | {best['n']} | {fmt(best['pearson_r'])} | {fmt(best['spearman_r'])} |")
    lines += [
        "",
        "## Table A — Within-class priority differentiation and queue isolation",
        "",
        "Rows use selected-price invocations only, so unpriced express-lane hello calls are excluded. The `domain_queue_weight` correlation uses an explicitly logged value where available; otherwise it is recovered from the logged queue-backlog toll only when its coefficient is nonzero. The recovered value is the toll formula's capped effective queue weight. Pair counts are shown because missing queue-weight observations can reduce that correlation's sample.",
        "",
        "| Function class | n | Median priority weight | Spearman(priority, pricing queue) | Spearman(domain queue weight, pricing queue) | Queue-weight pairs | Median normalized queue wait | p95 dispatch overhead (ms) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in priority_queue_table(rows):
        lines.append(
            f"| {row['function_class']} | {row['n']} | {fmt(row['median_priority_weight'])} | "
            f"{fmt(row['spearman_priority_vs_pricing_queue'])} | {fmt(row['spearman_domain_queue_weight_vs_pricing_queue'])} | "
            f"{row['domain_queue_weight_pair_n']} | {fmt(row['median_norm_queue_wait'])} | {fmt(row['p95_dispatch_overhead_ms'])} |"
        )
    lines += [
        "",
        "## Table B — Concurrent cold-boot contention curve",
        "",
        "Cold init time is the elapsed worker-log interval from `Trying to cold start a new container` to `Container cold start completed`. Concurrency is the number of matched boot intervals active at the cold boot's start timestamp (half-open intervals; the invocation's own boot is included). Init stretch is each bucket's median divided by the one-boot bucket median. PELT columns use exact-tid sampled assigned-domain metrics and may have fewer observations than `n`.",
        "",
        "| Concurrent host boots | n | Median cold init (ms) | p95 cold init (ms) | Init stretch | Mean selected PELT runnable | Mean PELT runnable gap | PELT sample n |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in concurrent_boot_table(all_rows):
        lines.append(
            f"| {row['concurrent_boots_host']} | {row['n']} | {fmt(row['median_cold_init_ms'])} | "
            f"{fmt(row['p95_cold_init_ms'])} | {fmt(row['init_stretch'])} | {fmt(row['mean_selected_pelt_runnable'])} | "
            f"{fmt(row['mean_pelt_runnable_gap'])} | {row['pelt_sample_n']} |"
        )
    lines += [
        "",
        "## Table C — Selected rho and PELT-like alignment",
        "",
        "`selected_pelt_util` is the logged assigned-domain PELT-like utilization percent divided by 100, matching `selected_rho`'s normalized scale. Correlations are pairwise and omit missing fields; `pelt_pair_n` and the named pair counts make sparse sampling explicit. Spread and affinity summaries use all available selected-price rows in each cohort.",
        "",
        "| Domain | n priced | PELT pairs | Pearson(rho, PELT util) | Spearman(rho, PELT util) | Mean bias (rho - PELT util) | Spearman(rho, stretch) | n | Spearman(PELT util, stretch) | n | Spearman(runnable gap, stretch) | n | Median / p95 rho spread | Median / p95 warm affinity gap |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in domain_alignment_table(rows):
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
        "## Time series",
        "",
        f"`price_timeseries.csv` contains per-function {bin_seconds:g}-second bins for mean and p50 price and latency metrics.",
        "If Matplotlib is installed, `price_latency_timeseries-<function-class>.png` plots binned mean net gain, marginal toll, load, and client E2E separately for each function class.",
        "",
        f"`unit_congestion_price_vs_pelt.png` plots {(coverage.get('candidate_price_pelt_matches') or 0):,} all-candidate evaluations with matched PELT telemetry. Price is `(warm_toll + queue_backlog_toll) / runtime_est_ms`; cold toll and creation credit are excluded. Candidate-domain PELT telemetry is nearest-matched by domain within 50 ms, and runnable load is divided by configured cores per domain.",
        "",
        "## Interpretation limits",
        "",
        "- Only invocations with a selected candidate price event can enter price/latency correlations; express-lane paths may have no price event.",
        "- For multiple candidate domains, the selected price is only one candidate; see the candidate CSV to compare alternatives.",
        "- The price is calculated at admission while latency is observed later. Workload phase, cold status, and queueing can confound correlations.",
        "- `client_e2e_ms` is load-generator E2E; `worker_e2e_ms` is the worker's `Invocation complete` time. They are distinct measures.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def plot_timeseries(path: Path, rows: list[dict[str, Any]]) -> list[str]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    metrics = [
        ("net_gain_mean", "Mean net gain (welfare units)"),
        ("total_marginal_toll_mean", "Mean marginal toll (welfare units)"),
        ("rho_mean", "Mean normalized domain load"),
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
                ax.plot([r["bin_start_seconds"] for r in group], [r[metric] for r in group], marker=".", linewidth=1, label=function_class)
            ax.set_ylabel(title)
            ax.grid(True, alpha=0.25)
        axes[-1].set_xlabel("Seconds since worker start")
        fig.suptitle(f"CongestionPricing prices and observed E2E: {function_class}")
        plot_path = path.with_name(f"{path.stem}-{function_class}{path.suffix}")
        fig.savefig(plot_path, dpi=150)
        plt.close(fig)
        created.append(plot_path.name)
    return created


def plot_pelt_marginal_price_by_domain(path: Path, rows: list[dict[str, Any]]) -> list[str]:
    """Plot time-ordered PELT and selected marginal-toll lines by domain."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    decision_times = [
        ts for row in rows
        if (ts := timestamp_seconds(row.get("log_timestamp"))) is not None
    ]
    time_origin = min(decision_times) if decision_times else 0.0
    fig, axes = plt.subplots(2, 2, figsize=(12, 9), sharex=True, sharey=True)
    toll_axes = []
    for domain, axis in enumerate(axes.flat):
        group = []
        for row in rows:
            timestamp = timestamp_seconds(row.get("log_timestamp"))
            if (
                timestamp is not None
                and number(row.get("selected_domain")) == domain
                and number(row.get("selected_pelt_util")) is not None
                and number(row.get("total_marginal_toll")) is not None
            ):
                group.append((timestamp, row))
        group.sort(key=lambda pair: pair[0])
        elapsed = [timestamp - time_origin for timestamp, _ in group]
        pelt = [number(row.get("selected_pelt_util")) for _, row in group]
        toll = [number(row.get("total_marginal_toll")) for _, row in group]
        axis.plot(elapsed, pelt, color="tab:blue", linewidth=1.1, label="PELT utilization")
        toll_axis = axis.twinx()
        toll_axes.append(toll_axis)
        toll_axis.plot(elapsed, toll, color="tab:orange", linewidth=1.1, alpha=0.85, label="Marginal toll")
        if domain % 2 == 0:
            toll_axis.tick_params(axis="y", right=False, labelright=False)
        max_abs_toll = max((abs(value) for value in toll), default=0.0)
        if max_abs_toll <= 1.0:
            low, high = min([0.0, *toll]), max([0.0, *toll])
            padding = max((high - low) * 0.15, 0.02)
            toll_axis.set_ylim(low - padding, high + padding)
        else:
            toll_axis.set_yscale("symlog", linthresh=1.0)
        corr = spearman_for([row for _, row in group], "selected_pelt_util", "total_marginal_toll")[1]
        axis.set_title(f"d{domain}: n={len(group)}, Spearman={fmt(corr)}")
        axis.set_ylim(bottom=0.0)
        axis.grid(True, alpha=0.25)
    for axis in axes[-1, :]:
        axis.set_xlabel("Decision time since run start (s)")
    for axis in axes[:, 0]:
        axis.set_ylabel("Selected-domain PELT utilization (0–1)", color="tab:blue")
    for axis in (toll_axes[1], toll_axes[3]):
        axis.set_ylabel("Selected total marginal toll (welfare units)", color="tab:orange")
    fig.legend(
        [plt.Line2D([], [], color="tab:blue"), plt.Line2D([], [], color="tab:orange")],
        ["PELT utilization", "Selected total marginal toll"],
        loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.95),
    )
    fig.suptitle("Selected-domain PELT utilization and marginal price", y=0.99)
    fig.subplots_adjust(top=0.87, bottom=0.11, left=0.08, right=0.92, hspace=0.25, wspace=0.12)
    plot_path = path.with_name("pelt_marginal_price_by_domain.png")
    fig.savefig(plot_path, dpi=160)
    plt.close(fig)
    return [plot_path.name]


def binned_median_band(
    rows: list[dict[str, Any]], x_field: str, y_field: str, bins: int = 24
) -> tuple[list[float], list[float], list[float], list[float]]:
    pairs = [
        (number(row.get(x_field)), number(row.get(y_field)))
        for row in rows
    ]
    pairs = [(x, y) for x, y in pairs if x is not None and y is not None]
    if not pairs:
        return [], [], [], []
    x_min, x_max = min(x for x, _ in pairs), max(x for x, _ in pairs)
    if x_min == x_max:
        return [x_min], [median([y for _, y in pairs])], [percentile([y for _, y in pairs], 0.25)], [percentile([y for _, y in pairs], 0.75)]
    grouped: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for x, y in pairs:
        bin_id = min(bins - 1, int((x - x_min) / (x_max - x_min) * bins))
        grouped[bin_id].append((x, y))
    centers, medians, lows, highs = [], [], [], []
    for bin_id, group in sorted(grouped.items()):
        y_values = [y for _, y in group]
        centers.append(statistics.mean(x for x, _ in group))
        medians.append(median(y_values))
        lows.append(percentile(y_values, 0.25))
        highs.append(percentile(y_values, 0.75))
    return centers, medians, lows, highs


def plot_unit_congestion_price_vs_pelt(
    path: Path, candidates: list[dict[str, Any]], rho_knee: float | None
) -> list[str]:
    """Plot normalized congestion toll across all telemetry-matched candidates."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    panels = (
        ("candidate_domain_pelt_util", "Candidate-domain normalized PELT utilization"),
        ("candidate_domain_norm_runnable", "Candidate-domain normalized runnable load"),
    )
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5), sharey=True)
    styles = ((False, "tab:blue", "Rejected candidate"), (True, "tab:orange", "Selected candidate"))
    all_unit_prices = [
        number(row.get("unit_congestion_price"))
        for row in candidates
        if number(row.get("unit_congestion_price")) is not None
    ]
    positive_prices = [value for value in all_unit_prices if value > 0]
    symlog = bool(positive_prices and max(positive_prices) / min(positive_prices) > 100.0)

    for axis, (x_field, x_label) in zip(axes, panels):
        plotted = [
            row for row in candidates
            if number(row.get(x_field)) is not None and number(row.get("unit_congestion_price")) is not None
        ]
        for selected, color, label in styles:
            group = [row for row in plotted if bool_value(row.get("is_selected")) is selected]
            axis.scatter(
                [number(row.get(x_field)) for row in group],
                [number(row.get("unit_congestion_price")) for row in group],
                s=24 if selected else 9,
                alpha=0.8 if selected else 0.18,
                color=color,
                marker="D" if selected else ".",
                linewidths=0.0,
                label=label,
                zorder=3 if selected else 1,
            )
        centers, medians, lows, highs = binned_median_band(plotted, x_field, "unit_congestion_price")
        if centers:
            axis.fill_between(centers, lows, highs, color="black", alpha=0.14, label="25th–75th percentile", zorder=2)
            axis.plot(centers, medians, color="black", linewidth=2.0, label="Binned median", zorder=4)
        if rho_knee is not None:
            axis.axvline(rho_knee, color="black", linestyle="--", linewidth=1.2, alpha=0.7, label="rho_knee")
        corr = spearman_for(plotted, x_field, "unit_congestion_price")[1]
        axis.set_title(f"n={len(plotted):,}, Spearman r={fmt(corr)}")
        axis.set_xlabel(x_label)
        axis.grid(True, alpha=0.25)
        if symlog:
            linthresh = max(min(positive_prices), max(positive_prices) * 1e-4)
            axis.set_yscale("symlog", linthresh=linthresh)
    axes[0].set_ylabel("Unit congestion price: (warm_toll + queue_toll) / s_i")
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=4, frameon=False, bbox_to_anchor=(0.5, 0.99))
    scale_text = "symlog" if symlog else "linear"
    fig.suptitle(f"Per-unit-service congestion price vs. candidate-domain PELT load ({scale_text} y)", y=1.06)
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return [path.name]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="Experiment result directory")
    parser.add_argument("--worker-log", help="Worker JSONL log (auto-detected by default)")
    parser.add_argument("--invocations", help="Load-generator output CSV (auto-detected by default)")
    parser.add_argument("--metadata", help="Trace metadata CSV with static warm_dur_ms fallback (auto-detects a metadata CSV under RUN_DIR)")
    parser.add_argument("--output-dir", type=Path, help="Output directory (default: sibling RUN_NAME-congestion-price-analysis)")
    parser.add_argument("--bin-seconds", type=float, default=5.0, help="Time-series bin width (default: 5)")
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    if args.bin_seconds <= 0:
        parser.error("--bin-seconds must be positive")
    worker, invocation_csv = locate_files(run_dir, args.worker_log, args.invocations)
    output_dir = (args.output_dir or run_dir.parent / f"{run_dir.name}-congestion-price-analysis").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    prices, candidates, worker_extras, pelt_samples, pelt_telemetry, boot_intervals, dispatch_times, assigned_domains = read_worker_log(worker)
    invocation_rows = load_invocations(invocation_csv)
    if args.metadata:
        metadata_path = Path(args.metadata)
    else:
        metadata_candidates = [run_dir / "trace_metadata.csv"]
        metadata_candidates.extend(sorted(run_dir.glob("*meta*.csv")))
        metadata_candidates.extend(sorted(run_dir.glob("*metadata*.csv")))
        metadata_path = next((candidate for candidate in metadata_candidates if candidate.is_file()), run_dir / "trace_metadata.csv")
    warm_runtimes = load_warm_runtime_metadata(metadata_path if metadata_path.is_file() else None)
    rho_knee, cores_per_domain = load_run_plot_config(run_dir)
    function_by_tid = {row["tid"]: row.get("function_name", "") for row in invocation_rows}
    candidate_price_pelt_matches = enrich_candidate_records(
        candidates, pelt_telemetry, warm_runtimes, function_by_tid, cores_per_domain
    )
    for interval in boot_intervals:
        selected = prices.get(interval["tid"], {})
        selected_domain = number(selected.get("final_domain"))
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
        row["function_class"] = (price or {}).get("function_class") or invocation.get("function_name", "unknown").split("-")[0].lower()
        for src, dst in (("pricing_queue_ms", "pricing_queue_ms"), ("container_acquire_ms", "container_acquire_ms"), ("exec_ms", "exec_ms"), ("post_overhead_ms", "post_overhead_ms")):
            row[dst] = number(extra.get(src))
        joined.append(row)

    add_derived_metrics(joined, candidates, pelt_samples, warm_runtimes, boot_intervals, dispatch_times, assigned_domains)

    joined_price_rows = [r for r in joined if r["tid"] in prices]
    correlations = correlation_rows(joined_price_rows)
    by_function = grouped_summary(joined_price_rows)
    coverage_functions = coverage_by_function(joined)
    time_rows = timeseries_rows(joined_price_rows, args.bin_seconds)

    # Keep the selected per-invocation output compact; all alternative-domain
    # candidate records remain available in a separate CSV.
    write_csv(output_dir / "price_candidates.csv", candidates)
    write_csv(output_dir / "prices_per_invocation.csv", joined)
    write_csv(output_dir / "price_correlations.csv", correlations)
    write_csv(output_dir / "price_by_function.csv", by_function)
    write_csv(output_dir / "price_coverage_by_function.csv", coverage_functions)
    write_csv(output_dir / "price_timeseries.csv", time_rows)

    timing_fields = ("pricing_queue_ms", "container_acquire_ms", "exec_ms", "post_overhead_ms")
    coverage = {
        "csv_invocations": len(invocation_rows),
        "price_candidate_records": len(candidates),
        "priced_invocations": len(prices),
        "joined_invocations": len(joined_price_rows),
        "with_client_e2e": sum(number(r.get("client_e2e_ms")) is not None for r in joined_price_rows),
        "with_worker_e2e": sum(number(r.get("worker_e2e_ms")) is not None for r in joined_price_rows),
        "with_timing_breakdown": sum(all(number(r.get(f)) is not None for f in timing_fields) for r in joined_price_rows),
        "with_runtime_estimate": sum(number(r.get("runtime_est_ms")) is not None for r in joined),
        "with_selected_pelt": sum(number(r.get("selected_pelt_util")) is not None and number(r.get("selected_pelt_runnable")) is not None for r in joined_price_rows),
        "matched_cold_boots": len(boot_intervals),
        "candidate_price_pelt_matches": candidate_price_pelt_matches,
        "candidate_unit_price_points": sum(number(row.get("unit_congestion_price")) is not None for row in candidates),
    }
    write_report(output_dir / "analysis.md", run_dir, joined_price_rows, joined, coverage_functions, correlations, coverage, args.bin_seconds)
    plots = plot_timeseries(output_dir / "price_latency_timeseries.png", time_rows)
    plots.extend(plot_pelt_marginal_price_by_domain(output_dir / "price_latency_timeseries.png", joined_price_rows))
    plots.extend(plot_unit_congestion_price_vs_pelt(output_dir / "unit_congestion_price_vs_pelt.png", candidates, rho_knee))
    (output_dir / "coverage.json").write_text(json.dumps({**coverage, "plots_created": plots}, indent=2) + "\n", encoding="utf-8")
    print(f"Analysis: {output_dir}")
    print(json.dumps({**coverage, "plots_created": plots}, indent=2))


if __name__ == "__main__":
    main()
