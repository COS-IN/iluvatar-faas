#!/usr/bin/env python3
"""Compare a paired CFS and FineSched Iluvatar experiment.

With no positional arguments, the newest complete ``*-cfs`` / ``*-finesched``
pair under ``--results-root`` is selected.  The parser trims perf and turbostat
to the worker's first ``Handling invocation request`` through its final
``Invocation complete`` event.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


DEFAULT_RESULTS = Path("/home/prateeks/code/results")
GROUPS = {
    "D0": {0, 1, 2, 12, 13, 14},
    "D1": {3, 4, 5, 15, 16, 17},
    "D2": {6, 7, 8, 18, 19, 20},
    "D3": {9, 10, 11, 21, 22, 23},
    "system": set(range(24)),
}
PERF_EVENTS = (
    "cycles",
    "instructions",
    "cache-references",
    "cache-misses",
    "branches",
    "branch-misses",
    "context-switches",
    "cpu-migrations",
)


def timestamp(value: str) -> datetime:
    """Parse timestamps onto one local, timezone-naive comparison timeline."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def nearest_rank(values: list[float], fraction: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def change_percent(new: float, old: float) -> float:
    return 100.0 * (new / old - 1.0) if old else math.nan


def cpu_groups(result: Path) -> dict[str, set[int]]:
    """Return the FineSched topology saved with a run, or the legacy layout."""
    config_path = result / "worker_config.json"
    if config_path.is_file():
        with config_path.open() as source:
            config = json.load(source)
        try:
            configured = config["fineloadbalancing"]["preallocated_groups"]["groups"]
            groups = {
                f"D{index}": {int(cpu) for cpu in group["cores"]}
                for index, group in enumerate(configured)
            }
            if groups:
                groups["system"] = set().union(*groups.values())
                return groups
        except (KeyError, TypeError, ValueError):
            pass
    return GROUPS


def newest_pair(results_root: Path) -> tuple[Path, Path]:
    pairs = []
    for cfs in results_root.glob("*-cfs"):
        fine = cfs.with_name(cfs.name.removesuffix("-cfs") + "-finesched")
        if fine.is_dir():
            pairs.append((max(cfs.stat().st_mtime, fine.stat().st_mtime), cfs, fine))
    if not pairs:
        raise FileNotFoundError(f"no *-cfs / *-finesched pairs found under {results_root}")
    _, cfs, fine = max(pairs)
    return cfs, fine


def select_file(result: Path, names: tuple[str, ...], glob: str | None = None) -> Path:
    for name in names:
        candidate = result / name
        if candidate.is_file():
            return candidate
    if glob:
        candidates = sorted(result.glob(glob))
        if candidates:
            return candidates[0]
    raise FileNotFoundError(f"no matching file in {result}: {names or glob}")


def worker_data(result: Path) -> dict[str, Any]:
    worker_log = select_file(
        result,
        ("worker1.log", "worker.log", "worker_worker1.log"),
        "worker_logs/*.log",
    )
    requests: list[datetime] = []
    completions: list[datetime] = []
    errors: Counter[tuple[str, str]] = Counter()
    assignments: Counter[tuple[str, int]] = Counter()

    with worker_log.open(errors="replace") as stream:
        for line in stream:
            try:
                entry = json.loads(line)
                fields = entry.get("fields", {})
                message = str(fields.get("message", ""))
                when = timestamp(entry["timestamp"])
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            if message == "Handling invocation request":
                requests.append(when)
            elif message == "Invocation complete":
                completions.append(when)
            elif message == "[finesched] domain assigned to function request":
                try:
                    assignments[(str(fields["fqdn"]), int(fields["domain_id"]))] += 1
                except (KeyError, TypeError, ValueError):
                    pass
            if entry.get("level") in {"WARN", "ERROR"}:
                errors[(str(entry["level"]), message)] += 1

    if not requests or not completions:
        raise ValueError(f"worker log has no complete invocation window: {worker_log}")
    first_request = min(requests)
    last_request = max(requests)
    last_completion = max(completions)
    return {
        "log": str(worker_log),
        "first_request": first_request,
        "last_request": last_request,
        "last_completion": last_completion,
        "request_count": len(requests),
        "completion_count": len(completions),
        "makespan_seconds": (last_completion - first_request).total_seconds(),
        "arrival_span_seconds": (last_request - first_request).total_seconds(),
        "drain_milliseconds": (last_completion - last_request).total_seconds() * 1000.0,
        "errors": [
            {"level": level, "message": message, "count": count}
            for (level, message), count in errors.most_common()
        ],
        "domain_assignments": [
            {"function": function, "domain": domain, "count": count}
            for (function, domain), count in sorted(assignments.items())
        ],
    }


def invocation_data(result: Path) -> dict[str, Any]:
    path = select_file(result, (), "output-*.csv")
    grouped: defaultdict[str, list[dict[str, float]]] = defaultdict(list)
    total = successes = cold = 0
    with path.open(newline="") as stream:
        for row in csv.DictReader(stream):
            total += 1
            successful = str(row.get("success", "")).lower() == "true"
            is_cold = str(row.get("was_cold", "")).lower() == "true"
            successes += successful
            cold += is_cold
            if not successful or is_cold:
                continue
            e2e = float(row["e2e_duration_us"]) / 1000.0
            worker = float(row["worker_duration_us"]) / 1000.0
            code = float(row["code_duration_sec"]) * 1000.0
            grouped[row["function_name"]].append(
                {
                    "e2e_ms": e2e,
                    "worker_ms": worker,
                    "code_ms": code,
                    "cpu_utilization": float(row["cpu_utilization"]),
                    "inside_worker_overhead_ms": max(0.0, worker - code),
                    "outside_worker_ms": max(0.0, e2e - worker),
                }
            )

    functions = {}
    all_e2e = []
    for function, rows in sorted(grouped.items()):
        e2e = [row["e2e_ms"] for row in rows]
        all_e2e.extend(e2e)
        functions[function] = {
            "warm_count": len(rows),
            "mean_e2e_ms": statistics.fmean(e2e),
            "p50_e2e_ms": nearest_rank(e2e, 0.50),
            "p95_e2e_ms": nearest_rank(e2e, 0.95),
            "p99_e2e_ms": nearest_rank(e2e, 0.99),
            "mean_worker_ms": statistics.fmean(row["worker_ms"] for row in rows),
            "mean_code_ms": statistics.fmean(row["code_ms"] for row in rows),
            "mean_cpu_utilization": statistics.fmean(
                row["cpu_utilization"] for row in rows
            ),
            "mean_inside_worker_overhead_ms": statistics.fmean(
                row["inside_worker_overhead_ms"] for row in rows
            ),
            "mean_outside_worker_ms": statistics.fmean(row["outside_worker_ms"] for row in rows),
        }
    return {
        "file": str(path),
        "total": total,
        "successes": successes,
        "failures": total - successes,
        "cold": cold,
        "warm": successes - cold,
        "weighted_warm_mean_e2e_ms": statistics.fmean(all_e2e),
        "functions": functions,
    }


def perf_start(path: Path) -> datetime:
    with path.open(errors="replace") as stream:
        for line in stream:
            if line.startswith("# started on "):
                return datetime.strptime(line[13:].strip(), "%a %b %d %H:%M:%S %Y")
    raise ValueError(f"perf start header not found in {path}")


def perf_data(result: Path, start: datetime, end: datetime) -> dict[str, Any] | None:
    path = result / "perf-stat.csv"
    if not path.is_file():
        return None
    started = perf_start(path)
    samples: defaultdict[float, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))
    enabled: defaultdict[tuple[float, str], list[float]] = defaultdict(list)
    with path.open(errors="replace") as stream:
        for row in csv.reader(stream):
            if not row or row[0].startswith("#") or len(row) < 7:
                continue
            try:
                elapsed = float(row[0])
                value = float(row[2])
                running = float(row[6])
            except ValueError:
                continue
            event = row[4]
            samples[elapsed][event] += value
            enabled[(elapsed, event)].append(running)

    selected: list[float] = []
    duration = 0.0
    previous = 0.0
    for elapsed in sorted(samples):
        interval_start = started + timedelta(seconds=previous)
        interval_end = started + timedelta(seconds=elapsed)
        if interval_start >= start and interval_end <= end:
            selected.append(elapsed)
            duration += elapsed - previous
        previous = elapsed
    if not selected:
        raise ValueError(f"no complete perf intervals inside invocation window for {result}")

    totals: defaultdict[str, float] = defaultdict(float)
    for elapsed in selected:
        for event, value in samples[elapsed].items():
            totals[event] += value
    ratios = {
        "ipc": totals["instructions"] / totals["cycles"],
        "cache_miss_percent": 100.0 * totals["cache-misses"] / totals["cache-references"],
        "branch_miss_percent": 100.0 * totals["branch-misses"] / totals["branches"],
    }
    median_enabled = {}
    for event in totals:
        values = []
        for elapsed in selected:
            values.extend(enabled.get((elapsed, event), []))
        if values:
            median_enabled[event] = statistics.median(values)
    return {
        "file": str(path),
        "sample_count": len(selected),
        "duration_seconds": duration,
        "totals": dict(totals),
        "per_second": {event: value / duration for event, value in totals.items()},
        "ratios": ratios,
        "median_enabled_percent": median_enabled,
    }


def turbostat_data(result: Path, start: datetime, end: datetime) -> dict[str, Any] | None:
    path = result / "turbostat.txt"
    if not path.is_file():
        return None
    marker_path = result / "profiling-start.txt"
    if marker_path.is_file():
        marker = timestamp(marker_path.read_text().strip())
    else:
        perf_path = result / "perf-stat.csv"
        if not perf_path.is_file():
            raise FileNotFoundError(f"no profiling-start.txt or perf-stat.csv in {result}")
        marker = perf_start(perf_path)

    lines = path.read_text(errors="replace").splitlines()
    columns = lines[0].split()
    wanted = (
        "CPU", "Avg_MHz", "Busy%", "Bzy_MHz", "IPC", "IRQ", "LLCMRPS",
        "LLC%hit", "POLL%", "C1%", "C2%", "C3%", "CorWatt", "PkgWatt",
    )
    indexes = {name: columns.index(name) for name in wanted}
    summaries: list[dict[str, float]] = []
    cpu_blocks: list[list[tuple[int, float, float, float]]] = []
    block: list[tuple[int, float, float, float]] = []
    for line in lines[1:]:
        values = line.split()
        if len(values) <= indexes["IPC"]:
            continue
        if values[indexes["CPU"]] == "-":
            if block:
                cpu_blocks.append(block)
                block = []
            try:
                summaries.append(
                    {name: float(values[index]) for name, index in indexes.items() if name != "CPU"}
                )
            except (IndexError, ValueError):
                pass
        else:
            try:
                block.append(
                    (
                        int(values[indexes["CPU"]]),
                        float(values[indexes["Busy%"]]),
                        float(values[indexes["Bzy_MHz"]]),
                        float(values[indexes["IPC"]]),
                    )
                )
            except (IndexError, ValueError):
                pass
    if block:
        cpu_blocks.append(block)
    count = min(len(summaries), len(cpu_blocks))
    summaries = summaries[:count]
    cpu_blocks = cpu_blocks[:count]

    # The wrapper writes profiling-start about 0.2 s after turbostat starts.
    # Restricting to complete one-second intervals avoids both boundary samples.
    selected = []
    for index in range(count):
        interval_start = marker + timedelta(seconds=index)
        interval_end = interval_start + timedelta(seconds=1)
        if interval_start >= start and interval_end <= end:
            selected.append(index)
    if not selected:
        raise ValueError(f"no complete turbostat intervals inside invocation window for {result}")

    summary = {
        name: statistics.fmean(summaries[index][name] for index in selected)
        for name in summaries[0]
    }
    summary["package_joules"] = sum(summaries[index]["PkgWatt"] for index in selected)
    summary["core_joules"] = sum(summaries[index]["CorWatt"] for index in selected)

    domains = {}
    for name, cpus in cpu_groups(result).items():
        intervals = []
        for index in selected:
            rows = [row for row in cpu_blocks[index] if row[0] in cpus]
            busy = sum(row[1] for row in rows)
            intervals.append(
                {
                    "busy_cores": busy / 100.0,
                    "busy_weighted_mhz": sum(row[1] * row[2] for row in rows) / busy if busy else 0.0,
                    "busy_weighted_ipc": sum(row[1] * row[3] for row in rows) / busy if busy else 0.0,
                }
            )
        domains[name] = {
            key: statistics.fmean(interval[key] for interval in intervals)
            for key in intervals[0]
        }
    return {
        "file": str(path),
        "sample_count": len(selected),
        "summary": summary,
        "domains": domains,
    }


def analyze_run(result: Path) -> dict[str, Any]:
    worker = worker_data(result)
    start = worker["first_request"]
    end = worker["last_completion"]
    analyzed = {
        "result_dir": str(result),
        "worker": worker,
        "invocations": invocation_data(result),
        "perf": perf_data(result, start, end),
        "turbostat": turbostat_data(result, start, end),
    }
    for key in ("first_request", "last_request", "last_completion"):
        worker[key] = worker[key].isoformat()
    return analyzed


def comparison(cfs: dict[str, Any], fine: dict[str, Any]) -> dict[str, Any]:
    latency = {}
    common_functions = sorted(
        set(cfs["invocations"]["functions"]) & set(fine["invocations"]["functions"])
    )
    latency_metrics = ("mean_e2e_ms", "p95_e2e_ms", "p99_e2e_ms", "mean_code_ms")
    for function in common_functions:
        old = cfs["invocations"]["functions"][function]
        new = fine["invocations"]["functions"][function]
        latency[function] = {
            metric: change_percent(new[metric], old[metric]) for metric in latency_metrics
        }
    result: dict[str, Any] = {
        "weighted_warm_mean_e2e_percent": change_percent(
            fine["invocations"]["weighted_warm_mean_e2e_ms"],
            cfs["invocations"]["weighted_warm_mean_e2e_ms"],
        ),
        "latency_percent": latency,
    }
    if cfs["perf"] and fine["perf"]:
        result["perf_rate_percent"] = {
            event: change_percent(fine["perf"]["per_second"][event], cfs["perf"]["per_second"][event])
            for event in PERF_EVENTS
        }
        result["perf_ratio_percent"] = {
            name: change_percent(fine["perf"]["ratios"][name], cfs["perf"]["ratios"][name])
            for name in cfs["perf"]["ratios"]
        }
    if cfs["turbostat"] and fine["turbostat"]:
        old = cfs["turbostat"]["summary"]
        new = fine["turbostat"]["summary"]
        old_requests = cfs["invocations"]["successes"]
        new_requests = fine["invocations"]["successes"]
        old_latency = cfs["invocations"]["weighted_warm_mean_e2e_ms"]
        new_latency = fine["invocations"]["weighted_warm_mean_e2e_ms"]
        old_package_per_request = old["package_joules"] / old_requests
        new_package_per_request = new["package_joules"] / new_requests
        old_core_per_request = old["core_joules"] / old_requests
        new_core_per_request = new["core_joules"] / new_requests
        result["power_percent"] = {
            "package_power": change_percent(new["PkgWatt"], old["PkgWatt"]),
            "core_power": change_percent(new["CorWatt"], old["CorWatt"]),
            "package_energy": change_percent(new["package_joules"], old["package_joules"]),
            "core_energy": change_percent(new["core_joules"], old["core_joules"]),
            "busy": change_percent(new["Busy%"], old["Busy%"]),
            "busy_frequency": change_percent(new["Bzy_MHz"], old["Bzy_MHz"]),
            "irq": change_percent(new["IRQ"], old["IRQ"]),
            "llc_references": change_percent(new["LLCMRPS"], old["LLCMRPS"]),
        }
        result["efficiency"] = {
            "cfs_package_joules_per_request": old_package_per_request,
            "finesched_package_joules_per_request": new_package_per_request,
            "package_joules_per_request_percent": change_percent(
                new_package_per_request, old_package_per_request
            ),
            "cfs_core_joules_per_request": old_core_per_request,
            "finesched_core_joules_per_request": new_core_per_request,
            "core_joules_per_request_percent": change_percent(
                new_core_per_request, old_core_per_request
            ),
            "cfs_response_edp_joule_ms": old_package_per_request * old_latency,
            "finesched_response_edp_joule_ms": new_package_per_request * new_latency,
            "response_edp_percent": change_percent(
                new_package_per_request * new_latency,
                old_package_per_request * old_latency,
            ),
        }
    return result


def markdown(data: dict[str, Any]) -> str:
    cfs = data["runs"]["CFS"]
    fine = data["runs"]["FineSched"]
    compare = data["comparison"]
    lines = [
        "# CFS versus FineSched comparison",
        "",
        f"- CFS: `{cfs['result_dir']}`",
        f"- FineSched: `{fine['result_dir']}`",
        "",
        "## Completion",
        "",
        "| Scheduler | Requests | Completions | Successes | Makespan (s) | Drain (ms) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, run in (("CFS", cfs), ("FineSched", fine)):
        worker = run["worker"]
        invocations = run["invocations"]
        lines.append(
            f"| {name} | {worker['request_count']} | {worker['completion_count']} | "
            f"{invocations['successes']} | {worker['makespan_seconds']:.3f} | "
            f"{worker['drain_milliseconds']:.3f} |"
        )

    lines.extend(
        [
            "",
            "## Warm latency",
            "",
            "| Function | CFS mean (ms) | FineSched mean (ms) | Mean delta | "
            "CFS p95 (ms) | FineSched p95 (ms) | p95 delta |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for function in sorted(compare["latency_percent"]):
        old = cfs["invocations"]["functions"][function]
        new = fine["invocations"]["functions"][function]
        delta = compare["latency_percent"][function]
        lines.append(
            f"| {function} | {old['mean_e2e_ms']:.3f} | {new['mean_e2e_ms']:.3f} | "
            f"{delta['mean_e2e_ms']:+.2f}% | {old['p95_e2e_ms']:.3f} | "
            f"{new['p95_e2e_ms']:.3f} | {delta['p95_e2e_ms']:+.2f}% |"
        )
    lines.append(
        f"\nInvocation-weighted warm mean delta: "
        f"**{compare['weighted_warm_mean_e2e_percent']:+.2f}%**."
    )

    lines.extend(
        [
            "",
            "## Warm latency decomposition",
            "",
            "Inside-worker overhead is `worker duration - function code duration`; "
            "outside-worker time is `end-to-end - worker duration`.",
            "",
            "| Function | CFS code (ms) | FineSched code (ms) | Code delta | "
            "CFS inside (ms) | FineSched inside (ms) | CFS outside (ms) | "
            "FineSched outside (ms) | CFS CPU util | FineSched CPU util |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for function in sorted(compare["latency_percent"]):
        old = cfs["invocations"]["functions"][function]
        new = fine["invocations"]["functions"][function]
        delta = compare["latency_percent"][function]
        lines.append(
            f"| {function} | {old['mean_code_ms']:.3f} | {new['mean_code_ms']:.3f} "
            f"| {delta['mean_code_ms']:+.2f}% "
            f"| {old['mean_inside_worker_overhead_ms']:.3f} "
            f"| {new['mean_inside_worker_overhead_ms']:.3f} "
            f"| {old['mean_outside_worker_ms']:.3f} | {new['mean_outside_worker_ms']:.3f} "
            f"| {old['mean_cpu_utilization']:.1f} | {new['mean_cpu_utilization']:.1f} |"
        )

    if cfs["turbostat"] and fine["turbostat"]:
        lines.extend(
            [
                "",
                "## Power and residency",
                "",
                "| Metric | CFS | FineSched | FineSched delta |",
                "|---|---:|---:|---:|",
            ]
        )
        old = cfs["turbostat"]["summary"]
        new = fine["turbostat"]["summary"]
        power = compare["power_percent"]
        rows = (
            ("Package power (W)", "PkgWatt", "package_power"),
            ("Package energy (J)", "package_joules", "package_energy"),
            ("Core power (W)", "CorWatt", "core_power"),
            ("Core energy (J)", "core_joules", "core_energy"),
            ("Busy (%)", "Busy%", "busy"),
            ("Busy frequency (MHz)", "Bzy_MHz", "busy_frequency"),
            ("Interrupts per sample", "IRQ", "irq"),
            ("LLC references/s", "LLCMRPS", "llc_references"),
        )
        for label, key, delta_key in rows:
            lines.append(f"| {label} | {old[key]:.3f} | {new[key]:.3f} | {power[delta_key]:+.2f}% |")
        lines.extend(
            [
                f"| C1 residency (%) | {old['C1%']:.3f} | {new['C1%']:.3f} |  |",
                f"| C2 residency (%) | {old['C2%']:.3f} | {new['C2%']:.3f} |  |",
                f"| C3 residency (%) | {old['C3%']:.3f} | {new['C3%']:.3f} |  |",
                "",
                "### Energy efficiency",
                "",
                "| Metric | CFS | FineSched | FineSched delta |",
                "|---|---:|---:|---:|",
                f"| Package energy/request (J) | "
                f"{compare['efficiency']['cfs_package_joules_per_request']:.4f} | "
                f"{compare['efficiency']['finesched_package_joules_per_request']:.4f} | "
                f"{compare['efficiency']['package_joules_per_request_percent']:+.2f}% |",
                f"| Core energy/request (J) | "
                f"{compare['efficiency']['cfs_core_joules_per_request']:.4f} | "
                f"{compare['efficiency']['finesched_core_joules_per_request']:.4f} | "
                f"{compare['efficiency']['core_joules_per_request_percent']:+.2f}% |",
                f"| Package response EDP (J·ms) | "
                f"{compare['efficiency']['cfs_response_edp_joule_ms']:.3f} | "
                f"{compare['efficiency']['finesched_response_edp_joule_ms']:.3f} | "
                f"{compare['efficiency']['response_edp_percent']:+.2f}% |",
            ]
        )

        lines.extend(
            [
                "",
                "## Per-domain CPU activity",
                "",
                "| Domain | CFS busy cores | FineSched busy cores | "
                "CFS busy MHz | FineSched busy MHz |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for domain in GROUPS:
            old_domain = cfs["turbostat"]["domains"][domain]
            new_domain = fine["turbostat"]["domains"][domain]
            lines.append(
                f"| {domain} | {old_domain['busy_cores']:.3f} | "
                f"{new_domain['busy_cores']:.3f} | "
                f"{old_domain['busy_weighted_mhz']:.1f} | "
                f"{new_domain['busy_weighted_mhz']:.1f} |"
            )

    if cfs["perf"] and fine["perf"]:
        lines.extend(
            [
                "",
                "## Perf counters",
                "",
                "| Counter | CFS total | FineSched total | FineSched rate delta |",
                "|---|---:|---:|---:|",
            ]
        )
        for event in PERF_EVENTS:
            lines.append(
                f"| {event} | {cfs['perf']['totals'][event]:.0f} | "
                f"{fine['perf']['totals'][event]:.0f} | "
                f"{compare['perf_rate_percent'][event]:+.2f}% |"
            )
        lines.extend(
            [
                f"| IPC | {cfs['perf']['ratios']['ipc']:.4f} | "
                f"{fine['perf']['ratios']['ipc']:.4f} | "
                f"{compare['perf_ratio_percent']['ipc']:+.2f}% |",
                f"| Cache-miss ratio (%) | {cfs['perf']['ratios']['cache_miss_percent']:.4f} | "
                f"{fine['perf']['ratios']['cache_miss_percent']:.4f} | "
                f"{compare['perf_ratio_percent']['cache_miss_percent']:+.2f}% |",
            ]
        )

    assignments = fine["worker"]["domain_assignments"]
    if assignments:
        lines.extend(["", "## FineSched domain assignments", "", "| Function | Domain | Count |", "|---|---:|---:|"])
        for row in assignments:
            lines.append(f"| {row['function']} | {row['domain']} | {row['count']} |")

    lines.extend(["", "## Worker warnings and errors", ""])
    for name, run in (("CFS", cfs), ("FineSched", fine)):
        errors = run["worker"]["errors"]
        if not errors:
            lines.append(f"- {name}: none")
        else:
            for error in errors:
                lines.append(f"- {name}: {error['count']} x {error['level']} `{error['message']}`")
    lines.append("")
    return "\n".join(lines)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cfs_dir", nargs="?", type=Path)
    parser.add_argument("finesched_dir", nargs="?", type=Path)
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--output-dir", type=Path, help="write comparison.md and comparison.json")
    return parser.parse_args()


def main() -> int:
    args = arguments()
    if (args.cfs_dir is None) != (args.finesched_dir is None):
        raise SystemExit("provide both CFS_DIR and FINESCHED_DIR, or neither")
    if args.cfs_dir is None:
        cfs_dir, fine_dir = newest_pair(args.results_root.resolve())
    else:
        cfs_dir, fine_dir = args.cfs_dir.resolve(), args.finesched_dir.resolve()
    for path in (cfs_dir, fine_dir):
        if not path.is_dir():
            raise SystemExit(f"result directory does not exist: {path}")

    cfs = analyze_run(cfs_dir)
    fine = analyze_run(fine_dir)
    data = {
        "runs": {"CFS": cfs, "FineSched": fine},
        "comparison": comparison(cfs, fine),
    }
    report = markdown(data)
    print(report)
    if args.output_dir:
        output = args.output_dir.resolve()
        output.mkdir(parents=True, exist_ok=True)
        (output / "comparison.md").write_text(report + "\n")
        (output / "comparison.json").write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
        print(f"Wrote {output / 'comparison.md'}")
        print(f"Wrote {output / 'comparison.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
