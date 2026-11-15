#!/usr/bin/env python3
"""Compare baseline and bursty trace latency, then summarize burst assignments."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SAMPLED_METRICS = {
    "sampled_domain_pelt_like_utilization_percent": "pelt_percent",
    "sampled_domain_proc_stat_utilization_percent": "proc_stat_percent",
    "sampled_domain_dsq_len": "domain_dsq_len",
    "sampled_runnable_load": "runnable_load",
}


def csv_path(result: Path, pattern: str) -> Path:
    matches = sorted(result.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"no {pattern} in {result}")
    return matches[-1]


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", errors="replace") as stream:
        return list(csv.DictReader(stream))


def mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def nearest_rank(values: list[float], q: float) -> float | None:
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values) * q) - 1)]


def as_float(value: str | None) -> float | None:
    """Read plain numbers and Rust debug options such as Ok(Some(12.3))."""
    if not value:
        return None
    match = re.search(r"[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)", value)
    return float(match.group()) if match else None


def read_run(result: Path, burst_seconds: float | None = None) -> dict[str, Any]:
    result = result.expanduser().resolve()
    output = csv_path(result, "output-*.csv")
    rows = load_rows(output)
    trace_rows: list[dict[str, str]] = []
    burst_start_ms = burst_end_ms = None
    mismatches = 0
    if burst_seconds is not None:
        trace_path = next(
            (p for p in sorted(result.glob("bursty-*.csv")) if not p.name.endswith("-metadata.csv")),
            None,
        )
        if trace_path is None:
            raise FileNotFoundError(f"no copied bursty trace CSV in {result}")
        trace_rows = load_rows(trace_path)
        trace_end_ms = max((int(row["invoke_time_ms"]) for row in trace_rows), default=0) + 1
        burst_ms = round(burst_seconds * 1000)
        if burst_ms > trace_end_ms:
            raise ValueError(f"burst window exceeds trace duration in {trace_path}")
        burst_start_ms = (trace_end_ms - burst_ms) // 2
        burst_end_ms = burst_start_ms + burst_ms
        mismatches = sum(
            trace_rows[i].get("func_name") != rows[i].get("function_name")
            for i in range(min(len(trace_rows), len(rows)))
        )

    cohorts: dict[str, list[dict[str, Any]]] = {"full": [], "burst": []}
    total = successful = 0
    for index, row in enumerate(rows):
        total += 1
        if row.get("success", "").lower() != "true":
            continue
        successful += 1
        if row.get("was_cold", "").lower() != "false":
            continue
        try:
            invocation = {
                "function": row.get("function_name", "unknown"),
                "e2e_ms": float(row["e2e_duration_us"]) / 1000.0,
                "exec_ms": float(row["code_duration_sec"]) * 1000.0,
                "worker_ms": float(row["worker_duration_us"]) / 1000.0,
                "domain": int(row["assigned_domain_id"]) if row.get("assigned_domain_id", "").strip() else None,
                "row": row,
            }
        except (KeyError, TypeError, ValueError):
            continue
        cohorts["full"].append(invocation)
        if (
            burst_seconds is not None
            and index < len(trace_rows)
            and burst_start_ms <= int(trace_rows[index]["invoke_time_ms"]) < burst_end_ms
        ):
            cohorts["burst"].append(invocation)

    return {
        "result_dir": str(result),
        "output_csv": str(output),
        "total": total,
        "successful": successful,
        "cohorts": cohorts,
        "burst_window_seconds": burst_seconds,
        "burst_start_ms": burst_start_ms,
        "burst_end_ms": burst_end_ms,
        "trace_output_row_name_mismatches": mismatches,
        "trace_rows": len(trace_rows),
        "output_rows": len(rows),
    }


def latency_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    functions = sorted({row["function"] for row in rows})
    by_function: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_function[row["function"]].append(row)
    result = {}
    for function in functions:
        entries = by_function[function]
        for metric in ("e2e_ms", "exec_ms"):
            values = [entry[metric] for entry in entries]
            result[(function, metric)] = {
                "n": len(values),
                "mean_ms": mean(values),
                "p50_ms": nearest_rank(values, 0.50),
                "p95_ms": nearest_rank(values, 0.95),
            }
    return result


def pearson(x: list[float], y: list[float]) -> float | None:
    if len(x) < 2 or len(x) != len(y):
        return None
    mx, my = statistics.fmean(x), statistics.fmean(y)
    xx = sum((v - mx) ** 2 for v in x)
    yy = sum((v - my) ** 2 for v in y)
    if not xx or not yy:
        return None
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / math.sqrt(xx * yy)


def burst_domain_analysis(run: dict[str, Any]) -> dict[str, Any]:
    invocations = run["cohorts"]["burst"]
    by_domain: dict[int, list[dict[str, Any]]] = defaultdict(list)
    function_counts: Counter[tuple[int, str]] = Counter()
    sampled: list[dict[str, Any]] = []
    for invocation in invocations:
        domain = invocation["domain"]
        if domain is None:
            continue
        by_domain[domain].append(invocation)
        function_counts[(domain, invocation["function"])] += 1
        if all(invocation["row"].get(key) for key in SAMPLED_METRICS):
            sample = {
                "function": invocation["function"],
                "domain": domain,
                "e2e_ms": invocation["e2e_ms"],
            }
            for source, target in SAMPLED_METRICS.items():
                sample[target] = as_float(invocation["row"].get(source))
            if all(sample.get(key) is not None for key in SAMPLED_METRICS.values()):
                sampled.append(sample)

    domains = []
    assignment_rows = []
    for domain in sorted(by_domain):
        entries = by_domain[domain]
        hello = [row for row in entries if row["function"] == "hello" or row["function"].startswith("hello-")]
        other = [row for row in entries if row not in hello]
        domains.append(
            {
                "domain": domain,
                "burst_invocations": len(entries),
                "hello_invocations": len(hello),
                "other_invocations": len(other),
                "hello_e2e_mean_ms": mean([row["e2e_ms"] for row in hello]),
                "other_e2e_mean_ms": mean([row["e2e_ms"] for row in other]),
                "hello_exec_mean_ms": mean([row["exec_ms"] for row in hello]),
                "other_exec_mean_ms": mean([row["exec_ms"] for row in other]),
                "functions": [
                    {"function": function, "count": count}
                    for (d, function), count in sorted(function_counts.items()) if d == domain
                ],
            }
        )
        for (d, function), count in sorted(function_counts.items()):
            if d == domain:
                assignment_rows.append({"domain": domain, "function": function, "is_hello": function == "hello" or function.startswith("hello-"), "count": count})

    correlations = {}
    for metric in SAMPLED_METRICS.values():
        pairs = [row for row in sampled if row.get(metric) is not None]
        correlations[metric] = {
            "n": len(pairs),
            "pearson_r_with_e2e": pearson([row[metric] for row in pairs], [row["e2e_ms"] for row in pairs]),
        }
    pairs = [row for row in sampled if row.get("pelt_percent") is not None]
    by_function: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pairs:
        by_function[row["function"]].append(row)
    residual_x: list[float] = []
    residual_y: list[float] = []
    for entries in by_function.values():
        if len(entries) < 2:
            continue
        load_mean = statistics.fmean(row["pelt_percent"] for row in entries)
        e2e_mean = statistics.fmean(row["e2e_ms"] for row in entries)
        residual_x.extend(row["pelt_percent"] - load_mean for row in entries)
        residual_y.extend(row["e2e_ms"] - e2e_mean for row in entries)
    correlations["pelt_within_function"] = {
        "n": len(residual_x),
        "pearson_r_with_e2e": pearson(residual_x, residual_y),
    }
    return {"domains": domains, "assignment_rows": assignment_rows, "sample_count": len(sampled), "correlations": correlations}


def fmt_pair(stats: dict[str, Any] | None) -> str:
    if not stats or stats.get("mean_ms") is None:
        return "—"
    return f"{stats['mean_ms']:.1f} / {stats['mean_ms_exec']:.1f}"


def build_latency_rows(runs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, dict[tuple[str, str], dict[str, Any]]]] = {}
    for label, run in runs.items():
        stats[label] = {
            cohort: latency_stats(run["cohorts"][cohort])
            for cohort in (("full", "burst") if cohort_burst(run) else ("full",))
        }
    names = sorted({function for run_stats in stats.values() for cohort_stats in run_stats.values() for function, _ in cohort_stats})
    output = []
    for function in names:
        row: dict[str, Any] = {"function": function}
        for scheduler in ("cfs", "finesched"):
            for cohort, run_label in (("hzga16", f"{scheduler}_baseline"), ("bursty_full", f"{scheduler}_bursty"), ("burst", f"{scheduler}_bursty")):
                stat_cohort = "full" if cohort != "burst" else "burst"
                available = stats.get(run_label, {}).get(stat_cohort, {})
                e2e = available.get((function, "e2e_ms"))
                execution = available.get((function, "exec_ms"))
                row[f"{scheduler}_{cohort}_n"] = e2e["n"] if e2e else 0
                row[f"{scheduler}_{cohort}_e2e_ms"] = e2e["mean_ms"] if e2e else None
                row[f"{scheduler}_{cohort}_exec_ms"] = execution["mean_ms"] if execution else None
        output.append(row)
    return output


def cohort_burst(run: dict[str, Any]) -> bool:
    return run.get("burst_window_seconds") is not None


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def markdown_report(
    runs: dict[str, dict[str, Any]],
    latency_rows: list[dict[str, Any]],
    domain: dict[str, Any],
    burst_seconds: float,
) -> str:
    lines = [
        "# Bursty trace comparison",
        "",
        "Per-function means are shown as E2E / function-execution latency in ms. Cohorts include warm successful invocations.",
        f"The burst portion is the approximate centered {burst_seconds:g}-second window in the copied trace; invocation rows are aligned to trace event order.",
        "",
        "## Trace-wide latency",
        "",
        "| Run | Warm successful | Mean E2E (ms) | Mean exec (ms) |",
        "|---|---:|---:|---:|",
    ]
    for label, run in runs.items():
        cohorts = [("full trace", run["cohorts"]["full"])]
        if cohort_burst(run):
            cohorts.append(("burst portion", run["cohorts"]["burst"]))
        for cohort_name, rows in cohorts:
            lines.append(
                f"| {label} — {cohort_name} | {len(rows)} | "
                f"{mean([row['e2e_ms'] for row in rows]) or 0:.3f} | {mean([row['exec_ms'] for row in rows]) or 0:.3f} |"
            )
    lines.extend(
        [
            "",
            "## Per-function latency",
            "",
            "| Function | CFS hzga-16 | CFS full bursty | CFS burst | FineSched hzga-16 | FineSched full bursty | FineSched burst |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in latency_rows:
        cells = [row["function"]]
        for scheduler in ("cfs", "finesched"):
            for cohort in ("hzga16", "bursty_full", "burst"):
                e2e, execution = row[f"{scheduler}_{cohort}_e2e_ms"], row[f"{scheduler}_{cohort}_exec_ms"]
                cells.append(f"{e2e:.1f} / {execution:.1f}" if e2e is not None and execution is not None else "—")
        lines.append("| " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "## FineSched burst-window domain assignment",
            "",
            "Counts below are warm successful invocations. The function list includes hello and coassigned non-hello functions.",
            "",
            "| Domain | All | Hello | Other | Hello mean E2E (ms) | Other mean E2E (ms) | Assigned functions (count) |",
            "|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in domain["domains"]:
        funcs = ", ".join(f"{item['function']} ({item['count']})" for item in row["functions"])
        hello_e2e = f"{row['hello_e2e_mean_ms']:.1f}" if row["hello_e2e_mean_ms"] is not None else "—"
        other_e2e = f"{row['other_e2e_mean_ms']:.1f}" if row["other_e2e_mean_ms"] is not None else "—"
        lines.append(f"| {row['domain']} | {row['burst_invocations']} | {row['hello_invocations']} | {row['other_invocations']} | {hello_e2e} | {other_e2e} | {funcs} |")

    lines.extend(
        [
            "",
            "## Sampled assignment-time load correlations",
            "",
            f"{domain['sample_count']} burst-window rows had all sampled load fields. Pearson correlations use E2E latency; they are descriptive, not causal.",
            "",
            "| Sampled metric | N | Pearson r with E2E |",
            "|---|---:|---:|",
        ]
    )
    labels = {
        "pelt_percent": "Domain PELT-like utilization",
        "proc_stat_percent": "Domain proc/stat utilization",
        "domain_dsq_len": "Domain DSQ length",
        "runnable_load": "Runnable load",
        "pelt_within_function": "PELT-like utilization, within-function residuals",
    }
    for key, label in labels.items():
        stat = domain["correlations"][key]
        corr = stat["pearson_r_with_e2e"]
        lines.append(f"| {label} | {stat['n']} | {corr:.3f} |" if corr is not None else f"| {label} | {stat['n']} | n/a |")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-cfs", type=Path, required=True, help="non-bursty hzga-16 CFS result directory")
    parser.add_argument("--baseline-finesched", type=Path, required=True, help="non-bursty hzga-16 FineSched result directory")
    parser.add_argument("--bursty-cfs", type=Path, required=True, help="bursty trace CFS result directory")
    parser.add_argument("--bursty-finesched", type=Path, required=True, help="bursty trace FineSched result directory")
    parser.add_argument("--burst-seconds", type=float, default=60.0, help="centered burst-window duration (default: 60)")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.burst_seconds <= 0:
        parser.error("--burst-seconds must be positive")

    runs = {
        "cfs_baseline": read_run(args.baseline_cfs),
        "finesched_baseline": read_run(args.baseline_finesched),
        "cfs_bursty": read_run(args.bursty_cfs, args.burst_seconds),
        "finesched_bursty": read_run(args.bursty_finesched, args.burst_seconds),
    }
    latency_rows = build_latency_rows(runs)
    domain = burst_domain_analysis(runs["finesched_bursty"])
    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)

    latency_fields = ["function"]
    for scheduler in ("cfs", "finesched"):
        for cohort in ("hzga16", "bursty_full", "burst"):
            latency_fields.extend(
                [f"{scheduler}_{cohort}_n", f"{scheduler}_{cohort}_e2e_ms", f"{scheduler}_{cohort}_exec_ms"]
            )
    write_csv(output / "per_function_latency.csv", latency_rows, latency_fields)
    assignment_fields = ["domain", "function", "is_hello", "count"]
    write_csv(output / "burst_domain_assignments.csv", domain["assignment_rows"], assignment_fields)
    report = markdown_report(runs, latency_rows, domain, args.burst_seconds)
    (output / "bursty_comparison.md").write_text(report)
    serializable_domain = {key: value for key, value in domain.items() if key != "assignment_rows"}
    summary = {
        "burst_seconds": args.burst_seconds,
        "runs": {
            label: {key: value for key, value in run.items() if key != "cohorts"}
            for label, run in runs.items()
        },
        "per_function": latency_rows,
        "burst_domain_analysis": serializable_domain,
    }
    (output / "bursty_comparison.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    print(report)
    print(f"\nWrote report and CSV/JSON summaries to {output}")


if __name__ == "__main__":
    main()
