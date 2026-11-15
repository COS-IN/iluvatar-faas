#!/usr/bin/env python3
"""Plot scheduler metrics across the hzga-8, hzga-16, and hzga-32 traces."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ANALYSIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ANALYSIS_DIR))
from cpu_sched import analyze_run  # noqa: E402

WORKSPACE = Path(__file__).resolve().parents[3]
DEFAULT_8_16_ROOT = WORKSPACE / "results/hzga-domain-sweep-20261114-101926"
DEFAULT_32_ROOT = WORKSPACE / "results/hzga-domain-sweep-20261114-223759"
SCHEDULERS = ("cfs", "domains-1", "domains-2", "domains-4", "domains-6")
LABELS = {
    "cfs": "CFS",
    "domains-1": "1 domain",
    "domains-2": "2 domains",
    "domains-4": "4 domains",
    "domains-6": "6 domains",
}
COLORS = {
    "cfs": "#202020",
    "domains-1": "#377eb8",
    "domains-2": "#ff7f00",
    "domains-4": "#4daf4a",
    "domains-6": "#984ea3",
}


def warm_invocations(result_dir: Path) -> tuple[list[dict[str, Any]], dict[str, float]]:
    csv_path = next(result_dir.glob("output-*.csv"), None)
    if csv_path is None:
        raise FileNotFoundError(f"no output-*.csv in {result_dir}")

    rows: list[dict[str, Any]] = []
    tids: set[str] = set()
    with csv_path.open(newline="") as stream:
        for row in csv.DictReader(stream):
            if row.get("success", "").lower() != "true" or row.get("was_cold", "").lower() != "false":
                continue
            tid = row.get("tid", "")
            if not tid:
                continue
            tids.add(tid)
            rows.append(row)

    queue_wait: dict[str, float] = {}
    log_path = result_dir / "worker1.log"
    with log_path.open(errors="replace") as stream:
        for line in stream:
            try:
                record = json.loads(line)
                fields = record.get("fields", {})
                tid = fields.get("tid")
                if fields.get("message") != "Item starting to execute" or tid not in tids:
                    continue
                inserted = datetime.fromisoformat(fields["insert_time"])
                removed = datetime.fromisoformat(fields["remove_time"])
                queue_wait[tid] = max(0.0, (removed - inserted).total_seconds() * 1000.0)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue

    missing = tids - queue_wait.keys()
    if missing:
        raise ValueError(
            f"{len(missing)} of {len(tids)} successful warm invocations lack worker queue timestamps in {result_dir}"
        )
    return rows, queue_wait


def analyze(result_dir: Path) -> dict[str, Any]:
    summary = analyze_run(result_dir)
    inv = summary["invocations"]
    power = summary["turbostat"]["summary"]
    rows, queue_wait = warm_invocations(result_dir)
    e2e = [float(row["e2e_duration_us"]) / 1000.0 for row in rows]
    execution = [float(row["code_duration_sec"]) * 1000.0 for row in rows]
    successes = inv["successes"]
    package_joules = power["package_joules"]
    return {
        "result_dir": str(result_dir),
        "requests": inv["total"],
        "successes": successes,
        "failures": inv["failures"],
        "warm": len(rows),
        "weighted_warm_e2e_ms": statistics.fmean(e2e),
        "queue_wait_ms": statistics.fmean(queue_wait.values()),
        "queue_samples": len(queue_wait),
        "function_execution_ms": statistics.fmean(execution),
        "busy_pct": power["Busy%"],
        "package_joules": package_joules,
        "package_joules_per_success": package_joules / successes,
        "package_edp_joule_ms": package_joules / successes * statistics.fmean(e2e),
    }


def plot_metric(
    plt: Any,
    data: dict[int, dict[str, dict[str, Any]]],
    metric: str,
    ylabel: str,
    title: str,
    output: Path,
) -> None:
    fig, axis = plt.subplots(figsize=(8.6, 5.2))
    for scheduler in SCHEDULERS:
        axis.plot(
            list(data),
            [data[trace][scheduler][metric] for trace in data],
            marker="o",
            linewidth=2,
            markersize=6,
            color=COLORS[scheduler],
            label=LABELS[scheduler],
        )
    axis.set_xlabel("HZGA trace size (functions)")
    axis.set_ylabel(ylabel)
    axis.set_title(title)
    axis.set_xticks(list(data))
    axis.grid(True, alpha=0.28)
    axis.legend(frameon=False, ncol=2)
    fig.tight_layout()
    fig.savefig(output.with_suffix(".png"), dpi=180)
    fig.savefig(output.with_suffix(".pdf"))
    plt.close(fig)


def plot_queue_execution(plt: Any, data: dict[int, dict[str, dict[str, Any]]], output: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.2), sharex=True)
    for axis, metric, ylabel, title in (
        (axes[0], "queue_wait_ms", "Mean queue wait (ms)", "Worker queue wait"),
        (axes[1], "function_execution_ms", "Mean function execution (ms)", "Function execution"),
    ):
        for scheduler in SCHEDULERS:
            axis.plot(
                list(data),
                [data[trace][scheduler][metric] for trace in data],
                marker="o",
                linewidth=2,
                markersize=6,
                color=COLORS[scheduler],
                label=LABELS[scheduler],
            )
        axis.set_xlabel("HZGA trace size (functions)")
        axis.set_ylabel(ylabel)
        axis.set_title(title)
        axis.set_xticks(list(data))
        axis.grid(True, alpha=0.28)
    axes[0].legend(frameon=False, ncol=2)
    fig.suptitle("Queueing and function execution across traces")
    fig.tight_layout()
    fig.savefig(output.with_suffix(".png"), dpi=180)
    fig.savefig(output.with_suffix(".pdf"))
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-8-16", type=Path, default=DEFAULT_8_16_ROOT)
    parser.add_argument("--results-32", type=Path, default=DEFAULT_32_ROOT)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=WORKSPACE / "results/hzga-trace-comparison-20261114",
    )
    args = parser.parse_args()

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise SystemExit("plots require matplotlib; install it with pip") from error

    run_roots = {8: args.results_8_16, 16: args.results_8_16, 32: args.results_32}
    data: dict[int, dict[str, dict[str, Any]]] = {}
    for trace, root in run_roots.items():
        data[trace] = {}
        for scheduler in SCHEDULERS:
            result_dir = root / f"hzga-{trace}-{scheduler}"
            if not result_dir.is_dir():
                raise FileNotFoundError(f"missing run directory: {result_dir}")
            print(f"Analyzing hzga-{trace} {LABELS[scheduler]}: {result_dir}")
            data[trace][scheduler] = analyze(result_dir)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_rows = []
    for trace, schedulers in data.items():
        for scheduler, row in schedulers.items():
            summary_rows.append({"trace_functions": trace, "scheduler": LABELS[scheduler], **row})
    summary_path = args.output_dir / "trace_comparison.csv"
    with summary_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)
    (args.output_dir / "trace_comparison.json").write_text(json.dumps(summary_rows, indent=2) + "\n")

    plot_metric(
        plt, data, "weighted_warm_e2e_ms", "Weighted warm E2E latency (ms)",
        "Warm end-to-end latency by trace size", args.output_dir / "latency_by_trace",
    )
    plot_queue_execution(plt, data, args.output_dir / "queue_execution_by_trace")
    fig, axes = plt.subplots(1, 3, figsize=(17, 5.2))
    for axis, metric, ylabel, title in (
        (axes[0], "busy_pct", "System Busy (%)", "Host CPU busy time"),
        (axes[1], "package_joules_per_success", "Package energy (J/success)", "Package energy per success"),
        (axes[2], "package_edp_joule_ms", "Package EDP (J·ms)", "Package energy-delay product"),
    ):
        for scheduler in SCHEDULERS:
            axis.plot(
                list(data),
                [data[trace][scheduler][metric] for trace in data],
                marker="o",
                linewidth=2,
                markersize=6,
                color=COLORS[scheduler],
                label=LABELS[scheduler],
            )
        axis.set_xlabel("HZGA trace size (functions)")
        axis.set_ylabel(ylabel)
        axis.set_title(title)
        axis.set_xticks(list(data))
        axis.grid(True, alpha=0.28)
    axes[0].legend(frameon=False, ncol=2)
    fig.suptitle("CPU use and energy across traces")
    fig.tight_layout()
    fig.savefig(args.output_dir / "cpu_energy_by_trace.png", dpi=180)
    fig.savefig(args.output_dir / "cpu_energy_by_trace.pdf")
    plt.close(fig)

    report = [
        "# HZGA trace comparison",
        "",
        "This comparison includes hzga-8, hzga-16, and hzga-32. All latency axes are linear. Queue wait is measured from worker queue insertion to dequeue timestamps; function execution is the measured code duration. Package EDP is package joules per successful invocation multiplied by weighted warm E2E latency.",
        "",
        "| Trace | Scheduler | Warm E2E (ms) | Queue wait (ms) | Exec (ms) | Busy (%) | Package J/success | Package EDP (J·ms) |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for trace, schedulers in data.items():
        for scheduler, row in schedulers.items():
            report.append(
                f"| {trace} | {LABELS[scheduler]} | {row['weighted_warm_e2e_ms']:.2f} | "
                f"{row['queue_wait_ms']:.2f} | {row['function_execution_ms']:.2f} | "
                f"{row['busy_pct']:.2f} | {row['package_joules_per_success']:.4f} | "
                f"{row['package_edp_joule_ms']:.2f} |"
            )
    (args.output_dir / "README.md").write_text("\n".join(report) + "\n")

    print(f"Wrote plots and summaries to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
