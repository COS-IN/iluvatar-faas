#!/usr/bin/env python3
"""Plot FineSched per-domain metrics recorded in a worker JSON log.

Example:
    python3 analyze_domain_metrics.py /home/prateeks/code/chbl0.8-6/worker1.log

Outputs go into ``<result-dir>/domain-metrics-analysis`` by default. Matplotlib
and NumPy are required for plots; the parsed CSV and correlations use only the
Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


SAMPLE_MESSAGE = "[finesched][core-metrics] sampled assigned domain"
ASSIGNMENT_MESSAGE = "[finesched] assign_domain_to_function_request"
METRICS = (
    "domain_proc_stat_utilization_percent",
    "domain_pelt_like_utilization_percent",
    "domain_power_watts",
    "used_cores",
)
NUMBER = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?")


def number(value: Any) -> float | None:
    """Extract a numeric value from JSON numbers or Rust Option debug text."""
    if value is None:
        return None
    match = NUMBER.search(str(value))
    if not match or "None" in str(value):
        return None
    try:
        return float(match.group())
    except ValueError:
        return None


def assignment_data(fields: dict[str, Any]) -> tuple[int, float] | None:
    """Read (domain id, used cores) from old or structured assignment records."""
    if fields.get("domain_id") is not None and fields.get("used_cores") is not None:
        try:
            return int(fields["domain_id"]), float(fields["used_cores"])
        except (TypeError, ValueError):
            return None

    text = str(fields.get("domain_assigned", ""))
    domain = re.search(r"\bid:\s*(\d+)", text)
    used = re.search(r"\bused_cores:\s*(\d+(?:\.\d+)?)", text)
    if domain and used:
        return int(domain.group(1)), float(used.group(1))
    return None


def read_samples(log_path: Path) -> list[dict[str, Any]]:
    assignments: dict[str, tuple[int, float]] = {}
    samples: list[dict[str, Any]] = []
    with log_path.open(errors="replace") as stream:
        for line in stream:
            try:
                entry = json.loads(line)
                fields = entry.get("fields", {})
                message = fields.get("message")
            except (json.JSONDecodeError, AttributeError):
                continue

            if message == ASSIGNMENT_MESSAGE and fields.get("tid"):
                assignment = assignment_data(fields)
                if assignment:
                    assignments[str(fields["tid"])] = assignment
                continue
            if message != SAMPLE_MESSAGE:
                continue

            try:
                timestamp = datetime.fromisoformat(entry["timestamp"])
                domain_id = int(fields["domain_id"])
            except (KeyError, TypeError, ValueError):
                continue

            assignment = assignments.get(str(fields.get("tid", "")))
            used_cores = None
            if assignment and assignment[0] == domain_id:
                used_cores = assignment[1]

            samples.append(
                {
                    "timestamp": timestamp,
                    "tid": str(fields.get("tid", "")),
                    "domain_id": domain_id,
                    "used_cores": used_cores,
                    "domain_proc_stat_utilization_percent": number(
                        fields.get("domain_proc_stat_utilization_percent")
                    ),
                    "domain_pelt_like_utilization_percent": number(
                        fields.get("domain_pelt_like_utilization_percent")
                    ),
                    "domain_power_watts": number(fields.get("domain_power_watts")),
                }
            )

    if not samples:
        raise ValueError(f"no FineSched domain metric samples found in {log_path}")
    samples.sort(key=lambda sample: sample["timestamp"])
    start = samples[0]["timestamp"]
    for sample in samples:
        sample["time_seconds"] = (sample["timestamp"] - start).total_seconds()
    return samples


def read_invocations(csv_path: Path, samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Load invocation latencies and attach only exact, logged metric samples."""
    sample_by_tid = {sample["tid"]: sample for sample in samples if sample["tid"]}
    invocations: list[dict[str, Any]] = []
    with csv_path.open(newline="", errors="replace") as stream:
        for row in csv.DictReader(stream):
            try:
                assignment_time = datetime.fromisoformat(row["assignment_timestamp"])
                domain_id = int(row["assigned_domain_id"])
                e2e_ms = float(row["e2e_duration_us"]) / 1000.0
                exec_ms = float(row["code_duration_sec"]) * 1000.0
                worker_ms = float(row["worker_duration_us"]) / 1000.0
            except (KeyError, TypeError, ValueError):
                continue

            tid = row.get("tid", "")
            sample = sample_by_tid.get(tid)
            if sample and sample["domain_id"] != domain_id:
                sample = None
            sample_time = sample["timestamp"] if sample else None
            invocations.append(
                {
                    "timestamp": assignment_time,
                    "time_seconds": 0.0,
                    "tid": tid,
                    "function_name": row.get("function_name", ""),
                    "success": row.get("success", "").lower() == "true",
                    "was_cold": row.get("was_cold", "").lower() == "true",
                    "domain_id": domain_id,
                    "e2e_latency_ms": e2e_ms,
                    "exec_latency_ms": exec_ms,
                    "worker_duration_ms": worker_ms,
                    "sample_timestamp": sample_time,
                    "sample_age_ms": (sample_time - assignment_time).total_seconds() * 1000.0
                    if sample_time
                    else None,
                    "domain_proc_stat_utilization_percent": sample[
                        "domain_proc_stat_utilization_percent"
                    ] if sample else None,
                    "domain_pelt_like_utilization_percent": sample[
                        "domain_pelt_like_utilization_percent"
                    ] if sample else None,
                    "domain_power_watts": sample["domain_power_watts"] if sample else None,
                    "used_cores": sample["used_cores"] if sample else None,
                }
            )

    if invocations:
        start = min(invocation["timestamp"] for invocation in invocations)
        for invocation in invocations:
            invocation["time_seconds"] = (invocation["timestamp"] - start).total_seconds()
    return sorted(invocations, key=lambda invocation: invocation["timestamp"])


def write_invocation_csv(invocations: list[dict[str, Any]], output: Path) -> None:
    columns = [
        "timestamp", "time_seconds", "tid", "function_name", "success", "was_cold",
        "domain_id", "e2e_latency_ms", "exec_latency_ms", "worker_duration_ms",
        "sample_timestamp", "sample_age_ms", *METRICS,
    ]
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for invocation in invocations:
            writer.writerow(
                {
                    **invocation,
                    "timestamp": invocation["timestamp"].isoformat(),
                    "sample_timestamp": invocation["sample_timestamp"].isoformat()
                    if invocation["sample_timestamp"]
                    else "",
                }
            )


def pearson(values_x: list[float], values_y: list[float]) -> tuple[int, float | None]:
    pairs = [(x, y) for x, y in zip(values_x, values_y) if math.isfinite(x) and math.isfinite(y)]
    if len(pairs) < 3:
        return len(pairs), None
    xs, ys = zip(*pairs)
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    denominator = math.sqrt(
        sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys)
    )
    return len(pairs), numerator / denominator if denominator else None


def ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=values.__getitem__)
    ranked = [0.0] * len(values)
    index = 0
    while index < len(order):
        end = index + 1
        while end < len(order) and values[order[end]] == values[order[index]]:
            end += 1
        average_rank = (index + 1 + end) / 2.0
        for ordered_index in order[index:end]:
            ranked[ordered_index] = average_rank
        index = end
    return ranked


def write_latency_correlations(invocations: list[dict[str, Any]], output: Path) -> None:
    latency_metrics = ("e2e_latency_ms", "exec_latency_ms", "worker_duration_ms")
    utilization_metrics = (
        "domain_proc_stat_utilization_percent",
        "domain_pelt_like_utilization_percent",
        "domain_power_watts",
        "used_cores",
    )
    successful = [invocation for invocation in invocations if invocation["success"]]
    cohorts: list[tuple[str, list[dict[str, Any]]]] = [
        ("all_successful", successful),
        ("warm_successful", [invocation for invocation in successful if not invocation["was_cold"]]),
        ("cold_successful", [invocation for invocation in successful if invocation["was_cold"]]),
    ]
    functions = sorted({invocation["function_name"] for invocation in successful})
    for function in functions:
        cohort = [
            invocation for invocation in successful
            if invocation["function_name"] == function and not invocation["was_cold"]
        ]
        cohorts.append((f"warm_function:{function}", cohort))

    columns = ["cohort", "latency_metric", "utilization_metric", "n", "pearson_r", "spearman_r"]
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for cohort_name, cohort in cohorts:
            for latency_metric in latency_metrics:
                for utilization_metric in utilization_metrics:
                    pairs = [
                        (float(invocation[latency_metric]), float(invocation[utilization_metric]))
                        for invocation in cohort
                        if invocation[latency_metric] is not None
                        and invocation[utilization_metric] is not None
                        and math.isfinite(float(invocation[latency_metric]))
                        and math.isfinite(float(invocation[utilization_metric]))
                    ]
                    xs = [pair[0] for pair in pairs]
                    ys = [pair[1] for pair in pairs]
                    count, pearson_r = pearson(xs, ys)
                    _, spearman_r = pearson(ranks(xs), ranks(ys)) if count >= 3 else (count, None)
                    writer.writerow(
                        {
                            "cohort": cohort_name,
                            "latency_metric": latency_metric,
                            "utilization_metric": utilization_metric,
                            "n": count,
                            "pearson_r": "" if pearson_r is None else pearson_r,
                            "spearman_r": "" if spearman_r is None else spearman_r,
                        }
                    )


def fit_log_latency(points: list[dict[str, Any]], x_metric: str, y_metric: str) -> dict[str, float] | None:
    """Fit ln(latency_ms) = intercept + slope * utilization for positive values."""
    pairs = [
        (float(point[x_metric]), math.log(float(point[y_metric])))
        for point in points
        if point[x_metric] is not None and point[y_metric] is not None
        and float(point[x_metric]) >= 0 and float(point[y_metric]) > 0
    ]
    if len(pairs) < 3:
        return None
    xs, log_ys = zip(*pairs)
    mean_x = sum(xs) / len(xs)
    mean_y = sum(log_ys) / len(log_ys)
    variance_x = sum((x - mean_x) ** 2 for x in xs)
    if variance_x == 0:
        return None
    slope = sum((x - mean_x) * (y - mean_y) for x, y in pairs) / variance_x
    intercept = mean_y - slope * mean_x
    residual = sum((y - (intercept + slope * x)) ** 2 for x, y in pairs)
    total = sum((y - mean_y) ** 2 for y in log_ys)
    r_squared = 1.0 - residual / total if total else 0.0
    return {"n": float(len(pairs)), "intercept": intercept, "slope": slope, "r_squared": r_squared}


def write_latency_models(invocations: list[dict[str, Any]], output: Path) -> None:
    warm = [
        invocation for invocation in invocations
        if invocation["success"] and not invocation["was_cold"]
    ]
    latency_metrics = ("e2e_latency_ms", "exec_latency_ms")
    utilization_metrics = (
        "domain_proc_stat_utilization_percent",
        "domain_pelt_like_utilization_percent",
    )
    with output.open("w", newline="") as stream:
        columns = [
            "function_name", "domains", "latency_metric", "utilization_metric", "n",
            "model", "intercept", "slope", "r_squared",
        ]
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for function in sorted({invocation["function_name"] for invocation in warm}):
            points = [invocation for invocation in warm if invocation["function_name"] == function]
            domains = ",".join(str(domain) for domain in sorted({point["domain_id"] for point in points}))
            for latency_metric in latency_metrics:
                for utilization_metric in utilization_metrics:
                    fit = fit_log_latency(points, utilization_metric, latency_metric)
                    writer.writerow(
                        {
                            "function_name": function,
                            "domains": domains,
                            "latency_metric": latency_metric,
                            "utilization_metric": utilization_metric,
                            "n": int(fit["n"]) if fit else 0,
                            "model": "ln(latency_ms) = intercept + slope * utilization_percent",
                            "intercept": fit["intercept"] if fit else "",
                            "slope": fit["slope"] if fit else "",
                            "r_squared": fit["r_squared"] if fit else "",
                        }
                    )


def write_csv(samples: list[dict[str, Any]], output: Path) -> None:
    columns = ["timestamp", "time_seconds", "tid", "domain_id", *METRICS]
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for sample in samples:
            writer.writerow(
                {
                    **sample,
                    "timestamp": sample["timestamp"].isoformat(),
                }
            )


def write_correlations(samples: list[dict[str, Any]], output: Path) -> None:
    columns = ["domain", "metric_x", "metric_y", "n", "pearson_r"]
    pairs = [
        ("used_cores", "domain_proc_stat_utilization_percent"),
        ("used_cores", "domain_pelt_like_utilization_percent"),
        ("used_cores", "domain_power_watts"),
        ("domain_proc_stat_utilization_percent", "domain_pelt_like_utilization_percent"),
        ("domain_proc_stat_utilization_percent", "domain_power_watts"),
        ("domain_pelt_like_utilization_percent", "domain_power_watts"),
    ]
    domains: list[int | str] = sorted({sample["domain_id"] for sample in samples})
    domains.append("all_domains")
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for domain in domains:
            subset = samples if domain == "all_domains" else [
                sample for sample in samples if sample["domain_id"] == domain
            ]
            for metric_x, metric_y in pairs:
                xs = [sample[metric_x] for sample in subset if sample[metric_x] is not None and sample[metric_y] is not None]
                ys = [sample[metric_y] for sample in subset if sample[metric_x] is not None and sample[metric_y] is not None]
                count, coefficient = pearson(xs, ys)
                writer.writerow(
                    {
                        "domain": domain,
                        "metric_x": metric_x,
                        "metric_y": metric_y,
                        "n": count,
                        "pearson_r": "" if coefficient is None else coefficient,
                    }
                )


def make_plots(samples: list[dict[str, Any]], output_dir: Path, bin_seconds: float) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as error:
        raise RuntimeError("plots require matplotlib and numpy; install them with pip") from error

    domains = sorted({sample["domain_id"] for sample in samples})
    fig, axes = plt.subplots(len(domains), 1, figsize=(13, max(7, 2.6 * len(domains))), sharex=True)
    axes = np.atleast_1d(axes)
    for axis, domain in zip(axes, domains):
        points = [sample for sample in samples if sample["domain_id"] == domain]
        for metric, label, color in (
            ("domain_proc_stat_utilization_percent", "ProcStat", "tab:blue"),
            ("domain_pelt_like_utilization_percent", "PELT-like", "tab:orange"),
        ):
            valid = [point for point in points if point[metric] is not None]
            if valid:
                axis.plot(
                    [point["time_seconds"] for point in valid],
                    [point[metric] for point in valid],
                    linewidth=1.2,
                    label=label,
                    color=color,
                )
        axis.set_ylabel(f"Domain {domain}\nutilization (%)")
        axis.grid(True, alpha=0.25)
    axes[0].legend(loc="upper right")
    axes[-1].set_xlabel("Time since first metric sample (seconds)")
    fig.suptitle("FineSched domain utilization over time")
    fig.tight_layout()
    fig.savefig(output_dir / "domain_metrics_over_time.png", dpi=160)
    plt.close(fig)

    fig, axes = plt.subplots(
        len(domains), 1, figsize=(13, max(7, 2.6 * len(domains))), sharex=True
    )
    axes = np.atleast_1d(axes)
    for axis, domain in zip(axes, domains):
        points = [sample for sample in samples if sample["domain_id"] == domain]
        power_points = [point for point in points if point["domain_power_watts"] is not None]
        used_points = [point for point in points if point["used_cores"] is not None]
        if power_points:
            axis.plot(
                [point["time_seconds"] for point in power_points],
                [point["domain_power_watts"] for point in power_points],
                linewidth=1.2,
                color="tab:green",
                label="Domain power",
            )
        axis.set_ylabel(f"Domain {domain}\npower (W)")
        axis.grid(True, alpha=0.25)
        used_axis = axis.twinx()
        if used_points:
            used_axis.plot(
                [point["time_seconds"] for point in used_points],
                [point["used_cores"] for point in used_points],
                linewidth=1.2,
                color="tab:red",
                label="used_cores",
            )
        used_axis.set_ylabel("used_cores", color="tab:red")
        lines = axis.get_lines() + used_axis.get_lines()
        if lines and domain == domains[0]:
            axis.legend(lines, [line.get_label() for line in lines], loc="upper right")
    axes[-1].set_xlabel("Time since first metric sample (seconds)")
    fig.suptitle("FineSched domain power and assignment load over time")
    fig.tight_layout()
    fig.savefig(output_dir / "domain_power_used_cores_over_time.png", dpi=160)
    plt.close(fig)

    max_time = max(sample["time_seconds"] for sample in samples)
    bin_count = max(1, math.ceil((max_time + 1e-9) / bin_seconds))
    heatmap = np.full((len(domains), bin_count), np.nan)
    for row, domain in enumerate(domains):
        buckets: dict[int, list[float]] = defaultdict(list)
        for sample in samples:
            value = sample["domain_pelt_like_utilization_percent"]
            if sample["domain_id"] == domain and value is not None:
                bucket = min(int(sample["time_seconds"] / bin_seconds), bin_count - 1)
                buckets[bucket].append(value)
        for bucket, values in buckets.items():
            heatmap[row, bucket] = sum(values) / len(values)

    masked = np.ma.masked_invalid(heatmap)
    cmap = plt.get_cmap("viridis").copy()
    cmap.set_bad(color="#eeeeee")
    fig, axis = plt.subplots(figsize=(14, max(3, 1.0 + 0.75 * len(domains))))
    image = axis.imshow(
        masked,
        aspect="auto",
        origin="lower",
        interpolation="nearest",
        extent=(0, bin_count * bin_seconds, -0.5, len(domains) - 0.5),
        cmap=cmap,
    )
    axis.set_yticks(range(len(domains)), [f"Domain {domain}" for domain in domains])
    axis.set_xlabel("Time since first metric sample (seconds)")
    axis.set_ylabel("Domain")
    axis.set_title(f"PELT-like domain utilization (mean per {bin_seconds:g}s bin)")
    colorbar = fig.colorbar(image, ax=axis)
    colorbar.set_label("Utilization (%)")
    fig.tight_layout()
    fig.savefig(output_dir / "pelt_utilization_heatmap.png", dpi=180)
    plt.close(fig)


def make_latency_plots(invocations: list[dict[str, Any]], output_dir: Path) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise RuntimeError("plots require matplotlib; install it with pip") from error

    successful = [invocation for invocation in invocations if invocation["success"]]
    domains = sorted({invocation["domain_id"] for invocation in successful})
    fig, axes = plt.subplots(
        len(domains), 1, figsize=(13, max(7, 2.7 * len(domains))), sharex=True
    )
    if len(domains) == 1:
        axes = [axes]
    for axis, domain in zip(axes, domains):
        points = [
            invocation for invocation in successful
            if invocation["domain_id"] == domain
        ]
        e2e = [point for point in points if point["e2e_latency_ms"] > 0]
        execution = [point for point in points if point["exec_latency_ms"] > 0]
        if e2e:
            axis.plot(
                [point["time_seconds"] for point in e2e],
                [point["e2e_latency_ms"] for point in e2e],
                linewidth=0.8,
                color="tab:blue",
                label="E2E latency",
            )
        axis.set_ylabel(f"Domain {domain}\nE2E (ms)")
        axis.grid(True, alpha=0.25, which="both")
        exec_axis = axis.twinx()
        if execution:
            exec_axis.plot(
                [point["time_seconds"] for point in execution],
                [point["exec_latency_ms"] for point in execution],
                linewidth=0.8,
                color="tab:orange",
                label="Function exec",
            )
        exec_axis.set_ylabel("Exec latency (ms)", color="tab:orange")
        lines = axis.get_lines() + exec_axis.get_lines()
        if lines and domain == domains[0]:
            axis.legend(lines, [line.get_label() for line in lines], loc="upper right")
    axes[-1].set_xlabel("Time since first invocation assignment (seconds)")
    fig.suptitle("Successful invocation latency over time")
    fig.tight_layout()
    fig.savefig(output_dir / "invocation_latencies_over_time.png", dpi=160)
    plt.close(fig)

    warm = [invocation for invocation in successful if not invocation["was_cold"]]
    functions = sorted({invocation["function_name"] for invocation in warm})
    latency_metrics = (
        ("e2e_latency_ms", "E2E latency (ms)"),
        ("exec_latency_ms", "Function exec latency (ms)"),
    )
    utilization_metrics = (
        ("domain_proc_stat_utilization_percent", "ProcStat"),
        ("domain_pelt_like_utilization_percent", "PELT-like"),
    )
    metric_colors = {"domain_proc_stat_utilization_percent": "tab:blue", "domain_pelt_like_utilization_percent": "tab:orange"}
    fig, axes = plt.subplots(
        len(functions), len(latency_metrics),
        figsize=(13, max(7, 2.7 * len(functions))), squeeze=False,
    )
    for row, function in enumerate(functions):
        function_points = [point for point in warm if point["function_name"] == function]
        sampled_function_points = [point for point in function_points if point["sample_timestamp"]]
        for column, (latency_metric, latency_label) in enumerate(latency_metrics):
            axis = axes[row][column]
            for utilization_metric, utilization_label in utilization_metrics:
                points = [
                    point for point in function_points
                    if point[utilization_metric] is not None and point[latency_metric] > 0
                ]
                if not points:
                    continue
                color = metric_colors[utilization_metric]
                axis.scatter(
                    [point[utilization_metric] for point in points],
                    [point[latency_metric] for point in points],
                    s=20,
                    alpha=0.65,
                    color=color,
                    label=f"{utilization_label} samples (n={len(points)})",
                )
                fit = fit_log_latency(points, utilization_metric, latency_metric)
                if fit:
                    x_values = [point[utilization_metric] for point in points]
                    x_min, x_max = min(x_values), max(x_values)
                    if x_max > x_min:
                        steps = [x_min + (x_max - x_min) * index / 59 for index in range(60)]
                        axis.plot(
                            steps,
                            [math.exp(fit["intercept"] + fit["slope"] * value) for value in steps],
                            color=color,
                            linewidth=1.7,
                            label=f"{utilization_label} fit (R²={fit['r_squared']:.2f})",
                        )
            axis.set_xlabel("Assignment-time utilization (%)")
            axis.set_ylabel(latency_label)
            axis.set_title(f"{function} · n={len(sampled_function_points)} warm sampled invocations")
            axis.grid(True, alpha=0.25, which="both")
            if row == 0 and column == 0:
                axis.legend(loc="best", fontsize="small")
    fig.suptitle("Warm invocation latency vs utilization by function\nlog-linear fits")
    fig.tight_layout()
    fig.savefig(output_dir / "latency_vs_domain_metrics.png", dpi=170)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("worker_log", type=Path, help="FineSched worker JSON log")
    parser.add_argument(
        "--invocations-csv",
        type=Path,
        help="invocation results CSV (defaults to newest output-*.csv beside the worker log)",
    )
    parser.add_argument("--output-dir", type=Path, help="output directory (defaults beside the log)")
    parser.add_argument("--bin-seconds", type=float, default=5.0, help="heatmap time-bin width (default: 5s)")
    args = parser.parse_args()
    if args.bin_seconds <= 0:
        parser.error("--bin-seconds must be positive")

    log_path = args.worker_log.expanduser().resolve()
    output_dir = args.output_dir or log_path.parent / "domain-metrics-analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    samples = read_samples(log_path)
    write_csv(samples, output_dir / "domain_metric_samples.csv")
    write_correlations(samples, output_dir / "metric_correlations.csv")
    make_plots(samples, output_dir, args.bin_seconds)
    invocations_csv = args.invocations_csv
    if invocations_csv is None:
        candidates = sorted(log_path.parent.glob("output-*.csv"), key=lambda path: path.stat().st_mtime)
        invocations_csv = candidates[-1] if candidates else None
    if invocations_csv is not None:
        invocations = read_invocations(invocations_csv, samples)
        write_invocation_csv(invocations, output_dir / "invocation_latency_samples.csv")
        write_latency_correlations(
            invocations, output_dir / "latency_utilization_correlations.csv"
        )
        write_latency_models(invocations, output_dir / "latency_utilization_models.csv")
        make_latency_plots(invocations, output_dir)
        matched = sum(invocation["sample_timestamp"] is not None for invocation in invocations)
        print(
            f"Loaded {len(invocations)} invocation rows; {matched} have exact utilization samples "
            f"from {invocations_csv}"
        )
    else:
        print("No output-*.csv found; generated domain-metric plots only")
    print(f"Parsed {len(samples)} samples across {len({s['domain_id'] for s in samples})} domains")
    print(f"Wrote analysis to {output_dir}")


if __name__ == "__main__":
    main()
