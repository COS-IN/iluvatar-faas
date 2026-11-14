#!/usr/bin/env python3
"""Compare warm per-function latencies across CHBL domain-count runs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


LATENCIES = {
    "e2e_latency_ms": ("e2e_duration_us", 1.0 / 1000.0),
    "exec_latency_ms": ("code_duration_sec", 1000.0),
    "worker_duration_ms": ("worker_duration_us", 1.0 / 1000.0),
}


def nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def parse_run(value: str) -> tuple[int, Path]:
    try:
        domain_count, directory = value.split("=", 1)
        return int(domain_count), Path(directory).expanduser().resolve()
    except ValueError as error:
        raise argparse.ArgumentTypeError("run must have form DOMAIN_COUNT=RESULT_DIR") from error


def load_run(domain_count: int, result_dir: Path) -> tuple[dict[str, list[dict[str, Any]]], int, int]:
    candidates = sorted(result_dir.glob("output-*.csv"), key=lambda path: path.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"no output-*.csv invocation file in {result_dir}")
    path = candidates[-1]
    by_function: dict[str, list[dict[str, Any]]] = defaultdict(list)
    total = successful = 0
    with path.open(newline="", errors="replace") as stream:
        for row in csv.DictReader(stream):
            total += 1
            if row.get("success", "").lower() != "true":
                continue
            successful += 1
            if row.get("was_cold", "").lower() == "true":
                continue
            latencies: dict[str, float] = {}
            try:
                for metric, (column, scale) in LATENCIES.items():
                    latencies[metric] = float(row[column]) * scale
            except (KeyError, TypeError, ValueError):
                continue
            by_function[row.get("function_name", "unknown")].append(latencies)
    return by_function, total, successful


def gather_stats(runs: list[tuple[int, Path]]) -> tuple[list[dict[str, Any]], dict[int, dict[str, list[dict[str, Any]]]], dict[int, tuple[int, int]]]:
    data: dict[int, dict[str, list[dict[str, Any]]]] = {}
    counts: dict[int, tuple[int, int]] = {}
    for domain_count, result_dir in runs:
        by_function, total, successful = load_run(domain_count, result_dir)
        data[domain_count] = by_function
        counts[domain_count] = (total, successful)

    functions = sorted({function for by_function in data.values() for function in by_function})
    stats: list[dict[str, Any]] = []
    for function in functions:
        for domain_count, _ in runs:
            rows = data[domain_count].get(function, [])
            for metric in LATENCIES:
                values = [row[metric] for row in rows]
                stats.append(
                    {
                        "domain_count": domain_count,
                        "function_name": function,
                        "cohort": "warm_successful",
                        "latency_metric": metric,
                        "n": len(values),
                        "mean_ms": statistics.fmean(values) if values else "",
                        "p50_ms": nearest_rank(values, 0.50) if values else "",
                        "p95_ms": nearest_rank(values, 0.95) if values else "",
                        "p99_ms": nearest_rank(values, 0.99) if values else "",
                    }
                )
    return stats, data, counts


def load_profile_metrics(
    runs: list[tuple[int, Path]], counts: dict[int, tuple[int, int]]
) -> list[dict[str, Any]]:
    profiles = []
    for domain_count, result_dir in runs:
        path = result_dir / "profile-analysis" / "analysis.json"
        if not path.is_file():
            continue
        with path.open() as stream:
            analysis = json.load(stream)["run"]
        turbo = analysis.get("turbostat") or {}
        summary = turbo.get("summary") or {}
        perf = analysis.get("perf") or {}
        perf_rates = perf.get("per_second") or {}
        perf_ratios = perf.get("ratios") or {}
        assignments = analysis.get("worker", {}).get("domain_assignments", [])
        active_domains = sorted({assignment["domain"] for assignment in assignments})
        _, successful = counts[domain_count]
        package_joules = summary.get("package_joules")
        core_joules = summary.get("core_joules")
        profiles.append(
            {
                "domain_count": domain_count,
                "active_domains": ",".join(map(str, active_domains)),
                "active_domain_count": len(active_domains),
                "invocation_seconds": analysis.get("worker", {}).get("makespan_seconds", ""),
                "profile_samples": turbo.get("sample_count", ""),
                "package_watts": summary.get("PkgWatt", ""),
                "package_joules": package_joules if package_joules is not None else "",
                "package_joules_per_success": package_joules / successful if package_joules is not None and successful else "",
                "core_watts": summary.get("CorWatt", ""),
                "core_joules": core_joules if core_joules is not None else "",
                "core_joules_per_success": core_joules / successful if core_joules is not None and successful else "",
                "busy_percent": summary.get("Busy%", ""),
                "busy_frequency_mhz": summary.get("Bzy_MHz", ""),
                "turbostat_ipc": summary.get("IPC", ""),
                "perf_ipc": perf_ratios.get("ipc", ""),
                "cache_miss_percent": perf_ratios.get("cache_miss_percent", ""),
                "context_switches_per_second": perf_rates.get("context-switches", ""),
                "cpu_migrations_per_second": perf_rates.get("cpu-migrations", ""),
                "cycles_per_second": perf_rates.get("cycles", ""),
                "instructions_per_second": perf_rates.get("instructions", ""),
            }
        )
    return profiles


def write_profile_csv(profiles: list[dict[str, Any]], output: Path) -> None:
    if not profiles:
        return
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(profiles[0]))
        writer.writeheader()
        writer.writerows(profiles)


def load_edp_rows(
    runs: list[tuple[int, Path]], cfs_result: Path | None
) -> list[dict[str, Any]]:
    tagged_runs: list[tuple[str, Path]] = []
    if cfs_result is not None:
        tagged_runs.append(("CFS", cfs_result))
    tagged_runs.extend((f"FineSched {domain_count}D", result_dir) for domain_count, result_dir in runs)

    rows = []
    for label, result_dir in tagged_runs:
        analysis_path = result_dir / "profile-analysis" / "analysis.json"
        if not analysis_path.is_file():
            continue
        with analysis_path.open() as stream:
            analysis = json.load(stream)["run"]
        summary = analysis.get("turbostat", {}).get("summary", {})
        invocation = analysis.get("invocations", {})
        package_joules = summary.get("package_joules")
        core_joules = summary.get("core_joules")
        latency_ms = invocation.get("weighted_warm_mean_e2e_ms")
        success_count = invocation.get("successes")
        if None in (package_joules, core_joules, latency_ms, success_count) or not success_count:
            continue
        rows.append(
            {
                "configuration": label,
                "result_dir": str(result_dir),
                "successful_invocations": success_count,
                "weighted_warm_e2e_ms": latency_ms,
                "package_energy_j": package_joules,
                "core_energy_j": core_joules,
                "package_edp_j_ms": package_joules * latency_ms,
                "package_edp_j_ms_per_success": package_joules * latency_ms / success_count,
                "core_edp_j_ms_per_success": core_joules * latency_ms / success_count,
            }
        )
    return rows


def write_edp_csv(rows: list[dict[str, Any]], output: Path) -> None:
    if not rows:
        return
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_stats(stats: list[dict[str, Any]], output: Path, baseline: int) -> None:
    by_key = {
        (row["domain_count"], row["function_name"], row["latency_metric"]): row
        for row in stats
    }
    for row in stats:
        base = by_key.get((baseline, row["function_name"], row["latency_metric"]))
        if base and base["p95_ms"] != "" and row["p95_ms"] != "":
            row["p95_change_vs_baseline_percent"] = (
                100.0 * (row["p95_ms"] / base["p95_ms"] - 1.0) if base["p95_ms"] else ""
            )
        else:
            row["p95_change_vs_baseline_percent"] = ""
    columns = [
        "domain_count", "function_name", "cohort", "latency_metric", "n",
        "mean_ms", "p50_ms", "p95_ms", "p99_ms", "p95_change_vs_baseline_percent",
    ]
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(stats)


def write_report(
    stats: list[dict[str, Any]],
    counts: dict[int, tuple[int, int]],
    runs: list[tuple[int, Path]],
    output: Path,
    baseline: int,
    profiles: list[dict[str, Any]],
    edp_rows: list[dict[str, Any]],
) -> None:
    lines = [
        "# CHBL domain-count latency comparison",
        "",
        "Only successful warm invocations are included. `exec_latency_ms` is the function code duration (`code_duration_sec`); worker duration is provided separately in the CSV.",
        "Percentiles use nearest-rank. Changes are relative to the four-domain run.",
        "",
        "## Run coverage",
        "",
        "| Domains | Result directory | Rows | Successful | Warm successful |",
        "|---:|---|---:|---:|---:|",
    ]
    for domain_count, result_dir in runs:
        total, successful = counts[domain_count]
        warm = sum(
            row["n"] for row in stats
            if row["domain_count"] == domain_count and row["latency_metric"] == "e2e_latency_ms"
        )
        lines.append(f"| {domain_count} | `{result_dir}` | {total} | {successful} | {warm} |")

    functions = sorted({row["function_name"] for row in stats})
    by_key = {
        (row["domain_count"], row["function_name"], row["latency_metric"]): row
        for row in stats
    }
    for function in functions:
        lines.extend(
            [
                "",
                f"## `{function}`",
                "",
                "| Domains | N | E2E p50 / p95 / p99 (ms) | Δ E2E p95 vs 4D | Exec p50 / p95 / p99 (ms) | Δ exec p95 vs 4D |",
                "|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for domain_count, _ in runs:
            e2e = by_key[(domain_count, function, "e2e_latency_ms")]
            execution = by_key[(domain_count, function, "exec_latency_ms")]
            fmt = lambda row: " / ".join(
                f"{row[key]:.3f}" if row[key] != "" else "n/a"
                for key in ("p50_ms", "p95_ms", "p99_ms")
            )
            e2e_change = e2e["p95_change_vs_baseline_percent"]
            exec_change = execution["p95_change_vs_baseline_percent"]
            lines.append(
                f"| {domain_count} | {e2e['n']} | {fmt(e2e)} | "
                f"{e2e_change:+.1f}% | {fmt(execution)} | {exec_change:+.1f}% |"
            )

    if profiles:
        lines.extend(
            [
                "",
                "## Energy and system counters",
                "",
                "Turbostat package/core energy and power cover the worker invocation window. Joules/request divide that host-level energy by successful requests; this is not isolated per-function energy. Perf counters are system-wide.",
                "",
                "| Configured domains | Active domain IDs | Pkg W / J | Pkg J / success | Core W / J | Core J / success | Busy % | Busy MHz | Perf IPC | Cache misses % | Ctx switches/s | CPU migrations/s |",
                "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for profile in profiles:
            fmt = lambda value, digits=2: f"{value:.{digits}f}" if isinstance(value, (int, float)) else "n/a"
            lines.append(
                f"| {profile['domain_count']} | {profile['active_domains']} | "
                f"{fmt(profile['package_watts'])} / {fmt(profile['package_joules'], 1)} | "
                f"{fmt(profile['package_joules_per_success'], 3)} | "
                f"{fmt(profile['core_watts'])} / {fmt(profile['core_joules'], 1)} | "
                f"{fmt(profile['core_joules_per_success'], 3)} | "
                f"{fmt(profile['busy_percent'])} | {fmt(profile['busy_frequency_mhz'], 0)} | "
                f"{fmt(profile['perf_ipc'], 3)} | {fmt(profile['cache_miss_percent'])} | "
                f"{fmt(profile['context_switches_per_second'], 0)} | "
                f"{fmt(profile['cpu_migrations_per_second'], 0)} |"
            )
        lines.append(
            "\nThe 6-domain run configured six domains, but only the listed domains received invocations in this trace. The per-domain CPU activity from `cpu_sched.py` is omitted here because its fallback CPU grouping assumes the legacy four-domain layout when generated topology groups are not serialized."
        )

    if edp_rows:
        lines.extend(
            [
                "",
                "## Energy-delay product including CFS",
                "",
                "EDP is package/core energy over the invocation window multiplied by invocation-weighted warm e2e latency. The per-success columns divide by all successful invocations in each run.",
                "",
                "| Configuration | Warm weighted e2e (ms) | Package energy (J) | Package EDP (J·ms/success) | Core EDP (J·ms/success) |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for row in edp_rows:
            lines.append(
                f"| {row['configuration']} | {row['weighted_warm_e2e_ms']:.3f} | "
                f"{row['package_energy_j']:.2f} | "
                f"{row['package_edp_j_ms_per_success']:.3f} | "
                f"{row['core_edp_j_ms_per_success']:.3f} |"
            )
        if any(row["configuration"] == "CFS" for row in edp_rows):
            lines.append(
                "\nThe CFS profile is from a separate run/date than the FineSched sweep; treat it as contextual rather than a paired measurement."
            )

    lines.extend(
        [
            "",
            "## Plots",
            "",
            "- `e2e_latency_by_function.png`: warm per-function e2e p50 and p95 by domain count.",
            "- `exec_latency_by_function.png`: warm per-function code-execution p50 and p95 by domain count.",
            "",
            "These are descriptive results from one run per configuration, not repeated-run estimates.",
        ]
    )
    output.write_text("\n".join(lines) + "\n")


def make_plots(stats: list[dict[str, Any]], runs: list[tuple[int, Path]], output_dir: Path) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise RuntimeError("plots require matplotlib; install it with pip") from error

    domain_counts = [domain_count for domain_count, _ in runs]
    functions = sorted({row["function_name"] for row in stats})
    for metric, label, filename in (
        ("e2e_latency_ms", "E2E latency (ms)", "e2e_latency_by_function.png"),
        ("exec_latency_ms", "Function exec latency (ms)", "exec_latency_by_function.png"),
    ):
        fig, axes = plt.subplots(
            len(functions), 1,
            figsize=(12, max(7, 2.8 * len(functions))),
            sharex=True,
        )
        if len(functions) == 1:
            axes = [axes]
        for axis, function in zip(axes, functions):
            for quantile, style, color in (
                ("p50_ms", "-o", "tab:blue"),
                ("p95_ms", "--s", "tab:orange"),
            ):
                values = []
                for domain_count in domain_counts:
                    row = next(
                        row for row in stats
                        if row["domain_count"] == domain_count
                        and row["function_name"] == function
                        and row["latency_metric"] == metric
                    )
                    values.append(row[quantile] if row[quantile] != "" else math.nan)
                axis.plot(
                    domain_counts,
                    values,
                    style,
                    linewidth=1.5,
                    markersize=4,
                    color=color,
                    label=quantile.removesuffix("_ms"),
                )
            axis.set_ylabel(f"{function}\n{label}")
            axis.grid(True, alpha=0.25)
        axes[0].legend(loc="upper right")
        axes[-1].set_xlabel("Number of FineSched domains")
        axes[-1].set_xticks(domain_counts)
        fig.suptitle(f"Warm {label} by function and domain count")
        fig.tight_layout()
        fig.savefig(output_dir / filename, dpi=170)
        plt.close(fig)


def make_energy_plot(profiles: list[dict[str, Any]], output_dir: Path) -> None:
    if not profiles:
        return
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise RuntimeError("plots require matplotlib; install it with pip") from error

    x = [profile["domain_count"] for profile in profiles]
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    for axis, metrics, title, ylabel in (
        (axes[0], (("package_watts", "Package", "tab:blue"), ("core_watts", "Core", "tab:orange")), "Mean power during invocation window", "Power (W)"),
        (axes[1], (("package_joules_per_success", "Package", "tab:blue"), ("core_joules_per_success", "Core", "tab:orange")), "Host energy per successful invocation", "Energy (J/request)"),
    ):
        for metric, label, color in metrics:
            axis.plot(
                x,
                [profile[metric] for profile in profiles],
                "-o",
                linewidth=1.5,
                markersize=5,
                color=color,
                label=label,
            )
        axis.set_ylabel(ylabel)
        axis.set_title(title)
        axis.grid(True, alpha=0.25)
        axis.legend()
    axes[-1].set_xlabel("Configured FineSched domains")
    axes[-1].set_xticks(x)
    fig.suptitle("Energy and power by domain-count configuration")
    fig.tight_layout()
    fig.savefig(output_dir / "energy_by_domain_count.png", dpi=170)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run",
        action="append",
        type=parse_run,
        required=True,
        metavar="DOMAINS=RESULT_DIR",
        help="run to compare; provide once per configuration",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--baseline-domains", type=int, default=4)
    parser.add_argument("--cfs-run", type=Path, help="matching profiled CFS result directory")
    args = parser.parse_args()
    runs = sorted(args.run, key=lambda item: item[0])
    domain_counts = [domain_count for domain_count, _ in runs]
    if len(domain_counts) != len(set(domain_counts)):
        parser.error("domain counts must be unique")
    if args.baseline_domains not in domain_counts:
        parser.error("the baseline domain count must be included in --run values")

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    stats, _, counts = gather_stats(runs)
    profiles = load_profile_metrics(runs, counts)
    edp_rows = load_edp_rows(runs, args.cfs_run.expanduser().resolve() if args.cfs_run else None)
    write_stats(stats, output_dir / "latency_percentiles.csv", args.baseline_domains)
    write_profile_csv(profiles, output_dir / "profile_metrics.csv")
    write_edp_csv(edp_rows, output_dir / "edp_comparison.csv")
    write_report(
        stats, counts, runs, output_dir / "latency_comparison.md",
        args.baseline_domains, profiles, edp_rows,
    )
    make_plots(stats, runs, output_dir)
    make_energy_plot(profiles, output_dir)
    print(f"Wrote CHBL latency comparison for domain counts {domain_counts} to {output_dir}")


if __name__ == "__main__":
    main()
