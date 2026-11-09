#!/usr/bin/env python3
"""Compare labeled FineSched experiment result directories."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        action="append",
        required=True,
        metavar="LABEL=DIR",
        help="labeled experiment directory; repeat for every case",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def timestamp(value: str) -> datetime:
    base, fraction = value.split(".", 1)
    return datetime.fromisoformat(f"{base}.{fraction[:6]}")


def analyze(label: str, directory: Path) -> dict:
    csv_files = sorted(directory.glob("output-*.csv"))
    if len(csv_files) != 1:
        raise ValueError(f"expected one output CSV in {directory}, found {len(csv_files)}")

    with csv_files[0].open(newline="") as source:
        rows = list(csv.DictReader(source))
    successful = [row for row in rows if row["success"].lower() == "true"]
    warm = [row for row in successful if row["was_cold"].lower() != "true"]

    functions = {}
    grouped = defaultdict(list)
    for row in warm:
        grouped[row["function_name"]].append(float(row["e2e_duration_us"]) / 1000.0)
    counts = Counter(row["function_name"] for row in rows)
    for function, values in sorted(grouped.items()):
        functions[function] = {
            "invocations": counts[function],
            "warm_invocations": len(values),
            "mean_ms": statistics.fmean(values),
            "p95_ms": percentile(values, 0.95),
        }

    first_request = None
    last_request = None
    last_completion = None
    levels = Counter()
    with (directory / "worker1.log").open() as source:
        for line in source:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            levels[entry.get("level", "")] += 1
            message = entry.get("fields", {}).get("message")
            if message == "Handling invocation request":
                time = timestamp(entry["timestamp"])
                first_request = first_request or time
                last_request = time
            elif message == "Invocation complete":
                last_completion = timestamp(entry["timestamp"])

    if first_request is None or last_request is None or last_completion is None:
        raise ValueError(f"missing invocation boundaries in {directory / 'worker1.log'}")

    with (directory / "worker_config.json").open() as source:
        config = json.load(source)

    warm_values = [float(row["e2e_duration_us"]) / 1000.0 for row in warm]
    fineloadbalancing = config.get("fineloadbalancing")
    policy = (
        fineloadbalancing.get("dispatchpolicy", "FineSched")
        if isinstance(fineloadbalancing, dict)
        else "CFS"
    )
    return {
        "label": label,
        "directory": str(directory.resolve()),
        "policy": policy,
        "requests": len(rows),
        "successes": len(successful),
        "warm_invocations": len(warm),
        "warm_mean_ms": statistics.fmean(warm_values),
        "warm_p95_ms": percentile(warm_values, 0.95),
        "arrival_span_s": (last_request - first_request).total_seconds(),
        "makespan_s": (last_completion - first_request).total_seconds(),
        "drain_ms": (last_completion - last_request).total_seconds() * 1000.0,
        "worker_errors": levels["ERROR"],
        "worker_warnings": levels["WARN"],
        "functions": functions,
    }


def render(cases: list[dict]) -> str:
    lines = [
        "# FineSched policy/load matrix",
        "",
        "## Completion and aggregate latency",
        "",
        "| Case | Policy | Requests | Successes | Warm mean (ms) | Warm p95 (ms) | Makespan (s) | Drain (ms) | Errors/warnings |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for case in cases:
        lines.append(
            f"| {case['label']} | `{case['policy']}` | {case['requests']} | {case['successes']} "
            f"| {case['warm_mean_ms']:.3f} | {case['warm_p95_ms']:.3f} "
            f"| {case['makespan_s']:.3f} | {case['drain_ms']:.3f} "
            f"| {case['worker_errors']}/{case['worker_warnings']} |"
        )

    by_label = {case["label"]: case for case in cases}
    for load in ("low", "high"):
        consistent = by_label[f"{load}-consistent"]
        guardrails = by_label[f"{load}-guardrailspick"]
        aggregate_delta = 100.0 * (
            guardrails["warm_mean_ms"] / consistent["warm_mean_ms"] - 1.0
        )
        lines.extend([
            "",
            f"## {load.capitalize()} load: Guardrails-pick versus consistent hashing",
            "",
            f"Invocation-weighted warm mean delta: **{aggregate_delta:+.2f}%**.",
            "",
            "| Function | Invocations | Consistent mean (ms) | Guardrails mean (ms) | Mean delta | Consistent p95 (ms) | Guardrails p95 (ms) |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ])
        for function in sorted(consistent["functions"]):
            base = consistent["functions"][function]
            candidate = guardrails["functions"][function]
            delta = 100.0 * (candidate["mean_ms"] / base["mean_ms"] - 1.0)
            lines.append(
                f"| {function} | {base['invocations']} | {base['mean_ms']:.3f} "
                f"| {candidate['mean_ms']:.3f} | {delta:+.2f}% "
                f"| {base['p95_ms']:.3f} | {candidate['p95_ms']:.3f} |"
            )

        historical = by_label.get(f"{load}-cfs-old")
        if historical is not None:
            consistent_delta = 100.0 * (
                consistent["warm_mean_ms"] / historical["warm_mean_ms"] - 1.0
            )
            guardrails_delta = 100.0 * (
                guardrails["warm_mean_ms"] / historical["warm_mean_ms"] - 1.0
            )
            lines.extend([
                "",
                "### Historical CFS reference",
                "",
                "This baseline is from an earlier run and is not a same-time control.",
                "",
                f"- Consistent hashing versus old CFS warm mean: **{consistent_delta:+.2f}%**.",
                f"- Guardrails-pick versus old CFS warm mean: **{guardrails_delta:+.2f}%**.",
                "",
                "| Function | Old CFS mean (ms) | Consistent delta | Guardrails delta | Old CFS p95 (ms) |",
                "|---|---:|---:|---:|---:|",
            ])
            for function in sorted(historical["functions"]):
                cfs = historical["functions"][function]
                consistent_function = consistent["functions"][function]
                guardrails_function = guardrails["functions"][function]
                consistent_function_delta = 100.0 * (
                    consistent_function["mean_ms"] / cfs["mean_ms"] - 1.0
                )
                guardrails_function_delta = 100.0 * (
                    guardrails_function["mean_ms"] / cfs["mean_ms"] - 1.0
                )
                lines.append(
                    f"| {function} | {cfs['mean_ms']:.3f} | {consistent_function_delta:+.2f}% "
                    f"| {guardrails_function_delta:+.2f}% | {cfs['p95_ms']:.3f} |"
                )

    lines.extend(["", "## Result directories", ""])
    lines.extend(f"- {case['label']}: `{case['directory']}`" for case in cases)
    return "\n".join(lines) + "\n"


def main() -> int:
    args = arguments()
    parsed_cases = []
    for specification in args.case:
        if "=" not in specification:
            raise ValueError(f"invalid --case value: {specification}")
        label, directory = specification.split("=", 1)
        parsed_cases.append(analyze(label, Path(directory)))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    markdown = render(parsed_cases)
    (args.output_dir / "comparison.md").write_text(markdown)
    (args.output_dir / "comparison.json").write_text(json.dumps(parsed_cases, indent=2) + "\n")
    print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
