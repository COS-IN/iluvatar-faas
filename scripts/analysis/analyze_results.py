#!/usr/bin/env python3

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def parse_timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def select_file(explicit, result_dir, candidates, required=True):
    if explicit:
        path = explicit.resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        return path
    for name in candidates:
        path = result_dir / name
        if path.is_file():
            return path
    if required:
        raise FileNotFoundError(f"none of {', '.join(candidates)} found in {result_dir}")
    return None


def write_lines(path, lines):
    path.write_text("".join(lines))


def parse_worker_log(worker_log, output_dir):
    selected = defaultdict(list)
    status_rows = []
    invalid_lines = 0
    message_outputs = {
        "cpu utilization": "cpu_utilization.log",
        "current load status": "cpu_utilization.log",
        "current queue info": "dispatcher_invoker.log",
        "domain released for function cgroup": "domain_released.log",
        "domain assigned to function cgroup": "domain_assigned.log",
    }

    with worker_log.open() as stream:
        for line in stream:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                invalid_lines += 1
                continue
            fields = entry.get("fields", {})
            message = str(fields.get("message", "")).lower()
            for marker, filename in message_outputs.items():
                if marker in message:
                    selected[filename].append(line)

            status_value = fields.get("cpu_util", fields.get("status"))
            if status_value is not None:
                if isinstance(status_value, str):
                    try:
                        status_value = json.loads(status_value)
                    except json.JSONDecodeError:
                        continue
                if isinstance(status_value, dict):
                    status_rows.append((entry.get("timestamp"), status_value))
            queue_value = fields.get("queue_info")
            if isinstance(queue_value, str):
                try:
                    queue_value = json.loads(queue_value)
                except json.JSONDecodeError:
                    queue_value = None
            if isinstance(queue_value, dict):
                queue_status = {
                    key: queue_value[key]
                    for key in ("num_running_funcs", "num_cold_starting_funcs")
                    if key in queue_value
                }
                cpu_queue = queue_value.get("queues", {}).get("CPU", {})
                if isinstance(cpu_queue, dict) and "len" in cpu_queue:
                    queue_status["cpu_queue_len"] = cpu_queue["len"]
                status_rows.append((entry.get("timestamp"), queue_status))

    for filename in set(message_outputs.values()):
        write_lines(output_dir / filename, selected[filename])
    return status_rows, invalid_lines, {name: len(lines) for name, lines in selected.items()}


def parse_load_gen_log(load_gen_log, output_dir):
    if load_gen_log is None:
        return None
    first = None
    invoke_start = None
    last = None
    with load_gen_log.open() as stream:
        for line in stream:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            timestamp = entry.get("timestamp")
            if not timestamp:
                continue
            first = first or timestamp
            last = timestamp
            if "starting invocations" in str(entry.get("fields", {}).get("message", "")).lower():
                invoke_start = timestamp
    if first is None or last is None:
        return None
    invoke_start = invoke_start or first
    timing = {
        "very_start": first,
        "invoke_start": invoke_start,
        "very_end": last,
        "total_seconds": (parse_timestamp(last) - parse_timestamp(first)).total_seconds(),
        "prewarm_seconds": (parse_timestamp(invoke_start) - parse_timestamp(first)).total_seconds(),
        "trace_seconds": (parse_timestamp(last) - parse_timestamp(invoke_start)).total_seconds(),
    }
    with (output_dir / "load_gen_timing.txt").open("w") as stream:
        for key, value in timing.items():
            stream.write(f"{key}: {value}\n")
    return timing


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def numeric(row, key):
    try:
        return float(row[key])
    except (KeyError, TypeError, ValueError):
        return None


def is_true(value):
    return str(value).strip().lower() == "true"


def summarize_invocations(invocations, output_dir):
    with invocations.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    successful = [row.copy() for row in rows if is_true(row.get("success"))]
    failed = [row for row in rows if not is_true(row.get("success"))]
    with (output_dir / "failed_invocations.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys() if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(failed)

    for row in successful:
        cpu_utilization = numeric(row, "cpu_utilization")
        code_duration = numeric(row, "code_duration_sec")
        e2e_duration = numeric(row, "e2e_duration_us")
        if cpu_utilization is not None and code_duration is not None:
            row["cpu_time_us"] = cpu_utilization * code_duration * 1_000_000
        if cpu_utilization is not None and e2e_duration is not None:
            row["invocation_cost_us"] = cpu_utilization * e2e_duration

    grouped = defaultdict(list)
    for row in successful:
        grouped[row["function_name"]].append(row)
    metrics = [
        "worker_duration_us",
        "code_duration_sec",
        "e2e_duration_us",
        "cpu_utilization",
        "cpu_time_us",
        "invocation_cost_us",
    ]
    summary_rows = []
    for function_name, function_rows in sorted(grouped.items()):
        classes = {
            "all": function_rows,
            "cold": [row for row in function_rows if is_true(row.get("was_cold"))],
            "warm": [row for row in function_rows if not is_true(row.get("was_cold"))],
        }
        for invocation_class, class_rows in classes.items():
            summary = {
                "function_name": function_name,
                "invocation_class": invocation_class,
                "count": len(class_rows),
            }
            for metric in metrics:
                values = [value for row in class_rows if (value := numeric(row, metric)) is not None]
                summary[f"{metric}_mean"] = statistics.fmean(values) if values else 0.0
                summary[f"{metric}_std"] = statistics.pstdev(values) if values else 0.0
                summary[f"{metric}_p99"] = percentile(values, 0.99)
            summary_rows.append(summary)

    fields = list(summary_rows[0]) if summary_rows else ["function_name", "invocation_class", "count"]
    with (output_dir / "function_summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summary_rows)
    return {"total": len(rows), "successful": len(successful), "failed": len(failed)}


def summarize_status(status_rows, timing, output_dir):
    if timing:
        start = parse_timestamp(timing["invoke_start"])
        end = parse_timestamp(timing["very_end"])
        status_rows = [
            (timestamp, status)
            for timestamp, status in status_rows
            if timestamp and start <= parse_timestamp(timestamp) <= end
        ]
    values = defaultdict(list)
    for _, status in status_rows:
        for key, value in status.items():
            if isinstance(value, (int, float)) and value is not None:
                values[key].append(float(value))
        if isinstance(status.get("cpu_id"), (int, float)):
            values["cpu_busy"].append(100.0 - float(status["cpu_id"]))
    with (output_dir / "status_summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["metric", "mean", "std", "samples"])
        writer.writeheader()
        for metric, samples in sorted(values.items()):
            writer.writerow(
                {
                    "metric": metric,
                    "mean": statistics.fmean(samples),
                    "std": statistics.pstdev(samples),
                    "samples": len(samples),
                }
            )
    return len(status_rows)


def read_makespan(result_dir):
    start_names = ["trace_start_time_including_prewarm.txt", "trace_start_time.txt"]
    start = next((result_dir / name for name in start_names if (result_dir / name).is_file()), None)
    end = result_dir / "trace_end_time.txt"
    if start and end.is_file():
        return int(end.read_text().splitlines()[0]) - int(start.read_text().splitlines()[0])
    return None


def main():
    parser = argparse.ArgumentParser(description="Analyze a local Iluvatar experiment result directory")
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--worker-log", type=Path)
    parser.add_argument("--invocations", type=Path)
    parser.add_argument("--load-gen-log", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    result_dir = args.result_dir.resolve()
    if not result_dir.is_dir():
        parser.error(f"result directory does not exist: {result_dir}")
    output_dir = (args.output_dir or result_dir / "analysis").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        worker_log = select_file(args.worker_log, result_dir, ["worker.log", "worker_worker1.log"])
        invocations = select_file(
            args.invocations,
            result_dir,
            ["output-funcs-in.csv", "benchmark-output.csv"],
        )
        load_gen_log = select_file(args.load_gen_log, result_dir, ["load_gen.log"], required=False)
    except FileNotFoundError as error:
        parser.error(str(error))

    status_rows, invalid_worker_lines, worker_events = parse_worker_log(worker_log, output_dir)
    timing = parse_load_gen_log(load_gen_log, output_dir)
    invocation_counts = summarize_invocations(invocations, output_dir)
    status_samples = summarize_status(status_rows, timing, output_dir)
    summary = {
        "result_dir": str(result_dir),
        "worker_log": str(worker_log),
        "invocations": str(invocations),
        "worker_events": worker_events,
        "invalid_worker_log_lines": invalid_worker_lines,
        "status_samples": status_samples,
        "invocations_summary": invocation_counts,
        "makespan_seconds": read_makespan(result_dir),
        "load_gen_timing": timing,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Analysis written to {output_dir}")


if __name__ == "__main__":
    main()
