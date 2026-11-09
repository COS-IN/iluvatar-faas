#!/usr/bin/env python3

import argparse
import csv
import statistics
from pathlib import Path


def parse_samples(path):
    samples = {}
    with path.open() as stream:
        for line in stream:
            fields = [field for field in line.strip().split(",") if field]
            if len(fields) < 3 or not fields[0].isdigit():
                continue
            for index in range(1, len(fields) - 1, 2):
                try:
                    entity = int(fields[index])
                    value = float(fields[index + 1])
                except ValueError:
                    continue
                samples.setdefault(entity, []).append(value)
    return samples


def main():
    parser = argparse.ArgumentParser(description="Summarize v-020-format bpftrace time-series logs")
    parser.add_argument("log_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    log_dir = args.log_dir.resolve()
    output = (args.output or log_dir / "bpftrace_summary.csv").resolve()
    rows = []
    for path in sorted(log_dir.glob("bpftrace_*.log")):
        for entity, values in sorted(parse_samples(path).items()):
            rows.append(
                {
                    "profile": path.stem.removeprefix("bpftrace_"),
                    "entity": entity,
                    "mean": statistics.fmean(values),
                    "std": statistics.pstdev(values),
                    "samples": len(values),
                }
            )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["profile", "entity", "mean", "std", "samples"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} profile summaries to {output}")


if __name__ == "__main__":
    main()
