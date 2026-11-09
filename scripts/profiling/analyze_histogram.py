#!/usr/bin/env python3

import argparse
import csv
import re
from pathlib import Path


HISTOGRAM = re.compile(r"^\s*((?:\[|\().*?(?:\)|\]))\s+([0-9]+)\s+\|")
MAP_VALUE = re.compile(r"^\s*@[^[]+\[([^]]+)\]:\s*([0-9]+)\s*$")


def last_dump(lines):
    dumps = []
    current = []
    for line in lines:
        if line.startswith("-------"):
            if current:
                dumps.append(current)
            current = []
        else:
            current.append(line)
    if current:
        dumps.append(current)
    return dumps[-1] if dumps else []


def main():
    parser = argparse.ArgumentParser(description="Convert the last bpftrace map or histogram dump to CSV")
    parser.add_argument("log", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    source = args.log.resolve()
    output = (args.output or source.with_suffix(".csv")).resolve()
    histogram_rows = []
    map_rows = []
    for line in last_dump(source.read_text().splitlines()):
        if match := HISTOGRAM.match(line):
            histogram_rows.append((match.group(1).strip(), int(match.group(2))))
        elif match := MAP_VALUE.match(line):
            map_rows.append((match.group(1).strip(), int(match.group(2))))

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as stream:
        if histogram_rows:
            total = sum(count for _, count in histogram_rows)
            writer = csv.writer(stream)
            writer.writerow(["bucket", "count", "probability"])
            writer.writerows((bucket, count, count / total if total else 0.0) for bucket, count in histogram_rows)
        elif map_rows:
            writer = csv.writer(stream)
            writer.writerow(["key", "value"])
            writer.writerows(map_rows)
        else:
            parser.error(f"no bpftrace histogram or map values found in {source}")
    print(f"Processed profile written to {output}")


if __name__ == "__main__":
    main()
