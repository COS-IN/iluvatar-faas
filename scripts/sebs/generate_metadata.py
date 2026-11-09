#!/usr/bin/env python3

import argparse
import csv
import json
from pathlib import Path


def benchmark_directories(root: Path):
    for function_file in sorted(root.glob("**/python/function.py")):
        yield function_file.parent.parent


def main():
    parser = argparse.ArgumentParser(description="Generate Iluvatar SEBS benchmark metadata")
    parser.add_argument("--benchmarks", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("metadata/sebs_meta.csv"))
    parser.add_argument("--image-repository", default="docker.io/aarehman")
    parser.add_argument("--isolation", default="DOCKER")
    parser.add_argument("--prewarms", type=int, default=10)
    args = parser.parse_args()

    rows = []
    for benchmark_dir in benchmark_directories(args.benchmarks.resolve()):
        config_path = benchmark_dir / "config.json"
        input_path = benchmark_dir / "python" / "default_input.json"
        if not config_path.is_file() or not input_path.is_file():
            continue
        config = json.loads(config_path.read_text())
        name = benchmark_dir.name
        rows.append(
            {
                "name": name,
                "image_name": f"{args.image_repository}/{name}-iluvatar-action:latest",
                "compute": "CPU",
                "isolation": args.isolation,
                "memory": config["memory"],
                "args": "",
                "args_jsonfile": str(input_path.resolve()),
                "prewarms": args.prewarms,
                "burst_factor": 0.5,
                "burst_request_count": 0,
                "burst_count": 0,
                "base_factor": 1.0,
            }
        )

    if not rows:
        parser.error(f"no SEBS functions found under {args.benchmarks}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} functions to {args.output}")


if __name__ == "__main__":
    main()
