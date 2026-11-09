#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path

try:
    from storage import Storage
except ModuleNotFoundError as error:
    if error.name == "minio":
        raise SystemExit("Python package 'minio' is required: python3 -m pip install minio") from error
    raise


def function_files(benchmarks):
    return sorted(benchmarks.glob("**/python/function.py"))


def prepare_benchmark(function_file, benchmarks, benchmark_data, storage):
    function_dir = function_file.parent
    benchmark_dir = function_dir.parent
    data_path = benchmark_data / benchmark_dir.relative_to(benchmarks)
    input_module = benchmark_dir / "input.py"
    default_input = function_dir / "default_input.json"
    bucket_name = benchmark_dir.name

    namespace = {}
    exec(compile(input_module.read_text(), str(input_module), "exec"), namespace)

    def upload_to_storage(bucket_index, key, filepath):
        del bucket_index
        storage.upload(bucket_name, key, filepath)

    input_config = namespace["generate_input"](
        str(data_path),
        size="small",
        benchmarks_bucket=bucket_name,
        input_paths=["", ""],
        output_paths=["exp1"],
        upload_func=upload_to_storage,
        nosql_func=None,
    )
    default_input.write_text(json.dumps(input_config, indent=4) + "\n")
    print(f"Prepared {bucket_name}")


def main():
    parser = argparse.ArgumentParser(description="Upload SEBS benchmark inputs to MinIO")
    parser.add_argument("--benchmarks", type=Path, required=True)
    parser.add_argument("--benchmark-data", type=Path, required=True)
    parser.add_argument("--minio-address", default="127.0.0.1:9000")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    args = parser.parse_args()

    benchmarks = args.benchmarks.resolve()
    benchmark_data = args.benchmark_data.resolve()
    if not benchmarks.is_dir() or not benchmark_data.is_dir():
        parser.error("--benchmarks and --benchmark-data must both be directories")

    os.environ["MINIO_ADDRESS"] = args.minio_address
    os.environ["MINIO_ACCESS_KEY"] = args.access_key
    os.environ["MINIO_SECRET_KEY"] = args.secret_key
    storage = Storage()

    files = function_files(benchmarks)
    if not files:
        parser.error(f"no SEBS functions found under {benchmarks}")
    for function_file in files:
        prepare_benchmark(function_file, benchmarks, benchmark_data, storage)


if __name__ == "__main__":
    main()
