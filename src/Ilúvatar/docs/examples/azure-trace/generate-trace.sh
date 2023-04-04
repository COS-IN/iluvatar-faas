#!/bin/bash

azure_dir="azure"
mkdir -p $azure_dir
if [[ ! -f "$azure_dir/invocations_per_function_md.anon.d01.csv" ]]; then
  python3 ../../../../load/generation/azure/download.py --out-folder $azure_dir
fi

python3 ../../../../load/generation/azure/ecdf_trace.py --out-folder $azure_dir --data-path $azure_dir --num-funcs 2 --duration 5 --seed 11
