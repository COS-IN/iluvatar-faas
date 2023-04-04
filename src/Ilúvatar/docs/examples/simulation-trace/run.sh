#!/bin/bash

./generate-trace.sh

ILU_HOME="../../.."
CORES=2
MEMORY=4096
host="127.0.0.1"
PORT=8080

results_dir="."
log_file="$results_dir/orchestration.log"

ret=$(pwd)
cd $ILU_HOME
make release
cd $ret

$ILU_HOME/target/release/ilúvatar_load_gen trace --out-folder $results_dir --port $PORT --host $host --target 'controller' --setup 'simulation' \
  --load-type 'functions' --input-csv ./four-functions.csv --metadata-csv ./four-functions-metadata.csv --prewarms 1 \
  --worker-config worker.json --controller-config controller.json --workers 3 \
  --function-data ../benchmark/worker_function_benchmarks.json >> $log_file
