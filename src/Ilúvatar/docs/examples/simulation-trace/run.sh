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

ILUVATAR_CONTROLLER__logging__directory=$ret ILUVATAR_WORKER__logging__directory=$ret $ILU_HOME/target/x86_64-unknown-linux-gnu/release/iluvatar_load_gen trace --out-folder $results_dir --port $PORT --host $host --target 'controller' --setup 'simulation' \
  --load-type 'functions' --input-csv ./four-functions.csv --metadata-csv ./four-functions-metadata.csv --prewarms 1 \
  --worker-config "$ILU_HOME/iluvatar_worker/src/worker.dev.json" --controller-config "$ILU_HOME/iluvatar_controller/src/controller.dev.json" --workers 3 \
  --function-data ../benchmark/worker_function_benchmarks.json >> $log_file
