#!/bin/bash

ILU_HOME=../../../
QUEUE="fcfs"
CORES=16
PREWARM=5
MEMORY=10240
LOAD_TYPE="functions"
ANSIBLE_ARGS=""
TRACE="./trace"

for i in "$@"
do
case $i in
    -h=*|--home=*)
    # home directory where Ilúvatar is, "src/Ilúvatar" in the 'iluvatar-faas' repo
    ILU_HOME="${i#*=}"
    ;;
    -t=*|--trace=*)
    # directory of the trace to run
    TRACE="${i#*=}"
    ;;
    -q=*|--queue=*)
    # directory of the trace to run
    QUEUE="${i#*=}"
    ;;
    -c=*|--cores=*)
    # directory of the trace to run
    CORES="${i#*=}"
    ;;
    -p=*|--prewarm=*)
    # directory of the trace to run
    PREWARM="${i#*=}"
    ;;
    -m=*|--memory=*)
    # directory of the trace to run
    MEMORY="${i#*=}"
    ;;
    *)
    # unknown option
    ;;
esac
done

target="target/debug"
config="$ILU_HOME/$target/worker.json"
benchmark="./worker_function_benchmarks.json"

ret=$(pwd)
cd $ILU_HOME
make 
cd $ret

results_base_dir="./results"

run_sim() {
  CORES=$1
  QUEUE=$2
  TRACE=$3

  TRACE_NAME=$(basename $TRACE)
  TRACE=$(realpath $TRACE)
  results_dir="$results_base_dir/$QUEUE/$CORES"

  input="$TRACE/$TRACE_NAME.csv"
  metadata="$TRACE/metadata-$TRACE_NAME.csv"
  log_file="$results_dir/orchestration.log"
  mkdir -p $results_dir

  if [ -f "$log_file" ]; then
    echo "Skipping $TRACE to $results_dir"
    # return
  else
    echo "Running $TRACE to $results_dir"
ILUVATAR_WORKER__logging__basename="worker_worker1" ILUVATAR_WORKER__logging__directory=$results_dir ILUVATAR_WORKER__container_resources__resource_map__cpu__count=$CORES ILUVATAR_WORKER__energy__log_folder=$results_dir ILUVATAR_WORKER__invocation__queue_policy=$QUEUE $ILU_HOME/$target/ilúvatar_load_gen trace --out-folder $results_dir --port 8070 --host 'simulation' --target 'worker' --setup 'simulation' --load-type $LOAD_TYPE --input-csv $input --metadata-csv $metadata --prewarms $PREWARM --worker-config $config --function-data ../benchmark/worker_function_benchmarks.json &> $log_file
  fi

python3 plot_status.py -l $results_dir -t $TRACE --out $results_dir
}

run_sim 0 $QUEUE $TRACE &
run_sim 20 $QUEUE $TRACE &
run_sim 18 $QUEUE $TRACE &

QUEUE="minheap"
run_sim 20 $QUEUE $TRACE &
run_sim 18 $QUEUE $TRACE &

wait $(jobs -p)