#!/bin/bash

ILU_HOME="../../.."
CORES=2
MEMORY=4096

results_dir="."
worker_log_dir="/tmp/ilúvatar/logs/ansible"
environment='local'
hosts="-e @$ILU_HOME/ansible/group_vars/local_addresses.yml"
host_file="$ILU_HOME/ansible/environments/$environment/hosts.ini"
host="127.0.0.1"
log_file="$results_dir/orchestration.log"

ret=$(pwd)
cd $ILU_HOME
make release
cd $ret

echo "Running benchmark"
source ../examples-venv/bin/activate

cleanup(){
  echo "cleanup"
  cp $worker_log_dir/* $results_dir >> $log_file
  # remove system parts
  ansible-playbook -i $host_file $ILU_HOME/ansible/worker.yml -e mode=clean $hosts  >> $log_file
}

user_interrupt() {
  cleanup 
  exit 0
}
trap user_interrupt 2

# clean worker, start worker, start load_gen
ansible-playbook -i $host_file $ILU_HOME/ansible/worker.yml -e worker_log_dir=$worker_log_dir $hosts -e mode=clean > $log_file &&
ansible-playbook -i $host_file $ILU_HOME/ansible/worker.yml $hosts -e mode=deploy -e worker_memory_mb=$MEMORY \
    -e worker_cores=$CORES -e worker_status_ms=500 -e worker_memory_buffer=1024 -e worker_queue_policy="fcfs" \
    -e influx_enabled=false -e worker_log_dir=$worker_log_dir -e worker_snapshotter='overlayfs' >> $log_file &&
$ILU_HOME/target/release/ilúvatar_load_gen benchmark --out-folder $results_dir --port 8070 --host $host --cold-iters 1 --warm-iters 2 --target worker --function-file $ILU_HOME/target/release/resources/cpu-benchmark.csv >> $log_file

sleep 30
cleanup
deactivate