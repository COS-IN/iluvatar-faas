#!/bin/bash

ILU_HOME="../../.."
CORES=2
MEMORY=4096

results_dir=$(pwd)
worker_log_dir=$results_dir
environment='local'
hosts="-e @../../../ansible/group_vars/local_addresses.yml"
host_file="../../../ansible/environments/$environment/hosts.ini"
host="127.0.0.1"
log_file="$results_dir/orchestration.log"

pushd $ILU_HOME
make release
popd

echo "Running scaling"
source ../examples-venv/bin/activate

cleanup(){
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
    -e worker_cores=$CORES -e worker_status_ms=500 -e worker_memory_buffer=1024 -e worker_queue_policy="fcfs" -e worker_snapshotter='overlayfs' \
    -e influx_enabled=false -e worker_log_dir=$worker_log_dir >> $log_file &&
$ILU_HOME/target/x86_64-unknown-linux-gnu/release/iluvatar_load_gen scaling --out-folder $results_dir --port 8070 --host $host --target worker --start 1 \
    --end 4 --image docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest --memory-mb 1024 --compute cpu --isolation containerd --duration=60 >> $log_file

sleep 30
cleanup
deactivate