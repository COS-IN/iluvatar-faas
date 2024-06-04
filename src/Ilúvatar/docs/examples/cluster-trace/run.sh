#!/bin/bash

./generate-trace.sh

ILU_HOME="../../.."
CORES=2
MEMORY=4096
host="127.0.0.1"
PORT=8080

results_dir="."
worker_log_dir="/tmp/iluvatar/logs/ansible"
environment='sshd'
hosts="-e @../../../ansible/group_vars/local_addresses.yml"
host_file="../../../ansible/environments/$environment/hosts.ini"
log_file="$results_dir/orchestration.log"

ret=$(pwd)
cd $ILU_HOME
make release
cd $ret

passphrase="cluster-trace-example"
ssh_key="./example-ssh"
rm -f $ssh_key
ssh-keygen -b 4096 -f $ssh_key -P $passphrase -q

pub_key=$(cat "$ssh_key.pub")
# add ssh key to authorized
echo $pub_key >> ~/.ssh/authorized_keys

cleanup(){
  echo "cleanup"
  sshpass -p $passphrase scp -i $ssh_key $host:$worker_log_dir/* $results_dir >> $log_file
  # remove system parts
  sshpass -p $passphrase ansible-playbook --private-key=$ssh_key --ask-pass  -i $host_file $ILU_HOME/ansible/iluvatar.yml -e mode=clean $hosts >> $log_file
}

user_interrupt() {
  cleanup 
  exit 0
}
trap user_interrupt 2

echo "Running cluster-trace"
source ../examples-venv/bin/activate

# clean worker, start worker, start load_gen
sshpass -p $passphrase ansible-playbook --private-key=$ssh_key --ask-pass -i $host_file $ILU_HOME/ansible/iluvatar.yml -e worker_log_dir=$worker_log_dir \
   -e controller_log_dir=$worker_log_dir $hosts -e mode=clean > $log_file &&
sshpass -p $passphrase ansible-playbook --private-key=$ssh_key --ask-pass -i $host_file $ILU_HOME/ansible/iluvatar.yml $hosts \
  -e mode=deploy $debug -e worker_memory_mb=$MEMORY -e worker_cores=$CORES -e controller_log_dir=$worker_log_dir \
  -e worker_status_ms=5000 -e worker_memory_buffer=512 -e worker_queue_policy="fcfs" -e worker_snapshotter='overlayfs' \
  -e influx_enabled=true -e worker_log_dir=$worker_log_dir -e controller_port=$PORT >> $log_file &&
sleep 15 &&
$ILU_HOME/target/x86_64-unknown-linux-gnu/release/iluvatar_load_gen trace --out-folder $results_dir --port $PORT --host $host --target 'controller' --setup 'live' \
  --load-type functions --input-csv ./four-functions.csv --metadata-csv ./four-functions-metadata.csv --prewarms 1 --function-data ../benchmark/worker_function_benchmarks.json &>> $log_file

sleep 30
cleanup

# remove temp key
grep -v "$pub_key" ~/.ssh/authorized_keys > tmp_authorized_keys
mv tmp_authorized_keys ~/.ssh/authorized_keys
deactivate