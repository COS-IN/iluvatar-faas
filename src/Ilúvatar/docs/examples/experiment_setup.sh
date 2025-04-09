#!/bin/bash

# This script prepares dependencies and configuration to run the samples on the landing page and in this directory.
# It is expecting to be run in the same directory as it is located.
# You should have already ran the `setup.sh` script in the parent directory to install dependencies for the worker and load generator

venv_name="examples-venv"


cargo install cross --git https://github.com/cross-rs/cross
python3 -m venv --clear $venv_name
source ./$venv_name/bin/activate
python3 -m pip install --upgrade pip --no-warn-script-location
python3 -m pip install -r ../../../load/reqs.txt --no-warn-script-location
python3 -m pip install jupyterlab psutil
deactivate

name=$(ip route get 8.8.8.8 | awk '{ print $5; exit }')

local_json="../../iluvatar_worker/src/worker.dev.json"
cp ../../iluvatar_worker/src/worker.json $local_json
jq ".networking.hardware_interface = \"$name\"" $local_json > tmp.json && mv tmp.json $local_json
jq ".container_resources.snapshotter = \"overlayfs\"" $local_json > tmp.json && mv tmp.json $local_json
jq ".influx.enabled = false" $local_json > tmp.json && mv tmp.json $local_json

local_json="../../iluvatar_controller/src/controller.dev.json"
cp ../../iluvatar_controller/src/controller.json $local_json
jq ".influx.enabled = false" $local_json > tmp.json && mv tmp.json $local_json

cat <<EOT > ../../ansible/group_vars/local_addresses.yml
servers:
  localhost:
    internal_ip: 127.0.0.1
    ipmi_ip: 127.0.0.1
    hardware_interface: "$name"
  127.0.0.1:
    internal_ip: 127.0.0.1
    ipmi_ip: 127.0.0.1
    hardware_interface: "$name"
EOT

ILU_HOME="../.."
ret=$(pwd)
cd $ILU_HOME
make release
cd $ret