#!/bin/bash

host_folder=/data2/worker_tmp/

func=fetch_1gfile
func=pyaes
func=float_operation
func=dd

function cleanup() {
    sudo ctr task kill contr_0

    wait $(jobs -p)
}

function run_image() {
    port=$1
    name=$2
    
    sudo ctr run  \
              --rm  \
              --env "GUNICORN_CMD_ARGS=--bind 0.0.0.0:$port"  \
              --net-host  \
              docker.io/aarehman/$func-iluvatar-action:v20 \
              $name  
}

run_image 8080 contr_0 &
# run_image 8001 contr_1 &

trap cleanup SIGINT

wait $(jobs -p)

#          -v $host_folder:/host_folder \
