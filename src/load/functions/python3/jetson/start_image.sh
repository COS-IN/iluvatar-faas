#!/bin/bash

host_folder=/data2/worker_tmp/

func=fetch_1gfile
func=float_operation
func=cnn_image_classification_gpu
func=dd
func=pyaes
func=tf_imagenet

###########
# GPU Funcs

##
# Doesn't work!  
func=ffmpeg # doesn't work natively 
func=rodinia # doesn't build 

##
# works!   
# using new base https://catalog.ngc.nvidia.com/orgs/nvidia/containers/l4t-ml
func=onnx-roberta    
func=cupy   
func=tf_squeezenet
func=tf_imagenet  

img_name=aarehman/$func-iluvatar-action:v20
img_name=aarehman/$func-iluvatar-action:aarch64
img_name=aarehman/$func-iluvatar-gpu:aarch64

echo "Running container for: $img_name"

function cleanup() {
    docker container stop contr_0
    docker container stop contr_1

    wait $(jobs -p)
}

function run_image() {
    port=$1
    name=$2

    sudo docker run  \
              --name $name  \
              --rm  \
              --gpus all \
              --runtime nvidia \
              -v $(pwd)/temp/:/local/ \
              -e "GUNICORN_CMD_ARGS=--bind 0.0.0.0:$port"  \
              -p $port:$port  \
              --expose $port $img_name 
}

cleanup 

run_image 8080 contr_0 &
# run_image 8001 contr_1 &

trap cleanup SIGINT

wait $(jobs -p)

#          -v $host_folder:/host_folder \
