#!/bin/bash 

img="test:latest"

docker build -t $img ./ 
docker run \
    --rm \
    -it  \
    --runtime nvidia \
    $img

