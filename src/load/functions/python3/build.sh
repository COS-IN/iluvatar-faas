#!/bin/bash

build() {
  pth=$1
  func_name=$2
  docker_base="Dockerfile.base"
  cp server.py $pth
  back=$PWD

  if ! [ -f "$pth/Dockerfile" ]; then
    cp $docker_base $pth/Dockerfile
    cd $pth
    docker build -t "alfuerst/$func_name-iluvatar-action:latest" .
    rm Dockerfile
    rm server.py
    cd $back
  else
    cp $docker_base $pth
    cd $pth
    docker build -f $docker_base -t "alfuerst/iluvatar-action-base:latest" .
    docker build -f "Dockerfile" -t "alfuerst/$func_name-iluvatar-action:latest" .
    rm $docker_base
    rm server.py
    cd $back
  fi
}

for dir in ./functions/*/
do

    dir=${dir%*/}      # remove the trailing "/"
    # echo ${dir##*/}    # print everything after the final "/"
    func_name=${dir##*/}

    # if [[ "$func_name" == "cnn_image_classification" ]]; then
    build $dir $func_name
    # fi
done

