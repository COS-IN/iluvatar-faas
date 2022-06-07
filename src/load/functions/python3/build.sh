#!/bin/bash

build() {
  pth=$1
  func_name=$2
  docker_base="Dockerfile.base"
  cp server.py $pth

  if ! [ -f "$pth/Dockerfile" ]; then
    cp $docker_base $pth/Dockerfile
    cd $pth
    docker build -t "alfuerst/$func_name-il-action" .
    rm Dockerfile
    rm server.py
    cd ../../
  else
    cp $docker_base $pth
    cd $pth
    docker build -f $docker_base -t "alfuerst/il-action-base" .
    docker build -f "Dockerfile" -t "alfuerst/$func_name-il-action" .
    rm $docker_base
    rm server.py
    cd ../../
  fi
}

for dir in ./functions/*/
do
    dir=${dir%*/}      # remove the trailing "/"
    # echo ${dir##*/}    # print everything after the final "/"
    func_name=${dir##*/}

    build $dir $func_name
done

