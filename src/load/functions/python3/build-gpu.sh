#!/bin/bash

build() {
  pth=$1
  func_name=$2
  docker_base="Dockerfile.gpu"
  cp server.py $pth
  back=$PWD

  cp $docker_base $pth/Dockerfile
  cd $pth
  docker build -t "alfuerst/$func_name-iluvatar-gpu:latest" . &> build.log
  rm Dockerfile
  rm server.py
  docker push "alfuerst/$func_name-iluvatar-gpu:latest" &>> build.log
  cd $back
}

for dir in ./gpu-functions/*/
do
    dir=${dir%*/}      # remove the trailing "/"
    # echo ${dir##*/}    # print everything after the final "/"
    func_name=${dir##*/}
    build $dir $func_name &
done

wait $(jobs -p)
