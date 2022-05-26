#!/bin/bash

build() {
  pth=$1
  func_name=$2

  cp Dockerfile.base $pth/Dockerfile
  cp server.py $pth
  cd $pth
  docker build -t "alfuerst/$func_name-il-action" .
  rm Dockerfile
  rm server.py
  cd ../../
}

for dir in ./functions/*/
do
    dir=${dir%*/}      # remove the trailing "/"
    # echo ${dir##*/}    # print everything after the final "/"
    func_name=${dir##*/}

    build $dir $func_name

done

