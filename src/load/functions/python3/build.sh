#!/bin/bash
# Build and pushes the CPU-only functions into images

REPO="alfuerst"
VERSION="latest"

for i in "$@"
do
case $i in
    --repo=*)
    REPO="${i#*=}"
    ;;
    --version=*)
    VERSION="${i#*=}"
    ;;
    *)
    # unknown option
    ;;
esac
done

build() {
  pth=$1
  func_name=$2
  docker_base="Dockerfile.base"
  cp server.py $pth
  back=$PWD

  img_name=$REPO/$func_name-iluvatar-action:$VERSION

  if ! [ -f "$pth/Dockerfile" ]; then
    cp $docker_base $pth/Dockerfile
    cd $pth
    docker build -t $img_name .
    rm Dockerfile
    rm server.py
  else
    cp $docker_base $pth
    cd $pth
    docker build -f $docker_base -t "$REPO/iluvatar-action-base:$VERSION" .
    docker build -f "Dockerfile" -t $img_name .
    rm $docker_base
    rm server.py
  fi
  docker push $img_name
  cd $back
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

