#!/bin/bash
# Build and pushes the GPU enabled functions into images

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
  docker_base="Dockerfile.gpu"
  cp server.py $pth
  back=$PWD

  cp $docker_base $pth/Dockerfile
  cd $pth
  log="$PWD/build.log"
  docker build -t "$REPO/$func_name-iluvatar-gpu:$VERSION" . &> $log || {
    echo "Failed to build $func_name, check $log";
    exit 1;
  }
  rm Dockerfile
  rm server.py
  docker push "$REPO/$func_name-iluvatar-gpu:$VERSION" &>> $log || {
    echo "Failed to push $func_name, check $log";
    exit 1;
  }
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
