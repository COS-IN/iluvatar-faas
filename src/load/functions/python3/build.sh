#!/bin/bash
# Build and pushes the CPU-only functions into images

REPO="alfuerst"
VERSION="latest"
PUSH=true

for i in "$@"
do
case $i in
    --repo=*)
    REPO="${i#*=}"
    ;;
    --version=*)
    VERSION="${i#*=}"
    ;;
    --skip-push)
    PUSH=false
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
  log="$PWD/$pth/build.log"
  if ! [ -f "$pth/Dockerfile" ]; then
    cp $docker_base $pth/Dockerfile
    cd $pth
    docker build --build-arg REPO=$REPO -t $img_name . &> $log || {
      echo "Failed to build $func_name, check $log";
      exit 1;
    }
    rm Dockerfile
    rm server.py
  else
    cp $docker_base $pth
    cd $pth
    docker build --build-arg REPO=$REPO -f $docker_base -t "$REPO/iluvatar-action-base:$VERSION" . &> $log || {
      echo "Failed to build action base, check $log";
      exit 1;
    }
    docker build --build-arg REPO=$REPO -f "Dockerfile" -t $img_name . &>> $log || {
      echo "Failed to build $func_name, check $log";
      exit 1;
    }
    rm $docker_base
    rm server.py
  fi
  if [ "$PUSH" = true ]; then
    docker push $img_name &>> $log || {
      echo "Failed to push $func_name, check $log";
      exit 1;
    }
  fi
}

for dir in ./functions/*/
do
  dir=${dir%*/}      # remove the trailing "/"
  # echo ${dir##*/}    # print everything after the final "/"
  func_name=${dir##*/}

  # if [[ "$func_name" == "cnn_image_classification" ]]; then
  build $dir $func_name &
  # fi
done

wait $(jobs -p)
