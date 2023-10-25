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
  cp $docker_base $pth

  img_name=$REPO/$func_name-iluvatar-action:$VERSION
  log="$(pwd)/$pth/build.log"
  if ! [ -f "$pth/Dockerfile" ]; then
    docker build --build-arg REPO=$REPO -f $docker_base -t $img_name $pth &> $log || {
      echo "Failed to build $func_name, check $log";
      rm "$pth/$docker_base"
      rm "$pth/server.py"
      exit 1;
    }
  else
    docker build --build-arg REPO=$REPO -f $docker_base -t "$REPO/iluvatar-action-base:$VERSION" $pth &> $log || {
      echo "Failed to build action base, check $log";
      rm "$pth/$docker_base"
      rm "$pth/server.py"
      exit 1;
    }
    docker build --build-arg REPO=$REPO -f "$pth/Dockerfile" -t $img_name $pth &>> $log || {
      echo "Failed to build $func_name, check $log";
      rm "$pth/$docker_base"
      rm "$pth/server.py"
      exit 1;
    }
  fi
  if [ "$PUSH" = true ]; then
    docker push $img_name &>> $log || {
      echo "Failed to push $func_name, check $log";
      rm "$pth/$docker_base"
      rm "$pth/server.py"
      exit 1;
    }
  fi
  rm "$pth/$docker_base"
  rm "$pth/server.py"
}

for dir in ./functions/*/
do
  dir=${dir%*/}      # remove the trailing "/"
  # echo ${dir##*/}    # print everything after the final "/"
  func_name=${dir##*/}
  # if [[ "$func_name" == "rodinia" ]]; then
  build $dir $func_name &
  # fi
done

wait $(jobs -p)
