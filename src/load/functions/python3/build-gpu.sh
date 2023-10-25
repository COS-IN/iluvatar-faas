#!/bin/bash
# Build and pushes the GPU enabled functions into images

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
  docker_base="Dockerfile.gpu"
  cp server.py $pth
  cp $docker_base $pth
  img_name=$REPO/$func_name-iluvatar-gpu:$VERSION

  log="$(pwd)/$pth/build.log"
  if ! [ -f "$pth/Dockerfile" ]; then
    docker build --build-arg REPO=$REPO -f $docker_base -t $img_name $pth &> $log || {
      echo "Failed to build $func_name, check $log";
      rm "$pth/$docker_base"
      rm "$pth/server.py"
      exit 1;
    }
  else
    docker build --build-arg REPO=$REPO --file $docker_base -t "$REPO/iluvatar-action-gpu-base:$VERSION" $pth &> $log || {
      echo "Failed to build action base, check $log";
      rm "$pth/$docker_base"
      rm "$pth/server.py"
      exit 1;
    }
    docker build --build-arg REPO=$REPO --file "$pth/Dockerfile" -t $img_name $pth &>> $log || {
      echo "Failed to build $func_name, check $log";
      rm "$pth/$docker_base"
      rm "$pth/server.py"
      exit 1;
    }
  fi
  rm "$pth/$docker_base"
  rm "$pth/server.py"

  if [ "$PUSH" = true ]; then
    docker push "$REPO/$func_name-iluvatar-gpu:$VERSION" &>> $log || {
      echo "Failed to push $func_name, check $log";
      exit 1;
    }
  fi
}

for dir in ./gpu-functions/*/
do
    dir=${dir%*/}      # remove the trailing "/"
    # echo ${dir##*/}    # print everything after the final "/"
    func_name=${dir##*/}
  # if [[ "$func_name" == "rodinia" ]]; then
    build $dir $func_name &
  # fi
done

wait $(jobs -p)
