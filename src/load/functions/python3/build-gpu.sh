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
  # echo "$pth -> $func_name" 
  docker_base="Dockerfile.gpu"
  back=$PWD
  cd $pth
  cp "$back/server.py" .
  cp "$back/$docker_base" .
  img_name=$REPO/$func_name-iluvatar-gpu:$VERSION

  if ! [ -f "Dockerfile" ]; then
    log="$PWD/build.log"
    docker build --build-arg REPO=$REPO -f $docker_base -t $img_name . &> $log || {
      echo "Failed to build $func_name, check $log";
      rm $docker_base
      rm server.py
      exit 1;
    }
  else
    log="$PWD/build.log"
    docker build --build-arg REPO=$REPO --file $docker_base -t "$REPO/iluvatar-action-gpu-base:$VERSION" . &> $log || {
      echo "Failed to build action base, check $log";
      rm $docker_base
      rm server.py
      exit 1;
    }
    echo "$pth -> $func_name $img_name $PWD `ls *.py`" 
    docker build --build-arg REPO=$REPO --file "Dockerfile" -t $img_name . &>> $log || {
      echo "Failed to build $func_name, check $log";
      rm $docker_base
      rm server.py
      exit 1;
    }
  fi
  rm $docker_base
  rm server.py

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
    build $dir $func_name &
done

wait $(jobs -p)
