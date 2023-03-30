#!/bin/bash
# Builds and pushes the lookbusy image

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

name="$REPO/lookbusy-iluvatar-action:$VERSION"
docker build -t $name . || {
  echo "Failed to build lookbusy";
  exit 1;
}
docker push $name || {
  echo "Failed to build lookbusy";
  exit 1;
}
