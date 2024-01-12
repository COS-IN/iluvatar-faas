#!/bin/bash

docker build  \
      --build-arg  \
      REPO=aarehman  \
      --file  ./gpu-functions/ffmpeg/Dockerfile  \
      -t  aarehman/ffmpeg-iluvatar-gpu:aarch64  \
      ./gpu-functions/ffmpeg

