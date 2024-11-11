ARG ACTION_BASE=alfuerst/iluvatar-action-gpu-base
FROM ${ACTION_BASE}:aarch64

RUN apt-get update -y && apt-get upgrade -y && apt-get install -y ffmpeg && apt-get clean
