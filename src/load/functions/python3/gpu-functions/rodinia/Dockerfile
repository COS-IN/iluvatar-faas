ARG REPO=alfuerst
FROM nvidia/cuda:11.8.0-devel-ubuntu20.04 as build_img

# Only need if binaries use files in data directory, none currently do
# RUN apt update && apt install wget git -y && \
#     wget http://www.cs.virginia.edu/~skadron/lava/Rodinia/Packages/rodinia_3.1.tar.bz2 && \
#     tar -xvf rodinia_3.1.tar.bz2 && \
RUN apt update && apt install wget git -y && \
    git clone https://github.com/ivanradanov/rodinia.git

WORKDIR /rodinia

COPY my.host.make.config common/host.make.config

RUN git checkout 9ac922cc3b0dc8fcfd18506d68f917b726884a2b && \
    make TIMED_CUDA

FROM ${REPO}/iluvatar-action-gpu-base

COPY --from=build_img /rodinia/cuda/srad/srad_v1/srad /rodinia/cuda/pathfinder/pathfinder /rodinia/cuda/nw/needle /rodinia/cuda/myocyte/myocyte.out /rodinia/cuda/backprop/backprop /rodinia/cuda/gaussian/gaussian /rodinia/cuda/lavaMD/lavaMD /rodinia/cuda/lud/cuda/lud_cuda /usr/bin/

# COPY --from=build_img /rodinia_3.1/data ./data