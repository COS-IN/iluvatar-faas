ARG ACTION_BASE=alfuerst/iluvatar-action-gpu-base
FROM nvidia/cuda:11.8.0-devel-ubuntu20.04 as build_img

# Only need if binaries use files in data directory, none currently do
# RUN apt update && apt install wget git -y && \
#     wget http://www.cs.virginia.edu/~skadron/lava/Rodinia/Packages/rodinia_3.1.tar.bz2 && \
#     tar -xvf rodinia_3.1.tar.bz2 && \
RUN apt update && apt install wget git -y && \
    git clone https://github.com/ivanradanov/rodinia.git

WORKDIR /rodinia

COPY my.host.make.config common/host.make.config
COPY openmp.host.make.config common/openmp.make.config

RUN git checkout 9ac922cc3b0dc8fcfd18506d68f917b726884a2b && \
    make all && \
    cd openmp/lavaMD && \
    make && \
    cd ../../

FROM ${ACTION_BASE}

WORKDIR /app

COPY --from=build_img /rodinia/cuda/srad/srad_v1/srad /rodinia/cuda/pathfinder/pathfinder /rodinia/cuda/nw/needle /rodinia/cuda/myocyte/myocyte.out /rodinia/cuda/backprop/backprop /rodinia/cuda/gaussian/gaussian /rodinia/cuda/lavaMD/lavaMD /rodinia/cuda/lud/cuda/lud_cuda ./cuda/

COPY --from=build_img /rodinia/openmp/srad/srad_v1/srad /rodinia/openmp/pathfinder/pathfinder /rodinia/openmp/nw/needle /rodinia/openmp/myocyte/myocyte.out /rodinia/openmp/backprop/backprop  /rodinia/openmp/lavaMD/lavaMD /rodinia/openmp/lud/omp/lud_omp ./openmp/

# COPY --from=build_img /rodinia_3.1/data ./data