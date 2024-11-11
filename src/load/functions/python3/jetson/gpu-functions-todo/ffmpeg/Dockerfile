ARG ACTION_BASE=alfuerst/iluvatar-action-gpu-base

# FROM nvidia/cuda:11.8.0-devel-ubuntu20.04 as build_img
FROM nvcr.io/nvidia/l4t-tensorflow:r35.3.1-tf2.11-py3 as build_img

RUN apt-get update -qq && apt-get upgrade -y && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  autoconf \
  automake \
  build-essential \
  cmake \
  git-core \
  libass-dev \
  libfreetype6-dev \
  libgnutls28-dev \
  libmp3lame-dev \
  libtool \
  libvorbis-dev \
  meson \
  ninja-build \
  pkg-config \
  texinfo \
  wget \
  yasm \
  zlib1g-dev \
  git \
  libc6 \
  libc6-dev \
  unzip \
  libnuma1 \
  libnuma-dev \
  libunistring-dev \
  libaom-dev && \
  git clone https://git.videolan.org/git/ffmpeg/nv-codec-headers.git && \
  cd nv-codec-headers && \
  git checkout n11.1.5.3 && \
  make && \
  make install

WORKDIR /build/ffmpeg
RUN git clone https://git.ffmpeg.org/ffmpeg.git . && \
  ./configure --prefix=/build --enable-nonfree --enable-cuda-nvcc --enable-ffnvcodec --enable-nvenc --enable-libnpp \
  --extra-cflags=-I/usr/local/cuda/include --extra-ldflags=-L/usr/local/cuda/lib64 && \
  make -j 10 && \
  make install && \
  wget https://raw.githubusercontent.com/ddps-lab/serverless-faas-workbench/master/dataset/video/SampleVideo_1280x720_10mb.mp4
# wget https://ia902303.us.archive.org/14/items/sample-video-file-100-mb/Sample_Video_File_100MB.mp4


FROM ${ACTION_BASE}:aarch64
WORKDIR /app

# matches driver version in 11.8 container
RUN apt-get update && apt-get install -y libnvidia-encode-535-server

# /build/ffmpeg/Sample_Video_File_100MB.mp4
COPY --from=build_img /build/ffmpeg/ffmpeg /build/ffmpeg/SampleVideo_1280x720_10mb.mp4 /app/
COPY ./run.sh /app/

# wget https://ia902303.us.archive.org/14/items/sample-video-file-100-mb/Sample_Video_File_100MB.mp4
# /app/ffmpeg -y -vsync 0 -hwaccel cuda -hwaccel_output_format cuda -i SampleVideo_1280x720_10mb.mp4 -c:a copy -c:v h264_nvenc -preset p6 -tune ll -b:v 5M -bufsize 5M -maxrate 10M -qmin 1 -qmax 50 -g 250 -bf 3 -b_ref_mode middle -temporal-aq 1 -rc-lookahead 20 -i_qfactor 0.75 -b_qfactor 1.1 output.mp4

# /app/ffmpeg -y -vsync 0 -i SampleVideo_1280x720_10mb.mp4 -c:a copy -preset p6 -tune ll -b:v 5M -bufsize 5M -maxrate 10M -qmin 1 -qmax 50 -g 250 -bf 3 -b_ref_mode middle -temporal-aq 1 -rc-lookahead 20 -i_qfactor 0.75 -b_qfactor 1.1 output.mp4
