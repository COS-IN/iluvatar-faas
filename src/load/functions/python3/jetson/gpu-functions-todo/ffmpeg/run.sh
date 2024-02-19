#!/bin/bash

./ffmpeg  \
 -y  \
 -vsync 0  \
 -hwaccel  cuda  \
 -hwaccel_output_format cuda  \
 -i  SampleVideo_1280x720_10mb.mp4  \
 -c:a  copy  \
 -c:v  h264_nvenc  \
 -preset  p6  \
 -tune  ll  \
 -b:v  5M  \
 -bufsize  5M  \
 -maxrate  10M  \
 -qmin  1  \
 -qmax  50  \
 -g  250  \
 -bf  3  \
 -b_ref_mode  middle  \
 -temporal-aq  1  \
 -rc-lookahead  20  \
 -i_qfactor  0.75  \
 -b_qfactor  1.1  \
 output.mp4

