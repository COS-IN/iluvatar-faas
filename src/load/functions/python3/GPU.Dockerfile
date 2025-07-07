FROM nvidia/cuda:11.8.0-base-ubuntu22.04 AS build

RUN apt-get update && apt-get upgrade -y && apt-get install -y make gcc
COPY . .
RUN make -C driver-hooks

FROM nvcr.io/nvidia/tensorrt:22.12-py3 AS base
FROM nvidia/cuda:11.8.0-base-ubuntu22.04

COPY --from=base /usr/lib/x86_64-linux-gnu/libcu*.so* /usr/lib/x86_64-linux-gnu/
COPY --from=base /usr/local/cuda-11.8/targets/x86_64-linux/lib/ /usr/local/cuda-11.8/targets/x86_64-linux/lib/
# COPY --from=base /usr/lib/x86_64-linux-gnu/libnvinfer.so.8 /usr/lib/x86_64-linux-gnu/libnvinfer.so.8
# COPY --from=base /usr/lib/x86_64-linux-gnu/libnvinfer_plugin.so.8 /usr/lib/x86_64-linux-gnu/libnvinfer_plugin.so.8
# COPY --from=base /usr/lib/x86_64-linux-gnu/libcudnn.so.8 /usr/lib/x86_64-linux-gnu/libcudnn.so.8
# COPY --from=base /usr/lib/x86_64-linux-gnu/libcudnn_ops_infer.so.8 /usr/lib/x86_64-linux-gnu/libcudnn_ops_infer.so.8
# COPY --from=base /usr/lib/x86_64-linux-gnu/libcudnn_cnn_infer.so.8 /usr/lib/x86_64-linux-gnu/libcudnn_cnn_infer.so.8
# COPY --from=base /usr/lib/x86_64-linux-gnu/ /usr/lib/x86_64-linux-gnu/

# COPY --from=base /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcublas.so.11 /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcublas.so.11
# COPY --from=base /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcufft.so.10 /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcufft.so.10
# COPY --from=base /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcurand.so.10 /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcurand.so.10
# COPY --from=base /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcusolver.so.11 /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcusolver.so.11
# COPY --from=base /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcusparse.so.11 /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcusparse.so.11
# COPY --from=base /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcublasLt.so.11 /usr/local/cuda-11.8/targets/x86_64-linux/lib/libcublasLt.so.11

WORKDIR /app

# LD_PRELOAD=/app/libgpushare.so before app run
COPY --from=build driver-hooks/libgpushare.so .

COPY reqs.txt .
RUN apt-get update && apt-get upgrade -y && apt-get install -y python3 python3-pip \
  && python3 -m pip install --upgrade pip && python3 -m pip install flask gunicorn \
  && python3 -m pip install -r reqs.txt && python3 -m pip cache purge && apt-get clean && apt-get autoremove
COPY *.py .

# RUN ln -s /usr/lib/x86_64-linux-gnu/libnvinfer.so.8 /usr/lib/x86_64-linux-gnu/libnvinfer.so.7 && \
#   ln -s /usr/lib/x86_64-linux-gnu/libnvinfer_plugin.so.8 /usr/lib/x86_64-linux-gnu/libnvinfer_plugin.so.7 && \
#   ln -s /usr/lib/x86_64-linux-gnu/libcudnn_cnn_infer.so.8 /usr/lib/x86_64-linux-gnu/libcudnn_cnn_infer.so.7

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/:/usr/local/cuda-11.8/targets/x86_64-linux/lib/:/usr/local/lib/python3.8/dist-packages/nvidia/cublas/lib/:/usr/local/lib/python3.8/dist-packages/nvidia/cuda_runtime/lib/:/usr/local/lib/python3.8/dist-packages/nvidia/cudnn/lib/:/usr/local/lib/python3.8/dist-packages/nvidia/cufft/lib/:/usr/local/lib/python3.8/dist-packages/nvidia/curand/lib/:/usr/lib/x86_64-linux-gnu/"

ENTRYPOINT [ "gunicorn", "-w", "1", "server:app" ]
