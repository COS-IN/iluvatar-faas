ARG ACTION_BASE=alfuerst/iluvatar-action-gpu-base
FROM ${ACTION_BASE}
WORKDIR /app

RUN wget https://media.githubusercontent.com/media/onnx/models/main/text/machine_comprehension/roberta/model/roberta-sequence-classification-9.onnx && \
    mv ./roberta-sequence-classification-9.onnx /tmp/

RUN wget https://nvidia.box.com/shared/static/iizg3ggrtdkqawkmebbfixo7sce6j365.whl -O onnxruntime_gpu-1.16.0-cp38-none-linux_aarch64.whl && \
    python3 -m pip install ./onnxruntime_gpu-1.16.0-cp38-none-linux_aarch64.whl

COPY ./reqs.txt /app/
RUN python3 -m pip install -r /app/reqs.txt

COPY ./main.py /app/

