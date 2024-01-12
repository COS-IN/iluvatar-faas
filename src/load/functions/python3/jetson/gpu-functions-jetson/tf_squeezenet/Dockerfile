ARG REPO=alfuerst
FROM ${REPO}/iluvatar-action-gpu-base:aarch64
WORKDIR /app

COPY reqs.txt .
RUN python3 -m pip install -r reqs.txt
COPY *.py ./

