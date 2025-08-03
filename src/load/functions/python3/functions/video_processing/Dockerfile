ARG ACTION_BASE=alfuerst/iluvatar-action-base
FROM ${ACTION_BASE}

WORKDIR /app
COPY reqs.txt *.py ./
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y ffmpeg && \
    apt-get clean && apt-get autoremove && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --compile -r reqs.txt && \
    python3 -m pip cache purge

#COPY *.py .
# RUN apt-get install -y jpeg-dev zlib-dev freetype-dev lcms2-dev openjpeg-dev tiff-dev tk-dev tcl-dev libgcc openssl-dev
# RUN apt-get install -y coreutils libxml2-dev libxslt-dev libffi-dev gcc musl-dev curl ffmpeg libsm6 libxext6 
