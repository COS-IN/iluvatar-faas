FROM python:3.10-slim-bullseye

# TODO: these images are really big...
# Do something to slim them down
ARG SERVER_PY=server.py

WORKDIR /app
COPY reqs.txt ${SERVER_PY} server.py gunicorn.conf.py ./
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --compile -r reqs.txt && \
    python3 -m pip cache purge && \
    apt-get clean && \
    apt-get autoremove

ENTRYPOINT [ "gunicorn", "-w", "1", "server:app" ]