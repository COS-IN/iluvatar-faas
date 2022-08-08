FROM python:3.10-slim-bullseye

# TODO: these images are really big...
# Do something to slim them down

RUN apt-get update -y && apt-get upgrade -y && apt-get install wget build-essential -y \ 
      && python3 -m pip install --upgrade pip && python3 -m pip install flask gunicorn

RUN wget http://www.devin.com/lookbusy/download/lookbusy-1.4.tar.gz && tar -xvf lookbusy-1.4.tar.gz \
  && rm lookbusy-1.4.tar.gz && cd ./lookbusy-1.4 && ./configure && make && make install && cd .. && \
  apt-get remove wget build-essential -y && apt-get clean && rm -rf ./lookbusy-1.4

WORKDIR /app
COPY *.py ./
COPY reqs.txt .

RUN python3 -m pip install -r reqs.txt

# ENTRYPOINT [ "python3", "./server.py" ]
ENTRYPOINT [ "gunicorn", "-w", "1", "server:app" ]