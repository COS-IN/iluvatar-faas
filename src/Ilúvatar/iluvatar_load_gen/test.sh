#!/bin/bash

./target/debug/iluvatar_worker_cli --port 8079 --host 172.29.200.223 register --name t --version 1 --compute gpu --cpu 1 --memory 4000 --image docker.io/alfuerst/cupy-iluvatar-gpu:latest --isolation docker
# ../target/debug/iluvatar_worker_cli --port 8079 --host 172.29.200.223 register --name t --version 2 --compute gpu --cpu 1 --memory 4000 --image docker.io/alfuerst/cupy-iluvatar-gpu:latest --isolation docker
# ../target/debug/iluvatar_worker_cli --port 8079 --host 172.29.200.223 register --name t --version 3 --compute gpu --cpu 1 --memory 4000 --image docker.io/alfuerst/cupy-iluvatar-gpu:latest --isolation docker
# ../target/debug/iluvatar_worker_cli --port 8079 --host 172.29.200.223 register --name t --version 4 --compute gpu --cpu 1 --memory 4000 --image docker.io/alfuerst/cupy-iluvatar-gpu:latest --isolation docker
# ../target/debug/iluvatar_worker_cli --port 8079 --host 172.29.200.223 register --name t --version 5 --compute gpu --cpu 1 --memory 4000 --image docker.io/alfuerst/cupy-iluvatar-gpu:latest --isolation docker
# ../target/debug/iluvatar_worker_cli --port 8079 --host 172.29.200.223 register --name t --version 6 --compute gpu --cpu 1 --memory 4000 --image docker.io/alfuerst/cupy-iluvatar-gpu:latest --isolation docker

call() {
echo $1
./target/debug/iluvatar_worker_cli --port 8079 --host 172.29.200.223 invoke --name t --version $1
}

call 1 &
call 2 &
call 3 &
call 4 &
call 5 &
call 6 &

wait $(jobs -p)
