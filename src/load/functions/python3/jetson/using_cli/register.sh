#!/bin/bash

source ./config.sh

$dir_target/ilúvatar_worker_cli \
                    --address $target_ip   \
                    --port $port register   \
                    --name $func_name   \
                    --version 1   \
                    --memory $((1024*2+512))   \
                    --cpu 1   \
                    --image $func_img   \
                    --isolation docker   \
                    --compute $compute 


