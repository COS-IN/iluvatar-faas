#!/bin/bash

source ./config.sh

$dir_target/ilúvatar_worker_cli   \
                    --address $target_ip   \
                    --port $port invoke   \
                    --name $func_name   \
                    --version 1 -a name=Test

