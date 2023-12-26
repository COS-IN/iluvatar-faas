#!/bin/bash 

dir_work='/workspace'

sudo docker run  \
            --rm \
            -it  \
            --runtime nvidia \
            --network host \
            --name custom_func_run_gpu  \
            -v $(realpath ./):"$dir_work" \
            cnn-gpu:latest \
            /usr/bin/python3 $dir_work/main.py 

            # nvcr.io/nvidia/l4t-tensorflow:r35.3.1-tf2.11-py3 \
            # "/bin/bash ls "
            # "cd $dir_work && python3 ./main.py"

            # -v "$dir_conf":/usr/local/apache2/temp \
            # -p 8892:80  \

