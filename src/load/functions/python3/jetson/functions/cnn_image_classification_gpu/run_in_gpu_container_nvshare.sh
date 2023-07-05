#!/bin/bash 

dir_work='/workspace'


sudo docker run  \
            --rm \
            -it  \
            --runtime nvidia \
            --network host \
            -v /usr/local/lib/libnvshare.so:/libnvshare.so \
            -v /var/run/nvshare:/var/run/nvshare \
            -v $(realpath ./):"$dir_work" \
            cnn-gpu:latest \
            bash -c "ENV_NVSHARE_DEBUG=1 LD_PRELOAD=/libnvshare.so python3 $dir_work/main.py"

            # --name custom_func_run_gpu  \
            # nvcr.io/nvidia/l4t-tensorflow:r35.3.1-tf2.11-py3 \
            # "/bin/bash ls "
            # "cd $dir_work && python3 ./main.py"

            # -v "$dir_conf":/usr/local/apache2/temp \
            # -p 8892:80  \

