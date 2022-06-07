#!/bin/bash

for dir in ./functions/*/
do
    dir=${dir%*/}      # remove the trailing "/"
    # echo ${dir##*/}    # print everything after the final "/"
    func_name=${dir##*/}

    docker push "alfuerst/$func_name-iluvatar-action:latest"

done

