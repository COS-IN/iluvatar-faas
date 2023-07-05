#!/bin/bash 

cleanup () {
    $cmd_ssh $cmd_kill
}
trap cleanup SIGINT

scp ./target/debug/ilúvatar_worker abrehman@v-021.victor.futuresystems.org:~/tmp
scp ./target/debug/worker.json abrehman@v-021.victor.futuresystems.org:~/tmp

cmd_ssh="ssh abrehman@v-021.victor.futuresystems.org"
cmd_ssh="ssh ar@$jetson_ip"
cmd_kill="ps -ef | grep -v grep | grep -w ilúvatar_worker | awk ""'"'{print $2}'"'"" | xargs sudo kill -9"
dir_t=~/tmp

$cmd_ssh sudo $dir_t/ilúvatar_worker -c $dir_t/worker.json clean 
$cmd_ssh sudo $dir_t/ilúvatar_worker -c $dir_t/worker.json 


