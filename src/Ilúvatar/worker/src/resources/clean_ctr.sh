#!/bin/bash
tasks=$(ctr t ls -q)

echo "removing tasks"

for task in ${tasks[@]}; do
  ctr t kill $task --signal SIGKILL
  ctr t rm $task
done

echo "removing containers"

containers=$(ctr c ls -q)

for container in ${containers[@]}; do
  ctr c rm $container
done
# TODO: finish this, or put in rust?