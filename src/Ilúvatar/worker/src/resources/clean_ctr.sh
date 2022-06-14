tasks = $((ctr t ls -q))

for task in ${tasks[@]}; do
  echo $task
done

containers = $((ctr c ls -q))

for container in ${containers[@]}; do
  echo $container
done
# TODO: finish this, or put in rust?