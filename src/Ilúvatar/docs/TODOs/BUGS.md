# Bugs

Known bugs in system that should be fixed.

## Bridge is full

A network bridge in Linux can only have 1024 `veth` devices attached to it.
We must either

1. Remove unused containers from the beidge to make room for new ones (eviction)
2. Increase the number of containers we can support, by running multiple bridges/

## Concurrent container creation in Containerd

When a significant number of concurrent requests are handled by the worker, Containerd can experience significant contention and essentially freeze the program.
The exact call happens inside `ContainerdLifecycle::load_mounts`.

1. Figure out what in containerd is causing this.
1. Solve that problem

## High CPU usage after large number of containers made

After an experiment (~ 1 hour running) the CPU usage of the worker is higher than at the start.
Is this the container manager worker thread?
What can be done?
