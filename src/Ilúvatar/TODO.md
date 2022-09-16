# TODOs:

## Graphite data

After an experiment, it would be great to have the data stored in graphite be extracted and stored alongside results.
We can match up timestamps and metrics to plot things together.

## Time-skipping simulation

Can we enable time-skipping when running in a simulation context?
All function invocations are just `tokio::time::sleep().await`
The background threads use either that, or the `std::thread::sleep()` if using an OS thread.
The simulation can be sped up by "jumping" the current time to the next instance in which an action will be performed.

If we can put a layer between how the Future system polls results, and move the clock ahead to match, this could work.
But that's either 
1. A lot of work
1. Maybe not possible

## Slow clean-up with large number of containers

When a large number of containers needs to be removed (> 100), the serial `clean` command can be very slow.
Spawn a thread for each container to delete, ensuring that all errors are still captured and propagated to the master thread.

## Bridge is full

A network bridge in linux can only have 1024 veth devices attached to it.
We must either
1. Remove unused containers from the brigde to make room for new ones (eviction)
2. Increase the number of containers we can support, by running multiple bridges/
