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

## Bridge is full

A network bridge in linux can only have 1024 veth devices attached to it.
We must either
1. Remove unused containers from the brigde to make room for new ones (eviction)
2. Increase the number of containers we can support, by running multiple bridges/

## Switch/Enable networking via unix sockets

Using HTTP connections to send/receive invocations has some networking overhead and scaling issues at high throughput.
Both the worker code and the server running inside the container must be updated to this new format.

Moving to a lower-latency solution would fix both of these problems.
A few solutions exist, with the first probably being the best one.
1. Unix Sockets
2. Posix message queues
3. Linux pipes
4. Dbus messages

## Concurrent container creation in Containerd

When a significant number of concurrent requests are handled by the worker, Containerd can experience significant contention and essentially freeze the program.
The exact call happens inside `ContainerdLifecycle::load_mounts`.

1. Figure out what in containerd is causing this.
1. Solve that problem

## High CPU usage after large number of containers made

After an experiment (~ 1 hour running) the CPU usage of the worker is higher than at the start.
Is this the container manager worker thread?
What can be done?

## Multiple concurrent backends

A worker's host may have variable capabilities and hardware.
We should be able to run functions on multiple containerization backend setups if they can be run.
I.e. docker+GPU, containerd, etc.
The function registration should container the information on which backend it runs on.

## Enforce CPU limits

Functions can request a specific number of CPU cores to have access to when executing.
Currently we just use processor shares on cgroups.
These allow a function to use several cores if nothing else is running on them.
Bad for a number of reasons.

## Retry on prewarm

If a prewarm request comes in, sometimes container startup can fail due to a transient issue inside containerd.
Retry it once or twice (configurable?) to gain stability.

## Reload/clean state on reboot

Currently the worker does not save, load, or recover any state anywhere.
Leftover state can lead to accumulating resource usage, and boot errors from the networking manager.
Clearing this state on startup would enable clean-slate as an assumption.

Or re-loading state would be a general nice feature to have.
Which containers, etc., belong to the worker, the bridge and network veths too.

## Run as Linux daemon

Currently startup on a remote machine via ansible runs the worker and controller as a background ansible job.
This is a hack and not the ideal way to deploy this as software.
Putting this as a linux daemon with the start/stop/restart paradigm would be better.
