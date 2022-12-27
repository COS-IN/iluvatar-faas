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

## Switch/Enable networking via unix sockets

Using HTTP connections to send/receive invocations has some networking overhead and scaling issues at high throughput.
Both the worker code and the server running inside the container must be updated to this new format.

Moving to a lower-latency solution would fix both of these problems.
A few solutions exist, with the first probably being the best one.
1. Unix Sockets
2. Posix message queues
3. Linux pipes
4. Dbus messages

## Limit frequency of container checking

Container memory usage _should_ only change during/immediately after it runs an invocaiton.
There is no need to check a container if it hasn't been used.
Only review a container's memory usage after/during an invocation.

## Multiple concurrent backends

A worker's host may have variable capabilities and hardware.
We should be able to run functions on multiple containerization backend setups if they can be run.
I.e. docker+GPU, containerd, etc.
The function registration should container the information on which backend it runs on.

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

## Disable-able worker registration

Currently worker registration with the controller always happens, and on failure an error is added to the log.
This whole process should be skipped based on config.
If registration is attempted and fails, the worker should exit.

## Split running and cached memory usage

Container memory usage is reported as one unified number.
We should track and log containers based on them being in-use or cached.

## Monitor background threads for crashes

We start a number of background threads for different reasons.
If any of them crash or panic, it is likely that nothing will be logged, but only go to `stderr`.
It would be nice if we could monitor them for such crashes and log them, possibly even re-starting the thread.
