# To-Dos

A list of possible improvements and changes to the base platform.
In no particular order.

## Improve Ansible interoperability

[This library](https://ansible.readthedocs.io/projects/runner/en/latest/) allows calling `ansible-playbook` via Python code.
Allow calling Ilúvatar Ansible scripts via this interface.
Allow this abstraction to be easily called from external Python code.
Make sure to use `kwargs` to avoid large numbers of function arguments.

## Create Jupyter notebooks for examples

Instead of shell scripts to run examples, do it with Jupyter notebooks.
Can add in graphing & analysis too.

## Monitor GPU utilization on Jetson platform

Jetson requires using `tegrastats` to get utilization numbers.
The [GPU monitor](iluvatar_worker_library/src/services/resources/gpu.rs) needs updated information for ideal usage.
Can cause dispatches to be blocked or broken on Jetson.

## Switch/Enable networking via unix sockets

Using HTTP connections to send/receive invocations to containers has some networking overhead and scaling issues at high throughput.
This can cause blocks of up to 60 seconds on some calls.
Both the worker code and the server running inside the container must be updated to this new format.

Moving to a lower-latency solution would fix both of these problems.
A few solutions exist, with the first probably being the best one.

1. Unix Sockets
2. Posix message queues
3. Linux pipes
4. Dbus messages

## Limit frequency of container checking

Container memory usage _should_ only change during/immediately after it runs an invocation.
There is no need to check a container if it hasn't been used.
Only review a container's memory usage after/during an invocation.

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

## Split running and cached memory usage

Container memory usage is reported as one unified number.
We should track and log containers based on them being in-use or cached.

## Monitor background threads for crashes

We start a number of background threads for different reasons.
If any of them crash or panic, it is likely that nothing will be logged, but only go to `stderr`.
It would be nice if we could monitor them for such crashes and log them, possibly even re-starting the thread.

## Reduce `.await` usages

Stacked calls to `await` futures just create more bookkeeping for tokio to manage.
This slowly increases overhead as concurrency rises.
If we can instead return `Future<T>`'s instead of a single `await` call, this will be improved.

## Graceful handling of container exceeding memory

Currently containers get a `limit` of memory usage from the `container_spec` we provide to containerd.
When they hit this limit during an invocation, the python process running that is killed, and the worker sees it as an abrput termination of the HTTP request.
The invocation is declared failed, and the container marked for removal.
We want to remove & replace too-large containers, but not have such failures.
Perhaps use `reservation` or `disableOOMKiller`?
We can handle removal ourselves after an invocation is done.

## Improved Agent Server

If a signal is received by gunicorn, SIGABRT for various errors or SIGKILL for OOM issues, the worker is given an empty response.
Actually returning something would be better, so the worker can handle it more gracefully.

## Improved data & output of load gen

Use something like [polars](https://github.com/pola-rs/polars) to store/compute data from load gen.
Better than hand-parsing / computing data structs and json everywhere.

## Controller optionally return internal data

Currently the controller just returns a function's output to the user.
Have it take an additional paramater to return enhanced data to the user, similar what the worker returns.
TransactionId, code execution time, latencies, etc.

## Generate Ansible variables

Can on-build, the ansible mappings of Iluvatar environment variables to config struct members be done?
Something like `iluvatar_worker` can reference `iluvatar_worker_library` in it's `build.rs`, giving it access to the types.
Can it then generate the ansible vars and put them somewhere?

## Cluster Simulation include worker node name

Logs for workers in a cluster simulation scenario are put into one file.
These become impossible to separate without any identifying factor.
Either split up the logs into separate files on worker name, or include the name in each log message so it can be split in post-processing.

Simple way: inject `basename` of logging config into each log statement and can post-filter on that.

Probably use a filter in `tracing-subscriber`, see [here](https://stackoverflow.com/questions/76939805/tracing-how-to-filter-logs-under-specified-levels-for-layer) and [here](https://docs.rs/tracing-subscriber/0.3.16/tracing_subscriber/layer/index.html#filtering-with-layers).

## CI Testing

Run automated tests in Github actions CI.

## Optional Memory capping

Currently a container has a limited amount of memory, and under memory pressure _inside_ the container, its processes can be killed by the OS.
Config allowing memory swapping to disk, or removing the isolation enforced memory cap would alleviate this.
The worker can still monitor container memory usage and remove containers if server memory pressure is high.
