# TODOs:

## Graphite data

After an experiment, it would be great to have the data stored in graphite be extracted and stored alongside results.
We can match up timestamps and metrics to plot things together.

## Span Tracing 

Put `#[tracing::instrument(skip(self), fields(tid=%tid))]` on all the functions that handle Ilúvatar worker and controller important paths.
Make sure nothing big is being logged, to maintain performance.
Document the ability to disable them via config.

Pass the span to the invoker queue to continue it's chain through the worker thread.

## Time-skipping simulation

Can we enable time-skipping when running in a simulation context?
All function invocations are just `tokio::time::sleep().await`
The background threads use either that, or the `std::thread::sleep()` if using an OS thread.
The simulation can be sped up by "jumping" the current time to the next instance in which an action will be performed.

If we can put a layer between how the Future system polls results, and move the clock ahead to match, this could work.
But that's either 
1. A lot of work
1. Maybe not possible
