# TODOs:

## snapshot overlay bug

Inside `containerlife`, happens very rarely

```
{ "Error": "Unknown error Unknown error Create task failed with: status: Unknown, message: 
  "failed to create shim task: failed to mount rootfs component 
    &{
      overlay overlay 
      [index=off workdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1149/work 
      upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1149/fs 
      lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/78/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/77/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/76/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/7/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/6/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/5/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/4/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/3/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/2/fs:/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs
      ]
      }: no such file or directory: unknown",
       details: [], 
       metadata: MetadataMap { headers: {"content-type": "application/grpc"} }" }
```

## Span Tracing 

Put `#[tracing::instrument(skip(self))]` on all the functions that handle Ilúvatar worker and controller important paths.
Make sure nothing big is being logged, to maintain performance.
Document the ability to disable them via config.

Pass the span to the invoker queue to continue it's chain through the worker thread.

## Log Tracing

Move all our log statements to use the structured log approach.
This way the json logging will be very easy to handle offline.
And we no longer have to format log messages with variables and `TransactionId`'s
For details, see (dev docs)[./docs/DEV.md]

## Time-skipping simulation

Can we enable time-skipping when running in a simulation context?
All function invocations are just `tokio::time::sleep().await`
The background threads use either that, or the `std::thread::sleep()` if using an OS thread.
The simulation can be sped up by "jumping" the current time to the next instance in which an action will be performed.

If we can put a layer between how the Future system polls results, and move the clock ahead to match, this could work.
But that's either 
1. A lot of work
1. Maybe not possible
