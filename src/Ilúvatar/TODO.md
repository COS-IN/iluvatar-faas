# Current intermittent bugs:

## snapshot overlay

Inside `containerlife`

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
