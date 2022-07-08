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

## Missing packages?

Happens in started up container. Standard library pacakges just are missing.
Other times the identical container will load successfully.

```
{ "Error": "Unknown error Unknown error Startup error: Failed to wait for container startup because Timeout while reading inotify events for container hello-0.0.5-19293B05-F0B7-8235-B4FC-C24D53331CC6; stdout: ''; stderr 'Traceback (most recent call last):
  File "/app/server.py", line 2, in <module>
    from flask import Flask, request, jsonify
  File "/usr/local/lib/python3.10/site-packages/flask/__init__.py", line 7, in <module>
    from .app import Flask as Flask
  File "/usr/local/lib/python3.10/site-packages/flask/app.py", line 27, in <module>
    from . import cli
  File "/usr/local/lib/python3.10/site-packages/flask/cli.py", line 32, in <module>
    from importlib import metadata
  File "/usr/local/lib/python3.10/importlib/metadata/__init__.py", line 4, in <module>
    import csv
ModuleNotFoundError: No module named 'csv'
'" }
```

```
{ "Error": "Unknown error Unknown error Startup error: Failed to wait for container startup because Timeout while reading inotify events for container hello-0.0.3-A92EFBC1-A433-4E8D-AC6E-2EB510EE47DC; stdout: ''; stderr 'Traceback (most recent call last):
  File "/app/server.py", line 2, in <module>
    from flask import Flask, request, jsonify
  File "/usr/local/lib/python3.10/site-packages/flask/__init__.py", line 3, in <module>
    from werkzeug.exceptions import abort as abort
  File "/usr/local/lib/python3.10/site-packages/werkzeug/__init__.py", line 1, in <module>
    from .serving import run_simple as run_simple
  File "/usr/local/lib/python3.10/site-packages/werkzeug/serving.py", line 24, in <module>
    from http.server import BaseHTTPRequestHandler
  File "/usr/local/lib/python3.10/http/server.py", line 92, in <module>
    import email.utils
  File "/usr/local/lib/python3.10/email/utils.py", line 33, in <module>
    from email._parseaddr import quote
  File "/usr/local/lib/python3.10/email/_parseaddr.py", line 16, in <module>
    import time, calendar
ModuleNotFoundError: No module named 'calendar'
'" }
```

## Processes dying?

Perfectly healthy containers are suddenly no longer working.
Only identification is the proc entry being missing.

```
[2022-07-08 15:59:16.127772 -04:00] [DEBUG] [iluvatar_lib::services::containers::containermanager] [CtrMrgWorker] container 'chameleon-0.0.2-550AA71B-E197-774A-362C-4D6ED52C01E9' new: 31; old: 31; diff:0
[2022-07-08 15:59:21.128956 -04:00] [WARN] [iluvatar_lib::services::containers::structs] [No such file or directory (os error 2)] Error trying to read container 'chameleon-0.0.2-550AA71B-E197-774A-362C-4D6ED52C01E9' /proc/<pid>/statm: CtrMrgWorker
[2022-07-08 15:59:21.129006 -04:00] [DEBUG] [iluvatar_lib::services::containers::containermanager] [CtrMrgWorker] container 'chameleon-0.0.2-550AA71B-E197-774A-362C-4D6ED52C01E9' new: 128; old: 31; diff:97
[2022-07-08 15:59:26.350464 -04:00] [WARN] [iluvatar_lib::services::containers::containermanager] [CtrMrgWorker] Removing an unhealthy container chameleon-0.0.2-550AA71B-E197-774A-362C-4D6ED52C01E9 stdout: ' * Serving Flask app 'server' (lazy loading)\n * Environment: production\n   WARNING: This is a development server. Do not use it in a production deployment.\n   Use a production WSGI server instead.\n * Debug mode: off\n' stderr: ' * Running on http://10.10.8.93:8080 (Press CTRL+C to quit)\n10.10.0.1 - - [08/Jul/2022 19:58:54] "POST /invoke HTTP/1.1" 200 -\n10.10.0.1 - - [08/Jul/2022 19:58:54] "POST /invoke HTTP/1.1" 200 -\n10.10.0.1 - - [08/Jul/2022 19:58:54] "POST /invoke HTTP/1.1" 200 -\n'
[2022-07-08 15:59:26.350543 -04:00] [INFO] [iluvatar_lib::services::containers::containermanager] [CtrMrgWorker] Removing container chameleon-0.0.2-550AA71B-E197-774A-362C-4D6ED52C01E9
[2022-07-08 15:59:26.350599 -04:00] [INFO] [iluvatar_lib::services::containers::containerdlife] [CtrMrgWorker] Removing container 'chameleon-0.0.2-550AA71B-E197-774A-362C-4D6ED52C01E9'
[2022-07-08 15:59:26.450213 -04:00] [INFO] [iluvatar_lib::services::containers::containerdlife] [CtrMrgWorker] Container: "chameleon-0.0.2-550AA71B-E197-774A-362C-4D6ED52C01E9" deleted
```
