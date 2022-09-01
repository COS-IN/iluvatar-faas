# Worker

The CLI can talk to any worker

## Configuration

The cli will search along these paths for the configuration.
Later values should override those in earlier paths

1. worker/src/worker.json (based on the launch location of the root dir on launch)
1. worker/src/worker.dev.json (based on the launch location of the root dir on launch)
1. A file path set using the `--config` flag to the exe

For example
```
ilúvatar_worker --config /my/config/path.json
```

More details are [here](docs/CONFIG.md)

## Commands

### Default

If nothing is passed on startup besides `--config`, the worker begins running an RPC server and does not terminate.
It **must** be run as `sudo` in order to communicate with containerd, the system networking stack, and other things.

### Clean

One can pass `clean` after any config to run the offline resource cleanup process.
The worker does not delete any resources created during it's execution on shutdown.
`clean` deletes network namespaces, containerd containers, and the network bridge that were created by previous executions of the worker.

It is recommended to do this before each new start to avoid a slow sapping of resources by repeated runs.

## Features

The rust library ([here](../ilúvatar_worker_library/)) most of the worker code is in has a feature to enable addition span information to be captured.
Enabling the feature `full_spans` with `iluvatar_worker_library = { path = "../ilúvatar_worker_library", features=["full_spans"] }` in the Cargo.toml will enable a large number of functions to record their spans.
This is a compile-time flag that will increase log file size and likely affect CPU and latenct of the worker.
