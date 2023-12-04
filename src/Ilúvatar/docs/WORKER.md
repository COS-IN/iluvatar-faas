# Worker

This document provides high-level design of the Ilúvatar worker.

## Configuration

On boot, the worker will pull configuration from two sources, a file and environment variables.

Multiple file paths will be searched, in this order.

1. `./worker/src/worker.json` (based on the launch location of the root directory on launch)
1. `./worker/src/worker.dev.json` (based on the launch location of the root directory on launch)
1. A file path set using the `--config` flag to the executable
Later values will override those in earlier paths if multiple are present.

Environment variables [ansible setup](../ansible/worker.yml).

See [this Rust file](../iluvatar_worker_library/src/worker_api/worker_config.rs) for details on the configuration file and its meaning.

Configuration via environment variables is also supported.
The must be prefixed with `ILUVATAR_WORKER`, and traversing down objects are separated by a double underscore `__`.
So `config.networking.use_pool` transforms to `ILUVATAR_WORKER__networking__use_pool`

A full example of launching the worker would look like this.

```bash
ILUVATAR_WORKER__networking__use_pool=false ./iluvatar_worker --config /my/config/path.json
```

## Commands

### Default

If nothing is passed on startup besides `--config`, the worker begins running an RPC server and does not terminate.
It **must** be run as `sudo` in order to communicate with `containerd`, the system networking stack, and other things.

### Clean

One can pass `clean` after any config to run the offline resource cleanup process.
The worker does not delete any resources created during its execution on shutdown.
`clean` deletes network namespaces, `containerd` containers, and the network bridge that were created by previous executions of the worker.

It is recommended to do this before each new start to avoid a slow sapping of resources by repeated runs.

## Detailed function Span information

This is a compile-time flag that will increase log file size and CPU usage of the worker.
It can also be compiled from the Makefile via `make spans` (release build) or `make spansd` (debug build).

## Detailed Documentation

The worker code is put [into a library](../iluvatar_worker_library/), [and a binary](../iluvatar_worker/).
This keeps the actual code only compiled for the binary to a minimum, maximizing code reuse

The documentation on the Rust code provides details about the purpose and utilization of individual structs, types, enums, etc.
It can be opened with this command in the `src/Ilúvatar` directory.

```bash
cargo doc -p iluvatar_worker_library --open
```

```bash
cargo doc -p iluvatar_worker --open
```
