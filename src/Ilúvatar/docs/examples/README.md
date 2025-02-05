# Examples

This folder has a variety of examples for how to use Ilúvatar and it's associated load generation tools.
Each sub-folder has a single example with more details, and a `run.sh` script that executes the example.
The first run will be slow as it builds the project in Rust's optimized `release` mode.

1. `scaling/` runs the worker with an increasing workload to test system scalability.
2. `benchmark/` takes a baseline runtime of all the packaged functions to capture their performance.
3. `basic-trace/` generates a simple trace with known characteristics to run against a worker.
4. `cluster-trace/` runs a trace against a cluster setup, with controller and worker.
5. `azure-trace/` generates a trace from the Azure function dataset and runs it against a worker.
6. `simuation-trace/` runs a cluster in simulation mode, using the same code, load, and cluster setup as the `cluster-trace` example above.
7. `detailed-spans/` shows off the capturable detail from inside the worker using span information generated at runtime.

## Preparation

Run the setup script in the parent folder to install dependencies and prepare the local machine to run any of these examples.

```shell
./setup.sh --worker --load
```

```shell
./experiment_setup.sh --worker --load
```

## Note

By default, containerd uses `overlayfs` as a [snapshot and file system manager](https://github.com/containerd/containerd/tree/main/docs/snapshotters) for creating new containers.
We have found that it lazily-creates some directories which can cause inconsistent race-condition errors when trying to access them too quickly.
Therefore, invocations in these examples *may* fail for this reason, and you will then see a Python `ImportError` in the logs.
To prevent this, we recommend using `zfs` as a backend for container as a more stable solution.
We have details on how to set this up [here](../SETUP.md#containerd).
