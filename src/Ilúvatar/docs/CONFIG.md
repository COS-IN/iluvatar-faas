# Configuration

## Worker configuration

See [this rust file](../iluvatar_lib/src/worker_api/config.rs) for details on the configuration file and its meaning.

Two additional path sources of config are currently supported.

1. `worker.dev.json` located in `worker/src` will override values in the default `worker/src/worker.json`
1. Passing a file path via `-c` or `--config` on launch
