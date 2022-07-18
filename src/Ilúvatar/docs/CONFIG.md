# Configuration

## Worker configuration

See [this rust file](../ilúvatar_lib/src/worker_api/worker_config.rs) for details on the configuration file and its meaning.

Two additional path sources of config are currently supported.

1. `worker.dev.json` located in `worker/src` will override values in the default `worker/src/worker.json`
1. Passing a file path via `-c` or `--config` on launch

Configuration via environment variables is also supported.
The must be prefixed with `ILUVATAR_WORKER`, and traversing down objects are separated by a double underscore `__`.
So `config.networking.use_pool` transforms to `ILUVATAR_WORKER__networking__use_pool`

## Controller configuration

See [this rust file](../ilúvatar_lib/src/load_balancer_api/lb_config.rs) for details on the configuration file and its meaning.

Two additional path sources of config are currently supported.

1. `load_balancer.dev.json` located in `load_balancer/src` will override values in the default `load_balancer/src/load_balancer.json`
1. Passing a file path via `-c` or `--config` on launch

Configuration via environment variables is also supported.
The must be prefixed with `ILUVATAR_CONTROLLER`, and traversing down objects are separated by a double underscore `__`.
So `config.load_balancer.algorithm` transforms to `ILUVATAR_CONTROLLER__load_balancer__algorithm`.