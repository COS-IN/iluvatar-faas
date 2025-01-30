# Controller

The controller acts as the entry point to a cluster of connected workers.
All registration and invocation requests are assumed to be routed through the controller.

## Configuration

The controller will search along these paths for the configuration.
Later values should override those in earlier paths.

1. `./load_balancer/src/load_balancer.json` (based on the launch location of the root directory on launch)
1. `./load_balancer/src/load_balancer.dev.json` (based on the launch location of the root directory on launch)
1. A file path set using the `--config` flag to the executable.

Configuration via environment variables is also supported.
The must be prefixed with `ILUVATAR_CONTROLLER`, and traversing down objects are separated by a double underscore `__`.
So `config.load_balancer.algorithm` transforms to `ILUVATAR_CONTROLLER__load_balancer__algorithm`.

For example.

```bash
ILUVATAR_CONTROLLER__load_balancer__algorithm='RoundRobin' ./iluvatar_controller --config /my/config/path.json
```

See [this Rust file](../iluvatar_controller_library/src/server/controller_config.rs) for details on the configuration file and their descriptions.
