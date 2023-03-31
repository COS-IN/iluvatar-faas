# Controller

## Configuration

The cli will search along these paths for the configuration.
Later values should override those in earlier paths

1. load_balancer/src/load_balancer.json (based on the launch location of the root dir on launch)
1. A file path set using the `--config` flag to the exe
1. load_balancer/src/load_balancer.dev.json (based on the launch location of the root dir on launch)

For example.

```bash
./ilúvatar_controller --config /my/config/path.json
```

See [this rust file](../ilúvatar_controller_library/src/controller/controller_config.rs) for details on the configuration file and its meaning.

Two additional path sources of config are currently supported.

1. `load_balancer.dev.json` located in `load_balancer/src` will override values in the default `load_balancer/src/load_balancer.json`
1. Passing a file path via `-c` or `--config` on launch

Configuration via environment variables is also supported.
The must be prefixed with `ILUVATAR_CONTROLLER`, and traversing down objects are separated by a double underscore `__`.
So `config.load_balancer.algorithm` transforms to `ILUVATAR_CONTROLLER__load_balancer__algorithm`.
