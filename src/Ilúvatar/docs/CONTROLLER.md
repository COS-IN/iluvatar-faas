# Controller

## Configuration

The cli will search along these paths for the configuration.
Later values should override those in earlier paths

1. load_balancer/src/load_balancer.json (based on the launch location of the root dir on launch)
1. A file path set using the `--config` flag to the exe
1. load_balancer/src/load_balancer.dev.json (based on the launch location of the root dir on launch)

For example
```
ilúvatar_controller --config /my/config/path.json
```

More details are [here](docs/CONFIG.md)
