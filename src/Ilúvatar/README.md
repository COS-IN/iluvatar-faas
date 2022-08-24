# Ilúvatar

FaaS system goes here

## Setup

See [SETUP.md](docs/SETUP.md)

## CLI

### Configuration

The cli will search along these paths for the configuration.
Later values should override those in earlier paths

1. worker_cli/src/worker_cli.json (based on the launch location of the cli)
1. ~/.config/Ilúvatar/worker_cli.json
1. A file path set using the --config flag

More details are [here](docs/DOCS.md)

### Commands

#### Ping

```bash
cargo run --bin iluvatar_worker_cli -- --worker local ping
```


#### Invoke

```bash
cargo run --bin iluvatar_worker_cli -- --worker=local invoke --name=invoke
```

#### Register

```bash
cargo run --bin iluvatar_worker_cli -- --worker=local register --name=helo
```

#### Status

```bash
cargo run --bin iluvatar_worker_cli -- --worker=local status
```