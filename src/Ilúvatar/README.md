# Ilúvatar

FaaS system goes here

## Setup

1. Install golang
1. install cnitool
```
go get github.com/containernetworking/cni
go install github.com/containernetworking/cni/cnitool
```
1. update `cnitool` in [this file](./worker/src/worker.json) with the dir where cnitool is found.
1. Install plugins
```bash
git clone https://github.com/containernetworking/plugins.git
cd plugins
./build_linux.sh
```
1. Place somewhere and update `cni_plugin_bin` in [this file](./worker/src/worker.json)


## CLI Commands

### Ping

```bash
cargo run --bin iluvatar_worker_cli -- --worker local ping
```


### Invoke

```bash
cargo run --bin iluvatar_worker_cli -- --worker=local invoke --name=invoke
```

### Register

```bash
cargo run --bin iluvatar_worker_cli -- --worker=local register --name=helo
```

### Status

```bash
cargo run --bin iluvatar_worker_cli -- --worker=local status
```