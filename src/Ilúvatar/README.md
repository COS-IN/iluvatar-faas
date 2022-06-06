# Ilúvatar

FaaS system goes here


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