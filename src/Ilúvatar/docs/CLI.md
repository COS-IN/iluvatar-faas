# CLI

The CLI can talk to any worker, so long as it is in the config file.

## Configuration

The cli will search along these paths for the configuration.
Later values should override those in earlier paths

1. worker_cli/src/worker_cli.json (based on the launch location of the cli)
1. ~/.config/Ilúvatar/worker_cli.json
1. A file path set using the `--config` flag

More details are [here](docs/DOCS.md)

## Commands

### Ping

'nuff said.

```bash
ilúvatar_worker_cli --worker local ping
```

### Invoke

Invoke a function with specific args, no arguments is allowed.
The result of the function is returned

```bash
ilúvatar_worker_cli --worker local invoke --name invoke -a key=value -a key1=value1
```

### Asynchronous Invoke

Invoke a function asynchronously with specific args, no arguments is allowed.
A cookie identifying the results when they are ready is returned.

```bash
ilúvatar_worker_cli --worker local invoke-async --name invoke
```

### Asynchronous Invoke Check

Check if the invocation has completed by querying the cookie.
Results can only be returned once.

```bash
ilúvatar_worker_cli --worker local invoke-async-check --name invoke -c <cookie>
```


### Register

Register a function with the worker, currently only supports docker images.
Registration _must_ be done before a function can be invoked on the worker.

```bash
ilúvatar_worker_cli --worker local register --name hello --image "docker.io/alfuerst/hello-iluvatar-action:latest" --memory 128 --cpu 1
```

### Prewarm

Tell the worker to prewarm a specific function. 
This can also be used to register a function if the worker has never seen it before.

```bash
ilúvatar_worker_cli --worker local prewarm --name hello
```

### Status

Returns the load status of the worker

```bash
ilúvatar_worker_cli --worker local status
```

### Health

Get information if the worker is healthy or not

```bash
ilúvatar_worker_cli --worker local health
```
