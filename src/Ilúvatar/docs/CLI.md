# CLI

The CLI can issue single commands to any running worker.
It is not designed to apply substantial load to the system, that should be done via the [load generator](./LOAD.md).

## Commands

### Ping

'nuff said.

```bash
iluvatar_worker_cli --host localhost -port 8000 ping
```

### Invoke

Invoke a function with specific args, no arguments is allowed.
The result of the function is returned

```bash
iluvatar_worker_cli --host localhost -port 8000 invoke --name invoke -a key=value -a key1=value1
```

### Asynchronous Invoke

Invoke a function asynchronously with specific args, no arguments is allowed.
A cookie identifying the results when they are ready is returned.

```bash
iluvatar_worker_cli --host localhost -port 8000 invoke-async --name invoke
```

### Asynchronous Invoke Check

Check if the invocation has completed by querying the cookie.
Results can only be returned once.

```bash
iluvatar_worker_cli --address localhost -port 8000 invoke-async-check --name invoke -c <cookie>
```

### Register

Register a function with the worker, currently only supports docker images.
Registration _must_ be done before a function can be invoked on the worker.

```bash
iluvatar_worker_cli --address localhost -port 8000 register --name hello --image "docker.io/alfuerst/hello-iluvatar-action:latest" --memory 128 --cpu 1
```

### Prewarm

Tell the worker to prewarm a specific function.
This can also be used to register a function if the worker has never seen it before.

```bash
iluvatar_worker_cli --address localhost -port 8000 prewarm --name hello
```

### Status

Returns the load status of the worker

```bash
iluvatar_worker_cli --address localhost -port 8000 status
```

### Health

Get information if the worker is healthy or not

```bash
iluvatar_worker_cli --address localhost -port 8000 health
```
