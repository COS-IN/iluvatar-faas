# Creating and Invoking a basic Function

Launch the worker using your custom configuration.

```bash
sudo ilúvatar_worker -c ilúvatar_worker/src/worker.dev.json
```

Register a simple function.

```shell
foo@bar:~$ ilúvatar_worker_cli --address localhost --port 8079 register --name myfunc --version 1 --memory 512 --cpu 1 --image docker.io/alfuerst/hello-iluvatar-action:latest --isolation containerd --compute cpu
{"Ok": "function registered"}
```

```console
ilúvatar_worker_cli --address localhost --port 8079 invoke --name myfunc --version 1 -a name=Alex
InvokeResponse { json_result: "{\"body\": {\"greeting\": \"Hello Alex from python!\", \"cold\": true, \"start\": 1680181436.3092613, \"end\": 1680181436.309262, \"latency\": 7.152557373046875e-07}}", success: true, duration_us: 87858, compute: 1, container_state: Cold }
```

```console
ilúvatar_worker_cli --address localhost --port 8079 invoke --name myfunc --version 1
InvokeResponse { json_result: "{\"body\": {\"greeting\": \"Hello stranger from python!\", \"cold\": false, \"start\": 1680181475.0026104, \"end\": 1680181475.0026116, \"latency\": 1.1920928955078125e-06}}", success: true, duration_us: 1941, compute: 1, container_state: Warm }
```

Or invoke the function asynchronously.

```bash
ilúvatar_worker_cli --address localhost --port 8079 invoke-async --name myfunc --version 1
125F17C0-C10A-7FB8-C12F-10C17450FB68
```

```bash
ilúvatar_worker_cli --address localhost --port 8079 invoke-async-check --cookie 125F17C0-C10A-7FB8-C12F-10C17450FB68
{"body": {"greeting": "Hello stranger from python!", "cold": true, "start": 1680184945.9233406, "end": 1680184945.9233415, "latency": 9.5367431640625e-07}}
```
