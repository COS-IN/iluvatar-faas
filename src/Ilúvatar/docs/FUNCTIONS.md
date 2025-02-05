# Function Basics

## Running code in a Function

Launch the worker using your custom configuration.

```bash
sudo iluvatar_worker -c iluvatar_worker/src/worker.dev.json
```

Register a simple function.
Currently, functions must use pre-built images.
The system does not support building images from function code at runtime.

```shell
foo@cosin:~$ iluvatar_worker_cli --host localhost --port 8079 register --name myfunc --version 1 --memory 512 --cpu 1 --image docker.io/alfuerst/hello-iluvatar-action:latest --isolation containerd --compute cpu
{"Ok": "function registered"}
```

Then invoke it, with or without arguments.

```shell
foo@cosin:~$ iluvatar_worker_cli --host localhost --port 8079 invoke --name myfunc --version 1 -a name=Alex
{"json_result":"{\"body\": {\"greeting\": \"Hello Alex from python!\", \"cold\": false, \"start\": 1680185561.5550308, \"end\": 1680185561.5550325, \"latency\": 1.6689300537109375e-06}}","success":true,"duration_us":2025,"compute":1,"container_state":3}
```

```shell
foo@cosin:~$ iluvatar_worker_cli --host localhost --port 8079 invoke --name myfunc --version 1
{"json_result":"{\"body\": {\"greeting\": \"Hello stranger from python!\", \"cold\": false, \"start\": 1680185580.6946194, \"end\": 1680185580.6946208, \"latency\": 1.430511474609375e-06}}","success":true,"duration_us":1916,"compute":1,"container_state":3}
```

Or invoke the function asynchronously.

```shell
foo@cosin:~$ iluvatar_worker_cli --host localhost --port 8079 invoke-async --name myfunc --version 1
125F17C0-C10A-7FB8-C12F-10C17450FB68
```

```shell
foo@cosin:~$ iluvatar_worker_cli --host localhost --port 8079 invoke-async-check --cookie 125F17C0-C10A-7FB8-C12F-10C17450FB68
{"json_result":"{\"body\": {\"greeting\": \"Hello stranger from python!\", \"cold\": false, \"start\": 1680185826.4311872, \"end\": 1680185826.431189, \"latency\": 1.9073486328125e-06}}","success":true,"duration_us":2138,"compute":1,"container_state":3}
```

## Preparing code to be Functions

Ilúvatar comes with pre-packaged functions of a variety of computation types.
The code for all of them resides [here](../../load/functions/).

All the current functions use Python3 as a language runtime and have our [custom server agent](../../load/functions/python3/server.py) wrap the function's Python code.
This server imports the function's code on startup and runs an HTTP server to wait for invocations.

### Lookbusy

We have a wrapper for [Lookbusy](http://www.devin.com/lookbusy/) that can use arguments to vary a function's CPU usage, execution duration, and memory usage.
[The code here](../../load/functions/lookbusy/main.py) calls the `lookbusy` executable located inside the image and kills it after the execution duration has completed.

### Python Code

[The rest of our functions]((../../load/functions/python3)) use Python3 code to perform different compute tasks.
Adding new functions is very simple.
The code must have a `main.py` file with a `main` function that takes as a single parameter a Python dictionary.
Create a new subfolder [here](../../load/functions/python3/functions/) that will be the name of your new function.
Put all Python code files in that folder, plus a `reqs.txt` file for dependency installation.
This file must conform to the expected [pip syntax](https://pip.pypa.io/en/stable/reference/requirements-file-format/).

### Building Containers

There are build scripts [such as this one](../../load/functions/python3/build-cpu.py) located in the relevant folders for creating Docker images from the various code snippets, plus installing their required dependencies.
Running these various scripts will compile all the functions into Docker images and push them to Docker Hub.
**NOTE**: You will need to pass a Docker repository name (the `--repo` flag) that you have write permission to via arguments to the script, along with an optional version for the image.

```bash
foo@cosin:~$ ./build-cpu.py --repo=cosin --version=latest
```

Once the script is completed, the images will be ready to use by Ilúvatar.

### Custom dependencies

If your code requires custom dependencies that can't be installed with `pip`, it can add a custom `dockerfile` that installs them.
It should be located in the same directory as the code, be named `Dockerfile`, and start with `FROM ${ACTION_BASE}`.
See `video_processing` doing this [here](../../load/functions/python3/functions/video_processing/Dockerfile) to install ffmpeg.
