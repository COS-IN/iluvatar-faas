# Intro

Details on the makeup of how stuff _inside_ the container is set up.

## Python3

### Run

How to run one (via docker).
There is an internal web server that listens on port `__IL_PORT` and host address `__IL_HOST` 

```bash
docker run -e __IL_PORT=8081 -p 8081:8081 alfuerst/cnn_image_classification-iluvatar-action
```

Default values for environment variables:
`__IL_PORT`: 8080
`__IL_HOST`: 0.0.0.0

### Invoke

It can then be accessed using any http client.
To simply ping the host, and check for simple code import errors just `GET` the base path.

```bash
curl http://localhost:8081/
```

To invoke the internal function, a `POST` to the `/invoke` path with some json content will suffice. The contents of the json will be deserialized and passed directly to the function code.

```bash
curl -X POST -H "Content-Type: application/json" -d "{}" http://localhost:8081/invoke
```


### Setup

Functions must implemented a main function at the top level of `main.py` and return a dictionary.

```
def main(args):
  return {}
```

Pip dependencies must be in a `reqs.txt` file located alongside the Python code.
And changes to the image can implemented a custom `Dockerfile` as well that must start with `FROM alfuerst/il-action-base` and NOT change the entrypoint.

### Build

Simply execute the `build.sh` script that's located in `./functions/python3/` while in that directory.
It will build all the containers, putting the web server, function code, and dependencies together.

## python3-alpine

Duplicates of the python3 folder, but builds smaller alpine images
**NOTE**: They use a slighly different entrypoint
