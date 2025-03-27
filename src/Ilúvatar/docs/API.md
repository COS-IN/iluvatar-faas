# HTTP API Interface

We have an HTTP API wrapper for our RPC calls using [Axum](https://github.com/tokio-rs/axum).

## Endpoints

- **GET /ping**  
  Returns a simple RPC ping.
  Example:

  ```bash
  curl "http://<host>:port/ping"
  ```

- **PUT /register**  
  Expects a JSON payload with:
  - `function_name`
  - `version`
  - `image`
  - `memory`
  - `cpu`
  - `isolate` ("docker" or "containerd")
  - `compute` ("CPU" or "GPU")

  Example:

  ```bash
  curl -X PUT http://host:port/register \
  -H "Content-Type: application/json" \
  -d '{
        "function_name": "hello",
        "version": "1",
        "image": "docker.io/alfuerst/hello-iluvatar-action",
        "memory": 512,
        "cpu": 1,
        "isolate": "docker",
        "compute": "CPU"
      }'
    ```

- **GET /invoke/:func_name/:version**  
  Pass key/value pairs as query parameters; they are converted to JSON and passed to RPC invoke.

 Example:
  ```bash
  curl "http://host:port/invoke/register/1?name=Saket"
  ```

- **GET /async_invoke/:func_name/:version**  
  Similar to `/invoke`, but returns a cookie string from RPC invoke_async.

  Example:
  ```bash
  curl "http://host:port/async_invoke/register/1?name=Saket"
  ```


- **GET /invoke_async_check/:cookie**  
  Returns the JSON response of the async invoke check.
  Example:
  ```bash
  curl "http://host:port/invoke_async_check/<COOKIE_STRING>"
    ```

- **GET /list_registered_func**  

Example:
  Lists registered functions in JSON format.
  ```bash
  curl "http://host:port/list_registered_func"
```
