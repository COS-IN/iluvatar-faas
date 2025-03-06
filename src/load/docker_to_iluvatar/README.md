# py2lambda - CLI for Building and Publishing Iluvatar FaaS Images  

**py2lambda** is a command-line tool designed to streamline the process of creating and publishing Docker images for functions running on the **Iluvatar** Function-as-a-Service (FaaS) platform. It ensures that your function code meets Iluvatar's requirements, sets up dependencies, builds a Docker image, and optionally pushes it to a registry.  

---

## Installation  

Ensure you have **Docker** installed and running on your system. Then, install `py2lambda` using `pip` (if distributed as a Python package) or download the binary from the releases page.  

```sh
pip install py2lambda
```

## Usage

```sh
Usage: py2lambda [OPTIONS]

  CLI tool to build a Docker runtime for FaaS.

Options:
  --function-dir TEXT     Path to the function directory containing your code
                          and dependencies.  [required]
  --runtime TEXT          Runtime to use (default: python).
  --tag TEXT              Docker image tag (default: iluvatar-runtime:latest).
  --docker-user TEXT      Docker registry username.
  --docker-pass TEXT      Docker registry password.
  --docker-registry TEXT  Docker registry URL (default: docker.io).
  --gpu                   Use GPU base image.
  --help                  Show this message and exit.

```
## Examples

Assuming your function is in a directory called my_function/, you can build a Docker image with:
```sh
py2lambda --function-dir my_function --tag <tag_name>:version
```

For python specific functions
This will:
    Validate that main.py exists and defines a main() function.
    Ensure a requirements.txt (or equivalent) file exists, creating an empty one if missing.

### Build and Push to a Private Docker Registry
```sh
    py2lambda --function-dir my_function \
  --docker-user myusername \
  --docker-pass mypassword \
  --docker-registry docker.io \
  --tag myusername/my-iluvatar-func:v1.0
```

## Troubleshooting 

### Python
Ensure your main.py exists and has main function defined which returns a dictionary

### Docker not found
Make sure Docker is installed and running:
```sh
docker --version
```

### Authentication failed when pushing image
Verify your Docker credentials and registry URL. Try logging in manually:
```sh
docker login -u myusername -p mypassword docker.io
```










