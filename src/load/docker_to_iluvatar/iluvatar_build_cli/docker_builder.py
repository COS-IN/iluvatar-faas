import os
import json
import shutil
import subprocess
import tempfile

def build_docker_image_from_directory(function_dir: str, tag: str,
                                      base_image: str, install_command: str, server_command: str) -> None:
    """
    Build a Docker image using the contents of the function directory.

    The function directory is copied into a temporary build context,
    a Dockerfile is written that:
      - Uses the specified base image.
      - Copies all files from the function directory.
      - Runs the given install command (e.g., to install dependencies).
      - Sets the server command as the container's CMD.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        app_dir = os.path.join(tmpdir, "app")
        shutil.copytree(function_dir, app_dir, dirs_exist_ok=True)

        # cmd_json = json.dumps(server_command.split())
        dockerfile_content = f"""
FROM {base_image}
WORKDIR /app
COPY app/ .
RUN {install_command}
ENTRYPOINT ["gunicorn", "-w", "1", "server:app"]
        """.strip()

        dockerfile_path = os.path.join(tmpdir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile_content)

        command = ["docker", "build", "-t", tag, tmpdir]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Docker build failed: {result.stderr}")


def push_docker_image(tag: str, docker_username: str, docker_password: str, docker_registry: str) -> None:
    """
    Push a Docker image to a registry.

    Parameters:
        tag (str): The local Docker image tag.
        docker_username (str): Docker registry username.
        docker_password (str): Docker registry password.
        docker_registry (str): Docker registry URL (e.g., "docker.io").
    """
    # Log in to the Docker registry.
    login_command = [
        "docker", "login", docker_registry,
        "-u", docker_username,
        "-p", docker_password
    ]
    login_result = subprocess.run(login_command, capture_output=True, text=True)
    if login_result.returncode != 0:
        raise RuntimeError(f"Docker login failed: {login_result.stderr}")
    # If the tag does not include a slash (registry prefix), tag it with the registry.
    if "/" in tag:
        full_tag = tag
    else:
        full_tag = f"{docker_registry}/{tag}"
        tag_command = ["docker", "tag", tag, full_tag]
        tag_result = subprocess.run(tag_command, capture_output=True, text=True)
        if tag_result.returncode != 0:
            raise RuntimeError(f"Docker tag failed: {tag_result.stderr}")

    push_command = ["docker", "push", full_tag]
    push_result = subprocess.run(push_command, capture_output=True, text=True)
    if push_result.returncode != 0:
        raise RuntimeError(f"Docker push failed: {push_result.stderr}")
