import os
from iluvatar_build_cli.runtime_handler.base import BaseRuntimeHandler
from iluvatar_build_cli.runtime_handler.python import validator
from iluvatar_build_cli import docker_builder
from .config import BASE_IMAGE, SERVER_COMMAND, BASE_IMAGE_GPU

class PythonRuntimeHandler(BaseRuntimeHandler):
    def validate_function_directory(self, function_dir: str) -> None:
        validator.validate_function_directory(function_dir)

    def ensure_dependencies(self, function_dir: str) -> None:
        """
        Ensure that the function directory contains a requirements.txt file.
        If not, create an empty one.
        """
        req_path = os.path.join(function_dir, "requirements.txt")
        if not os.path.exists(req_path):
            with open(req_path, "w") as f:
                f.write("")

    def build_image(self, function_dir: str, tag: str, gpu: bool) -> None:
        install_command = "pip install --no-cache-dir -r requirements.txt"
        docker_builder.build_docker_image_from_directory(
            function_dir,
            tag,
            base_image=BASE_IMAGE if not gpu else BASE_IMAGE_GPU,
            install_command=install_command,
            server_command=SERVER_COMMAND
        )
    def push_docker_image(self, tag: str, docker_username: str, docker_password: str, docker_registry: str) -> None:
        """
        Push the Docker image to the registry.
        """
        docker_builder.push_docker_image(tag, docker_username, docker_password, docker_registry)
