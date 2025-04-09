from abc import ABC, abstractmethod

class BaseRuntimeHandler(ABC):
    @abstractmethod
    def validate_function_directory(self, function_dir: str) -> None:
        """
        Validate that the function directory is compatible with the runtime.
        For Python, for example, this means the directory must contain a main.py that defines
        a main() function with no parameters and returns a dict.
        """
        pass

    @abstractmethod
    def ensure_dependencies(self, function_dir: str) -> None:
        """
        Ensure that the dependencies file exists in the function directory,
        or create a default one if needed.
        """
        pass

    @abstractmethod
    def build_image(self, function_dir: str, tag: str, gpu:bool) -> None:
        """
        Build a Docker image from the function directory.
        """
        pass

    @abstractmethod
    def push_docker_image(self, tag: str, docker_username: str, docker_password: str, docker_registry: str) -> None:
        """
        Push a Docker image to a registry.
        """
        pass
