import click
import os
from iluvatar_build_cli.runtime_handler.python.handler import PythonRuntimeHandler

@click.command()
@click.option("--function-dir", required=True, help="Path to the function directory containing your code and dependencies.")
@click.option("--runtime", default="python", help="Runtime to use (default: python).")
@click.option("--tag", default="iluvatar-runtime:latest", help="Docker image tag (default: iluvatar-runtime:latest).")
@click.option("--docker-user", default=None, help="Docker registry username.")
@click.option("--docker-pass", default=None, help="Docker registry password.")
@click.option("--docker-registry", default="docker.io", help="Docker registry URL (default: docker.io).")
@click.option("--gpu", is_flag=True, help="Use GPU base image.")
def main(function_dir, runtime, tag, docker_user, docker_pass, docker_registry, gpu):
    """
    CLI tool to build a Docker runtime for FaaS.

    This tool validates that the entry point file is Iluvatar-compatible (i.e. it defines a main() function
    that returns a dict), ensures that the dependencies file exists (or creates an empty one if missing),
    builds a Docker image based on the specified runtime, and optionally pushes it to a Docker registry.
    """
    function_dir = os.path.abspath(function_dir)
    click.echo(f"Using function directory: {function_dir}")
    click.echo(f"Runtime: {runtime}")
    click.echo(f"Docker image tag: {tag}")

    if runtime.lower() == "python":
        handler = PythonRuntimeHandler()
    # Future handlers for Node.js, Go, etc. can be added here.
    else:
        click.echo(f"Unsupported runtime: {runtime}")
        return

    try:
        click.echo("Validating function directory...")
        handler.validate_function_directory(function_dir)
    except Exception as e:
        click.echo(f"Validation failed: {e}")
        return

    try:
        click.echo("Ensuring dependencies file exists...")
        handler.ensure_dependencies(function_dir)
    except Exception as e:
        click.echo(f"Error handling dependencies: {e}")
        return

    try:
        click.echo("Building Docker image...")
        handler.build_image(function_dir, tag, gpu)
    except Exception as e:
        click.echo(f"Error building Docker image: {e}")
        return

    click.echo("Docker image built successfully.")

    # If Docker registry credentials are provided, push the image.
    if docker_user and docker_pass:
        click.echo("Pushing Docker image to registry...")
        try:
            handler.push_docker_image(tag, docker_user, docker_pass, docker_registry)
        except Exception as e:
            click.echo(f"Error pushing Docker image: {e}")
            return
        click.echo("Docker image pushed successfully.")
    else:
        click.echo("No Docker registry credentials provided. Skipping push.")

if __name__ == "__main__":
    main()
