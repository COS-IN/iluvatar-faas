from setuptools import setup, find_packages

setup(
    name="ilubuild",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "click",
    ],
    entry_points={
        "console_scripts": [
            "ilubuild=iluvatar_build_cli.cli:main",
        ],
    },
    author="Saket Bhatt",
    description="CLI tool to build and publish Docker images for Iluvatar FaaS",
)
