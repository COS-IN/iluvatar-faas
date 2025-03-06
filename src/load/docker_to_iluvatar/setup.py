from setuptools import setup, find_packages

setup(
    name="py2lambda",
    version="1.0.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "iluvatar_build_cli": ["templates/*.template"],
    },
    install_requires=[
        "click",
    ],
    entry_points={
        "console_scripts": [
            "py2lambda=iluvatar_build_cli.cli:main",
        ],
    },
    author="Saket Bhatt",
    description="CLI tool to build and publish Docker images for Iluvatar FaaS",
)
