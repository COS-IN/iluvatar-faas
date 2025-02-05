#!/bin/bash

echo "Running benchmark"
source ../examples-venv/bin/activate

python3 bench.py

deactivate