#!/bin/bash

echo "Running azure-trace"
source ../examples-venv/bin/activate

# Prepare azure trace
./generate-trace.sh

python3 run.py

deactivate
