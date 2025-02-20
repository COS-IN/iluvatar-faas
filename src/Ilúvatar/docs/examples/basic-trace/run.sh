#!/bin/bash

echo "Running basic-trace"
source ../examples-venv/bin/activate

python3 run.py
# MPLBACKEND=agg jupyter execute basic-trace.ipynb

deactivate
