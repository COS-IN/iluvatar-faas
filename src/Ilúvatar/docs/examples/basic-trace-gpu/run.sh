#!/bin/bash

echo "Running basic-trace"
source ../examples-venv/bin/activate

python3 gen_invokes.py -o .
python3 run.py

deactivate
