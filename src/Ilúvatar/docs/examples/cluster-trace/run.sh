#!/bin/bash

echo "Running cluster-trace"
source ../examples-venv/bin/activate

./generate-trace.sh

python3 run.py

deactivate
