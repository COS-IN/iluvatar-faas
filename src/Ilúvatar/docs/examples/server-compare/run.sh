#!/bin/bash

echo "Running server scaling comparison"
source ../examples-venv/bin/activate

python3 scaling.py --duration 120
echo "Experiment done, plotting."
python3 plot_scaling.py --duration 120

deactivate