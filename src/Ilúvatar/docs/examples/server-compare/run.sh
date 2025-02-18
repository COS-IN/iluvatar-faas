#!/bin/bash

echo "Running server scaling comparison"
source ../examples-venv/bin/activate

dur=30
python3 scaling.py --duration $dur
echo "Experiment done, plotting."
#python3 plot_scaling.py --duration $dur

deactivate