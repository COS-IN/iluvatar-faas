#!/bin/bash

echo "Running server scaling comparison"
source ../examples-venv/bin/activate

dur=120
python3 scaling.py --duration $dur --points 8
echo "Experiment done, plotting."
python3 plot_scaling.py --duration $dur

deactivate