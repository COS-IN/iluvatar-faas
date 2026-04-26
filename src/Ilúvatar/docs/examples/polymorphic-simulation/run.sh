#!/bin/bash

source ../examples-venv/bin/activate

python3 ./four_funcs.py --out-folder .
#rm -rf results/
python3 run.py

deactivate
