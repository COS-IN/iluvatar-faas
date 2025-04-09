#!/bin/bash

source ../examples-venv/bin/activate

python3 ./four_funcs.py --out-folder .
python3 run.py

deactivate
