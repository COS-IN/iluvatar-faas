#!/bin/bash

echo "Running detailed spans"
source ../examples-venv/bin/activate

python3 run.py

python3 stacked_timelime.py --log results/worker1.log --csv results/output-in.csv --output results/
deactivate

cp ./results/*.png .
