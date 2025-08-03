#!/bin/bash

echo "Running code-zip-upload"
source ../examples-venv/bin/activate

tar -czf code.tar.gz ./cnn_image_classification/
python3 run.py

deactivate
