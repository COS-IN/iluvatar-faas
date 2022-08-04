#!/bin/bash

name="alfuerst/lookbusy-iluvatar-action:latest"
docker build -t $name .
docker push $name
