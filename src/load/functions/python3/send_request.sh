#!/bin/bash

set -x 

port=$1 

# time curl -X POST http://localhost:$port/invoke  \
time curl -X POST http://127.0.0.1:$port/invoke  \
   -H 'Content-Type: application/json' \
   -d '{"login":"my_login","password":"my_password"}'

