#!/bin/bash
export $(cat .secrets | xargs)

docker compose build --no-cache
docker compose up 

python3 system_tests/main.py
