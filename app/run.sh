#!/bin/bash
cd /home/tyler/repos/funance/app
source ./venv/bin/activate

python main.py

cd ./dbt
dbt run