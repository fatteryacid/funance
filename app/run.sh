#!/bin/bash
cd /home/tyler/repos/funance/app
source ./venv/bin/activate
python webscraper.py && python data.py