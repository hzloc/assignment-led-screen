#!/bin/bash
set -e
pip install -r requirements.txt
python -m src.producer
