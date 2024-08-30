#!/bin/bash
set -e
pip install -r requirements.txt
exec python -m src.producer & exec python -m src.consumer
