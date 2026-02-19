#!/bin/bash
set -euo pipefail

sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install kafka-python boto3 great-expectations==1.0.0
