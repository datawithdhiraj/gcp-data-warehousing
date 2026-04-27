#!/bin/bash

# set -e
# set -x

echo "Starting initialization..."

# # Avoid long apt update (optional but safer)
# apt-get update -y -qq

# # Ensure pip3 exists
# apt-get install -y python3-pip -qq

# # Upgrade pip quickly
# pip3 install --upgrade pip --no-cache-dir

# # Install only required libs (fast)
pip3 install --no-cache-dir google-cloud-secret-manager
pip3 install --no-cache-dir requests

echo "Initialization completed!"