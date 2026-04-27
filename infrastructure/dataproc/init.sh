#!/bin/bash
set -e
set -x

echo "Installing Python dependencies..."

# Ensure python3 + pip3
apt-get update -y
apt-get install -y python3-pip

# Install packages globally
pip3 install --upgrade pip
pip3 install google-cloud-secret-manager
pip3 install requests

echo "Dependencies installed successfully"