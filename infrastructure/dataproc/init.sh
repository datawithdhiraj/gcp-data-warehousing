#!/bin/bash

echo "Starting initialization..."

apt-get update -y

pip3 install google-cloud-secret-manager
pip3 install requests

echo "Initialization completed!"