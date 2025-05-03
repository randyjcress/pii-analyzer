#!/bin/bash
# Script to install Python 3.11 on Ubuntu 24.04 using deadsnakes PPA

set -e  # Exit immediately if a command exits with a non-zero status
echo "Installing Python 3.11 on Ubuntu..."

# Update package lists
echo "Updating package lists..."
sudo apt-get update

# Install software-properties-common for add-apt-repository
echo "Installing dependencies..."
sudo apt-get install -y software-properties-common

# Add deadsnakes PPA repository
echo "Adding deadsnakes PPA for Python 3.11..."
sudo add-apt-repository -y ppa:deadsnakes/ppa

# Update package lists again with the new repository
echo "Updating package lists with new repository..."
sudo apt-get update

# Install Python 3.11
echo "Installing Python 3.11..."
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev python3.11-distutils

# Verify installation
echo "Verifying Python 3.11 installation..."
python3.11 --version

# Make Python 3.11 the default Python 3
echo "Setting Python 3.11 as the default Python 3..."
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Verify the default Python version
echo "Verifying default Python version..."
python3 --version

# Install pip for Python 3.11
echo "Installing pip for Python 3.11..."
curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
sudo python3.11 /tmp/get-pip.py
rm /tmp/get-pip.py

# Verify pip installation
echo "Verifying pip installation..."
python3.11 -m pip --version

echo ""
echo "Python 3.11 installation complete!"
echo "You can now continue with the main setup script:"
echo "cd ~/pii-analysis"
echo "./setup.sh" 