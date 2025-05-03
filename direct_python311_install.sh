#!/bin/bash
# Direct Python 3.11 installation script that works around apt_pkg issues
# For Ubuntu 24.04 on Azure

set -e
echo "Direct Python 3.11 installation for Ubuntu 24.04..."

# Temporarily disable the problematic post-invoke script
echo "Disabling problematic apt hook temporarily..."
if [ -f /etc/apt/apt.conf.d/20apt-esm-hook.conf ]; then
    sudo mv /etc/apt/apt.conf.d/20apt-esm-hook.conf /etc/apt/apt.conf.d/20apt-esm-hook.conf.bak
fi

if [ -f /etc/apt/apt.conf.d/command-not-found ]; then
    sudo mv /etc/apt/apt.conf.d/command-not-found /etc/apt/apt.conf.d/command-not-found.bak
fi

# Update package lists
echo "Updating package lists..."
sudo apt-get update -y

# Install software-properties-common
echo "Installing dependencies..."
sudo apt-get install -y software-properties-common

# Add PPA without updating
echo "Adding deadsnakes PPA..."
sudo add-apt-repository -y ppa:deadsnakes/ppa

# Update again 
echo "Updating with new repository..."
sudo apt-get update -y

# Install Python 3.11
echo "Installing Python 3.11..."
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev python3.11-distutils

# Install pip for Python 3.11
echo "Installing pip for Python 3.11..."
curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
sudo python3.11 /tmp/get-pip.py
rm -f /tmp/get-pip.py

# Verify Python installation
echo "Verifying Python 3.11 installation..."
python3.11 --version

# Make Python 3.11 the default Python 3
echo "Setting Python 3.11 as the default Python version..."
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Restore the original apt configuration files 
echo "Restoring original apt configuration..."
if [ -f /etc/apt/apt.conf.d/20apt-esm-hook.conf.bak ]; then
    sudo mv /etc/apt/apt.conf.d/20apt-esm-hook.conf.bak /etc/apt/apt.conf.d/20apt-esm-hook.conf
fi

if [ -f /etc/apt/apt.conf.d/command-not-found.bak ]; then
    sudo mv /etc/apt/apt.conf.d/command-not-found.bak /etc/apt/apt.conf.d/command-not-found
fi

echo ""
echo "Python 3.11 installation complete!"
echo "You can now proceed with the setup script:"
echo "./setup.sh" 