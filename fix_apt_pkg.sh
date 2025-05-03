#!/bin/bash
# Script to fix the apt_pkg module issue

echo "Fixing apt_pkg module issue..."

# Install python3-apt to make sure apt_pkg is available
sudo apt-get install -y python3-apt

# Fix command-not-found database
sudo apt-get install -y --reinstall command-not-found

# Update the command-not-found database
sudo update-command-not-found

echo "apt_pkg module issue fixed!"
echo ""
echo "You can now continue with Python 3.11 installation:"
echo "sudo ./install_python311.sh" 