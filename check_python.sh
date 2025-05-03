#!/bin/bash
# Script to check Python installation and reinstall Python 3.11 if needed

# Check if Python is installed
echo "Checking Python installation..."
python3_exists=$(command -v python3)
if [ -z "$python3_exists" ]; then
    echo "Python 3 is not installed"
    need_install=true
else
    echo "Python 3 exists: $python3_exists"
    python_version=$(python3 --version 2>&1)
    echo "Python version: $python_version"
    
    if [[ "$python_version" == *"3.11"* ]]; then
        echo "✅ Python 3.11 is installed correctly"
        need_install=false
    else
        echo "❌ Python 3.11 is not installed (found $python_version)"
        need_install=true
    fi
fi

# Check for Python 3.11 specifically
python311_exists=$(command -v python3.11)
if [ -z "$python311_exists" ]; then
    echo "Python 3.11 executable not found"
    need_install=true
else
    echo "Python 3.11 exists: $python311_exists"
    python311_version=$(python3.11 --version 2>&1)
    echo "Python 3.11 version: $python311_version"
fi

# Check if pip is installed
pip_exists=$(command -v pip)
pip3_exists=$(command -v pip3)
echo "Pip exists: $(if [ -z "$pip_exists" ]; then echo "No"; else echo "Yes - $pip_exists"; fi)"
echo "Pip3 exists: $(if [ -z "$pip3_exists" ]; then echo "No"; else echo "Yes - $pip3_exists"; fi)"

# Install Python 3.11 if needed
if [ "$need_install" = true ]; then
    echo "Installing Python 3.11..."
    
    # Check if we need to add deadsnakes repository
    if ! grep -q "deadsnakes" /etc/apt/sources.list /etc/apt/sources.list.d/*; then
        echo "Adding deadsnakes PPA..."
        sudo apt update
        sudo apt install -y software-properties-common
        sudo add-apt-repository -y ppa:deadsnakes/ppa
        sudo apt update
    fi
    
    # Install Python 3.11
    sudo apt install -y python3.11 python3.11-venv python3.11-dev python3.11-distutils
    
    # Check if installation was successful
    if command -v python3.11 >/dev/null; then
        echo "✅ Python 3.11 installed successfully"
        python3.11 --version
    else
        echo "❌ Python 3.11 installation failed"
        exit 1
    fi
    
    # Make Python 3.11 the default Python 3
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
    
    # Install pip for Python 3.11
    echo "Installing pip for Python 3.11..."
    curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3.11
    
    # Verify pip installation
    if python3.11 -m pip --version; then
        echo "✅ pip installed successfully"
    else
        echo "❌ pip installation failed"
        exit 1
    fi
else
    echo "No need to reinstall Python 3.11"
fi

# Create a virtual environment for testing
echo "Creating a test virtual environment..."
python3 -m venv ~/test-venv
source ~/test-venv/bin/activate

# Check the Python version in the virtual environment
echo "Python version in virtual environment:"
python --version

# Clean up
deactivate
rm -rf ~/test-venv

echo "Python check complete!"
echo "---------------------"
echo "If Python 3.11 is installed correctly, you can proceed with the setup:"
echo "cd ~/pii-analysis"
echo "./setup.sh" 