#!/bin/bash
# Safe setup script for PII Analyzer on Ubuntu 24.04
# Works around apt_pkg module issues

set -e  # Exit immediately if a command exits with a non-zero status
echo "Starting PII Analyzer safe setup on Ubuntu 24.04..."

# Function to display progress
progress() {
    echo "==============================================="
    echo "   $1"
    echo "==============================================="
}

# Disable problematic apt hooks
progress "Disabling problematic apt hooks"
if [ -f /etc/apt/apt.conf.d/command-not-found ]; then
    sudo mv /etc/apt/apt.conf.d/command-not-found /etc/apt/apt.conf.d/command-not-found.disabled
    echo "Disabled command-not-found hook"
fi

# Always run apt-get update with the -o option to disable post-invoke scripts
safe_apt_update() {
    sudo apt-get -o APT::Update::Post-Invoke-Success="" update
}

safe_apt_install() {
    sudo apt-get -o APT::Update::Post-Invoke-Success="" install -y "$@"
}

# Update system packages
progress "Updating system packages (safely)"
safe_apt_update

# Install essential packages
progress "Installing essential packages"
safe_apt_install build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev curl \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev \
    libffi-dev liblzma-dev pkg-config libpq-dev git \
    unzip wget software-properties-common htop dstat iotop

# Install Tesseract OCR and document handling utilities
progress "Installing Tesseract OCR and document utilities"
safe_apt_install tesseract-ocr libtesseract-dev tesseract-ocr-eng \
    poppler-utils antiword odt2txt

# Add deadsnakes PPA and install Python 3.11
progress "Adding deadsnakes PPA for Python 3.11"
sudo add-apt-repository -y ppa:deadsnakes/ppa
safe_apt_update

progress "Installing Python 3.11"
safe_apt_install python3.11 python3.11-venv python3.11-dev python3.11-distutils

# Install pip directly
progress "Installing pip for Python 3.11"
curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
sudo python3.11 /tmp/get-pip.py
rm -f /tmp/get-pip.py

# Set Python 3.11 as default
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Create virtual environment
progress "Creating virtual environment"
python3 -m venv ~/pii-venv
source ~/pii-venv/bin/activate

# Install Python packages
progress "Installing Python dependencies"
pip install --upgrade pip wheel setuptools

# Install exact versions from requirements.txt
progress "Installing specific dependencies from requirements.txt"
pip install tika==2.6.0
pip install presidio-analyzer==2.2.351 presidio-anonymizer==2.2.351
pip install spacy==3.7.2
pip install pdf2image==1.16.3
pip install pytesseract==0.3.10
pip install Pillow==10.3.0
pip install click==8.1.7

# Support libraries
pip install pandas==2.0.3
pip install pydantic==2.3.0
pip install numpy==1.24.4
pip install requests==2.31.0
pip install rich==13.7.0
pip install setproctitle==1.3.3
pip install psutil==7.0.0

# Install NLP models
progress "Downloading spaCy models"
python -m spacy download en_core_web_lg
python -m spacy download en_core_web_sm

# Install NLTK models
progress "Downloading NLTK models"
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('averaged_perceptron_tagger')"

# Setup for large-scale processing
progress "Optimizing system for large-scale processing"

# Increase file descriptor limits
echo "fs.file-max = 2097152" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
echo "* soft nofile 1048576" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 1048576" | sudo tee -a /etc/security/limits.conf
echo "session required pam_limits.so" | sudo tee -a /etc/pam.d/common-session

# Configure SQLite for better performance
progress "Configuring SQLite optimizations"
echo 'alias sqlite3="sqlite3 -init ~/.sqliterc"' >> ~/.bashrc
echo ".timeout 60000" > ~/.sqliterc
echo ".mode column" >> ~/.sqliterc

# Create working directories
progress "Creating working directories"
mkdir -p ~/pii-data
mkdir -p ~/pii-results

# Configure environment variables
progress "Setting up environment variables"
echo 'export PATH="$HOME/pii-venv/bin:$PATH"' >> ~/.bashrc
echo 'export PYTHONPATH="$HOME/pii-analysis:$PYTHONPATH"' >> ~/.bashrc
echo 'export PII_WORKER_COUNT=85' >> ~/.bashrc  # Set to ~90% of available cores
echo 'export PII_MEMORY_LIMIT=4096' >> ~/.bashrc  # 4GB per worker in MB

# Create a script to activate the environment and run the tool
progress "Creating helper scripts"
cat > ~/run_pii.sh << 'EOF'
#!/bin/bash
source ~/pii-venv/bin/activate
cd ~/pii-analysis
python3 src/process_files.py "$@"
EOF

chmod +x ~/run_pii.sh

# Create a script to pull latest code
cat > ~/update_pii.sh << 'EOF'
#!/bin/bash
cd ~/pii-analysis
git pull
source ~/pii-venv/bin/activate
pip install -r requirements.txt
EOF

chmod +x ~/update_pii.sh

# Create monitoring script
cat > ~/monitor_pii.sh << 'EOF'
#!/bin/bash
source ~/pii-venv/bin/activate
cd ~/pii-analysis
watch -n 60 "python3 inspect_db.py --db-path \$1 --show-speed --time-window 60"
EOF

chmod +x ~/monitor_pii.sh

# Restore apt hooks if needed
progress "Restoring apt configuration"
if [ -f /etc/apt/apt.conf.d/command-not-found.disabled ]; then
    sudo mv /etc/apt/apt.conf.d/command-not-found.disabled /etc/apt/apt.conf.d/command-not-found
fi

progress "Setup complete! Follow these steps to start processing:"
echo ""
echo "1. Use these scripts to run the PII analyzer:"
echo "   ~/run_pii.sh /path/to/files --db-path results.db --monitor --workers 85"
echo ""
echo "2. Monitor processing in another terminal:"
echo "   ~/monitor_pii.sh results.db"
echo ""
echo "System configuration:"
echo "- Python 3.11 installed with required packages"
echo "- Virtual environment at ~/pii-venv"
echo "- Worker count set to 85 (optimized for E96as_v5)"
echo "- Memory limit set to 4GB per worker"
echo ""
echo "You may need to restart your shell or run 'source ~/.bashrc' for environment variables to take effect." 