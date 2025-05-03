#!/bin/bash
# Standalone script to install Docker and Apache Tika
# For Ubuntu 24.04 on Azure

set -e  # Exit immediately if a command exits with a non-zero status
echo "Installing Docker and Apache Tika..."

# Install Docker prerequisites
echo "Installing Docker prerequisites..."
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
echo "Adding Docker GPG key..."
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up Docker repository
echo "Setting up Docker repository..."
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
echo "Installing Docker Engine..."
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin docker-compose

# Add current user to the docker group
echo "Adding user to docker group..."
sudo usermod -aG docker $USER
echo "NOTE: You'll need to log out and back in for Docker permissions to take effect"

# Test Docker installation
echo "Testing Docker installation..."
sudo docker run --rm hello-world

# Pull Tika Docker image
echo "Pulling Apache Tika Docker image..."
sudo docker pull apache/tika:2.6.0

# Show Docker images
echo "Docker images:"
sudo docker image ls

# Create a script to start Tika server
echo "Creating Tika server script..."
cat > ~/start_tika.sh << 'EOF'
#!/bin/bash
# Start Apache Tika server
echo "Starting Apache Tika server..."

# Check if container already exists
if docker ps -a | grep -q "tika"; then
    echo "Removing existing Tika container..."
    docker rm -f tika
fi

# Start a new container
docker run -d --name tika -p 9998:9998 apache/tika:2.6.0
echo "Tika server is running at http://localhost:9998"
echo "Check Tika status with: curl http://localhost:9998/tika"
EOF

chmod +x ~/start_tika.sh

# Create a script to stop Tika server
cat > ~/stop_tika.sh << 'EOF'
#!/bin/bash
# Stop Apache Tika server
echo "Stopping Apache Tika server..."
docker stop tika
echo "Tika server stopped"
EOF

chmod +x ~/stop_tika.sh

echo ""
echo "Docker and Apache Tika installation complete!"
echo ""
echo "To start Tika server:"
echo "  ~/start_tika.sh"
echo ""
echo "To stop Tika server:"
echo "  ~/stop_tika.sh"
echo ""
echo "For the PII analyzer to use Tika, add this to your environment:"
echo "  export TIKA_SERVER_ENDPOINT=\"http://localhost:9998\""
echo ""
echo "Important: You'll need to log out and back in for Docker permissions to take effect." 