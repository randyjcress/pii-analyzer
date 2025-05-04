#!/bin/bash
# Script to set up and configure multiple Tika instances with load balancing

# Set strict error handling
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Setting up Tika Load Balancer${NC}"
echo "This script will configure multiple Tika instances for load balancing."

# Check if docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed.${NC}"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: docker-compose is not installed.${NC}"
    echo "Please install docker-compose first: https://docs.docker.com/compose/install/"
    exit 1
fi

# Stop any running Tika containers
echo -e "${YELLOW}Stopping any running Tika containers...${NC}"
docker-compose down

# Check if .env file exists, create it if not
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file from template...${NC}"
    cp .env-template .env
fi

# Update .env file to use load balancing
echo -e "${YELLOW}Updating .env file for load balancing...${NC}"
# Check if TIKA_SERVER_ENDPOINTS already exists in .env
if grep -q "TIKA_SERVER_ENDPOINTS" .env; then
    echo "TIKA_SERVER_ENDPOINTS already configured in .env"
else
    echo "Adding TIKA_SERVER_ENDPOINTS to .env"
    echo "TIKA_SERVER_ENDPOINTS=http://localhost:9998,http://localhost:9999,http://localhost:10000" >> .env
fi

# Check if USE_TIKA_LOAD_BALANCER exists
if grep -q "USE_TIKA_LOAD_BALANCER" .env; then
    # Update the value if it exists
    sed -i.bak 's/USE_TIKA_LOAD_BALANCER=.*/USE_TIKA_LOAD_BALANCER=true/' .env && rm -f .env.bak
else
    # Add it if it doesn't exist
    echo "USE_TIKA_LOAD_BALANCER=true" >> .env
fi

# Start the Tika containers
echo -e "${YELLOW}Starting Tika containers...${NC}"
docker-compose up -d

# Wait for containers to be ready
echo -e "${YELLOW}Waiting for Tika services to be ready...${NC}"
sleep 5

# Check if containers are running
if docker-compose ps | grep -q "tika1"; then
    echo -e "${GREEN}Tika1 service is running.${NC}"
else
    echo -e "${RED}Error: Tika1 service failed to start.${NC}"
fi

if docker-compose ps | grep -q "tika2"; then
    echo -e "${GREEN}Tika2 service is running.${NC}"
else
    echo -e "${RED}Error: Tika2 service failed to start.${NC}"
fi

if docker-compose ps | grep -q "tika3"; then
    echo -e "${GREEN}Tika3 service is running.${NC}"
else
    echo -e "${RED}Error: Tika3 service failed to start.${NC}"
fi

# Test Tika endpoints
echo -e "${YELLOW}Testing Tika endpoints...${NC}"

test_tika_endpoint() {
    local endpoint=$1
    if curl --silent --max-time 5 "$endpoint/tika" > /dev/null; then
        echo -e "${GREEN}✓ $endpoint is responsive${NC}"
        return 0
    else
        echo -e "${RED}✗ $endpoint is not responsive${NC}"
        return 1
    fi
}

test_tika_endpoint "http://localhost:9998"
test_tika_endpoint "http://localhost:9999"
test_tika_endpoint "http://localhost:10000"

echo -e "${GREEN}Setup complete!${NC}"
echo "The Tika load balancer is now configured with 3 Tika instances."
echo -e "To monitor Tika usage, check ${BLUE}docker-compose logs -f${NC} or use the monitoring script."
echo -e "To visualize the load balancing, add ${BLUE}logger.debug(f\"Tika stats: {json.dumps(extractor.get_tika_stats())}\")${NC}"
echo "to your code after using the extractor." 