#!/bin/bash

set -e  # Exit on error

echo "============================================"
echo "  Supermarket Data - Local Setup Script"
echo "============================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

# Check if Python 3 is installed
print_info "Step 1: Checking Python installation..."
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
print_success "Python $PYTHON_VERSION found"

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    print_error "pip3 is not installed. Please install pip3."
    exit 1
fi
print_success "pip3 found"

# Create virtual environment
print_info "Step 2: Setting up Python virtual environment..."
if [ -d "venv" ]; then
    print_info "Virtual environment already exists. Skipping creation."
else
    python3 -m venv venv
    print_success "Virtual environment created"
fi

# Activate virtual environment
print_info "Activating virtual environment..."
source venv/bin/activate
print_success "Virtual environment activated"

# Upgrade pip
print_info "Step 3: Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1
print_success "pip upgraded"

# Install dependencies
print_info "Step 4: Installing Python dependencies..."
print_info "Installing production dependencies from requirements.txt..."
pip install -r requirements.txt > /dev/null 2>&1
print_success "Production dependencies installed"

print_info "Installing development dependencies from requirements-dev.txt..."
pip install -r requirements-dev.txt > /dev/null 2>&1
print_success "Development dependencies installed"

# Install python-dotenv for environment variable management
print_info "Installing python-dotenv..."
pip install python-dotenv > /dev/null 2>&1
print_success "python-dotenv installed"

# Create necessary directories
print_info "Step 5: Creating necessary directories..."
mkdir -p app_data
mkdir -p mongo_data
print_success "Directories created (app_data, mongo_data)"

# # Create .env.unittest if it doesn't exist
# print_info "Step 6: Setting up environment files..."
# if [ ! -f ".env.unittest" ]; then
#     cat > .env.unittest << 'EOF'
# # Test Environment Variables
# MONGODB_URI=mongodb://testuser:testpass@localhost:27017/
# MONGO_USERNAME=testuser
# MONGO_PASSWORD=testpass
# MONGO_URL=localhost
# MONGO_PORT=27017
# MONGO_DATA_PATH=./mongo_data

# # Kaggle Configuration (use test values or your actual credentials)
# KAGGLE_KEY=dummy_key_for_testing
# KAGGLE_USERNAME=dummy_username
# KAGGLE_DATASET_REMOTE_NAME=test/dataset
# TEST_DB_NAME=test_dataset

# # Supabase Configuration (use test values or your actual credentials)
# SUPABASE_URL=https://dummy.supabase.co
# SUPABASE_KEY=dummy_key

# # API Configuration
# API_TOKEN=test_token_12345

# # Application Configuration
# APP_DATA_PATH=./app_data
# ENABLED_SCRAPERS=COFIX
# ENABLED_FILE_TYPES=PROMO_FILE,STORE_FILE,PRICE_FILE
# LOG_LEVEL=DEBUG
# LIMIT=10
# NUM_OF_PROCESSES=2
# OUTPUT_DESTINATION=mongo
# OPERATION=scraping
# STOP=ONCE
# REPEAT=ONCE
# WAIT_TIME_SECONDS=60
# HEALTHCHECK_MAX_AGE_SECONDS=300
# EOF
#     print_success ".env.unittest created (configured for local testing)"
# else
#     print_info ".env.unittest already exists. Skipping creation."
# fi

# # Create .env.test if it doesn't exist
# if [ ! -f ".env.test" ]; then
#     cat > .env.test << 'EOF'
# # Test Environment Variables for Docker Compose
# MONGODB_URI=mongodb://testuser:testpass@mongodb:27017/
# MONGO_USERNAME=testuser
# MONGO_PASSWORD=testpass
# MONGO_URL=mongodb
# MONGO_PORT=27017
# MONGO_DATA_PATH=./mongo_data

# # Kaggle Configuration
# KAGGLE_KEY=your_kaggle_key_here
# KAGGLE_USERNAME=your_kaggle_username
# KAGGLE_DATASET_REMOTE_NAME=your_username/test-dataset

# # Supabase Configuration
# SUPABASE_URL=https://your-project.supabase.co
# SUPABASE_KEY=your_supabase_key_here

# # API Configuration
# API_TOKEN=test_token_12345

# # Application Configuration
# APP_DATA_PATH=./app_data
# ENABLED_SCRAPERS=COFIX
# ENABLED_FILE_TYPES=PROMO_FILE,STORE_FILE,PRICE_FILE
# LOG_LEVEL=INFO
# LIMIT=10
# NUM_OF_PROCESSES=2
# OUTPUT_DESTINATION=mongo
# OPERATION=
# STOP=ONCE
# REPEAT=ONCE
# WAIT_TIME_SECONDS=60
# HEALTHCHECK_MAX_AGE_SECONDS=300
# EOF
#     print_success ".env.test created (configured for Docker Compose testing)"
# else
#     print_info ".env.test already exists. Skipping creation."
# fi

# # Create .env for local development if it doesn't exist
# if [ ! -f ".env" ]; then
#     cat > .env << 'EOF'
# # Local Development Environment Variables
# MONGODB_URI=mongodb://localhost:27017/
# MONGO_USERNAME=admin
# MONGO_PASSWORD=admin123
# MONGO_URL=localhost
# MONGO_PORT=27017
# MONGO_DATA_PATH=./mongo_data

# # Kaggle Configuration - REPLACE WITH YOUR ACTUAL CREDENTIALS
# KAGGLE_KEY=your_kaggle_key_here
# KAGGLE_USERNAME=your_kaggle_username
# KAGGLE_DATASET_REMOTE_NAME=your_username/your-dataset

# # Supabase Configuration - REPLACE WITH YOUR ACTUAL CREDENTIALS
# SUPABASE_URL=https://your-project.supabase.co
# SUPABASE_KEY=your_supabase_key_here

# # API Configuration
# API_TOKEN=local_dev_token_12345

# # Application Configuration
# APP_DATA_PATH=./app_data
# ENABLED_SCRAPERS=COFIX,VICTORY
# ENABLED_FILE_TYPES=PROMO_FILE,STORE_FILE,PRICE_FILE,PROMO_FULL_FILE,PRICE_FULL_FILE
# LOG_LEVEL=INFO
# LIMIT=50
# NUM_OF_PROCESSES=5
# OUTPUT_DESTINATION=mongo
# OPERATION=
# STOP=EOD
# REPEAT=FOREVER
# WAIT_TIME_SECONDS=1800
# HEALTHCHECK_MAX_AGE_SECONDS=300
# EOF
#     print_success ".env created (configured for local development)"
#     print_info "Please edit .env and add your actual Kaggle and Supabase credentials!"
# else
#     print_info ".env already exists. Skipping creation."
# fi

# # Check if Docker is installed
# print_info "Step 7: Checking Docker installation..."
# if ! command -v docker &> /dev/null; then
#     print_error "Docker is not installed. Please install Docker to run MongoDB locally."
#     print_info "You can still run unit tests without Docker."
# else
#     DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | tr -d ',')
#     print_success "Docker $DOCKER_VERSION found"
    
#     # Ask if user wants to start MongoDB
#     echo ""
#     read -p "Do you want to start MongoDB with Docker Compose? (y/N): " START_MONGO
#     START_MONGO=${START_MONGO:-N}
    
#     if [[ "$START_MONGO" =~ ^[Yy]$ ]]; then
#         print_info "Starting MongoDB container..."
        
#         # Load environment variables
#         if [ -f ".env" ]; then
#             export $(cat .env | grep -v '^#' | xargs)
#         fi
        
#         # Start only MongoDB service
#         docker compose up -d mongodb
        
#         if [ $? -eq 0 ]; then
#             print_success "MongoDB started successfully"
#             print_info "MongoDB is running on localhost:27017"
#             print_info "Username: $MONGO_USERNAME"
#             print_info "Password: $MONGO_PASSWORD"
#         else
#             print_error "Failed to start MongoDB"
#         fi
#     fi
# fi

# Display next steps
echo ""
echo "============================================"
print_success "Setup completed successfully!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Activate virtual environment:"
echo "     ${GREEN}source venv/bin/activate${NC}"
echo ""
echo "  2. Edit .env file with your actual credentials:"
echo "     - KAGGLE_KEY and KAGGLE_USERNAME"
echo "     - SUPABASE_URL and SUPABASE_KEY"
echo ""
echo "  3. Run unit tests:"
echo "     ${GREEN}python -m pytest${NC}"
echo ""
echo "  4. Run specific tests:"
echo "     ${GREEN}python -m pytest tests/test_complete_integration.py${NC}"
echo ""
echo "  5. Start the API server locally:"
echo "     ${GREEN}python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000${NC}"
echo ""
echo "  6. Run full integration test with Docker:"
echo "     ${GREEN}./local_test.sh${NC}"
echo ""
echo "  7. Check API documentation:"
echo "     ${GREEN}http://localhost:8000/docs${NC}"
echo ""
echo "Environment files created:"
echo "  - .env            (local development)"
echo "  - .env.test       (Docker Compose integration tests)"
echo "  - .env.unittest   (unit tests)"
echo ""

