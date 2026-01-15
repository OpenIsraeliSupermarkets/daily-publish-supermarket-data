#!/bin/bash

# Usage: ./local_test.sh [--incremental|--persist] [--no-cache]
# Arguments:
#   --incremental: Run Full Production Flow (scrape then publish as separate executions) - default
#   --persist: Run Loop/Persistent mode (runs continuously)
#   --no-cache: Build Docker images without cache

TEST_MODE=""
BUILD_NO_CACHE="N"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --incremental)
            TEST_MODE="production"
            shift
            ;;
        --persist)
            TEST_MODE="loop"
            shift
            ;;
        --no-cache)
            BUILD_NO_CACHE="y"
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: ./local_test.sh [--incremental|--persist] [--no-cache]"
            exit 1
            ;;
    esac
done

# If test mode not provided via flags, prompt interactively
if [ -z "$TEST_MODE" ]; then
    echo ""
    echo "Select test mode:"
    echo "1) --incremental - Full Production Flow (default - run scrape then publish as separate executions)"
    echo "2) --persist - Loop/Persistent mode (runs continuously)"
    read -p "Enter choice (incremental/persist): (Enter for incremental) " TEST_MODE_INPUT
    TEST_MODE_INPUT=${TEST_MODE_INPUT:-incremental}
    
    if [ "$TEST_MODE_INPUT" == "persist" ]; then
        TEST_MODE="loop"
    else
        TEST_MODE="production"
    fi
    
    echo ""
    read -p "Do you want to build without cache? (y/N): (Enter for N) " BUILD_NO_CACHE
    BUILD_NO_CACHE=${BUILD_NO_CACHE:-N}
fi

echo "Step 1: Loading environment variables from .env.prod if exists"
if [ -f .env.test ]; then
    export $(cat .env.test | xargs)
fi

echo "Step 2: Setting up test environment variables"
# limit the run time
export ENABLED_SCRAPERS=COFIX
export LIMIT=10
export OUTPUT_DESTINATION=mongo
# -- should correspond to the number of times the data should be validated
export NUM_OF_OCCASIONS=1


# Function to check container health
check_container_health() {
    local container_name=$1
    local retries=20
    local wait_seconds=5
    local i=0

    while [ $i -lt $retries ]; do
        health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container_name" 2>/dev/null)
        if [ "$health_status" == "healthy" ]; then
            echo "$container_name is healthy."
            return 0
        elif [ "$health_status" == "unhealthy" ]; then
            echo -e "\033[31m$container_name is unhealthy. Exiting.\033[0m"
            return 1
        else
            echo "Waiting for $container_name to become healthy... ($((i+1))/$retries)"
            sleep $wait_seconds
        fi
        i=$((i+1))
    done

    echo -e "\033[31mTimeout waiting for $container_name to become healthy. Exiting.\033[0m"
    return 1
}

echo "Step 3: Stopping and removing existing Docker containers"
docker compose down -v
docker compose rm -f

echo "Step 4: Cleaning app data directory"
# Clean the app data folder
if [ -d "$APP_DATA_PATH" ]; then
    rm -rf "$APP_DATA_PATH"  # Remove hidden files/folders except . and ..
    echo "Cleaned contents of $APP_DATA_PATH including hidden files"
fi

mkdir -p "$APP_DATA_PATH"

echo "Step 5: Cleaning MongoDB data directory"
# Clean the mongo data folder
if [ -d "$MONGO_DATA_PATH" ]; then
    rm -rf "$MONGO_DATA_PATH/mongo_data"
    echo "Cleaned contents of $MONGO_DATA_PATH"
fi

mkdir -p "$MONGO_DATA_PATH/mongo_data"

echo "Step 6: Rebuilding Docker containers without cache"
if [[ "$BUILD_NO_CACHE" =~ ^[Yy]$ ]]; then
    docker compose build --no-cache
fi

if [ $? -ne 0 ]; then
    echo -e "\033[31mDocker build failed. Exiting.\033[0m"
    exit 1
fi

echo "Step 7: Starting background services (MongoDB and API)"
docker compose up -d mongodb api

echo "Step 8: Starting data processor"

if [ "$TEST_MODE" == "production" ]; then
    echo "Step 8.1: Running SCRAPE operation (scraping,converting,clean_dump_files,api_update)"
    export OPERATION="scraping,converting,clean_dump_files,api_update"
    docker compose up data_processor
    
    if [ $? -ne 0 ]; then
        echo -e "\033[31mScrape operation failed. Exiting.\033[0m"
        exit 1
    fi
    
    echo "Step 8.2: Scrape operation completed successfully"
    
    echo "Step 8.3: Running PUBLISHING operation (publishing,clean_all_source_data)"
    export OPERATION="publishing,clean_all_source_data"
    docker compose up data_processor
    
    if [ $? -ne 0 ]; then
        echo -e "\033[31mPublishing operation failed. Exiting.\033[0m"
        exit 1
    fi
    
    echo "Step 8.4: Publishing operation completed successfully"
else
    echo "Step 8.1: Running in Loop/Persistent mode"
    
    export STOP_DAG_CONDITION=NEVER
    export SECOND_TO_WAIT_BETWEEN_OPERATIONS=0
    export EXEC_FINAL_OPERATIONS_CONDITION=ONCE
    export SECOND_TO_WAIT_AFTER_FINAL_OPERATIONS=60000
    
    docker compose up data_processor -d
    
    echo "Step 8.2: Waiting 2 minutes before proceeding with a progress bar..."
    wait_seconds=120
    for ((i=1; i<=wait_seconds; i++)); do
        bar=$(printf '#%.0s' $(seq 1 $((i*90/wait_seconds))))
        printf "\rWaiting: [%-90s] %d/%d seconds" "$bar" "$i" "$wait_seconds"
        sleep 1
    done
    echo -e "\nDone waiting."

    check_container_health "raw-data-api" || exit 1
    check_container_health "data-fetcher" || exit 1

fi

echo "Step 9: Checking health of API and data_processor containers"

check_container_health "raw-data-api" || exit 1

if [ "$TEST_MODE" == "loop" ]; then
    check_container_health "data-fetcher" || exit 1
fi

echo "Step 10: Running system tests"

docker build --target testing -t supermarket-testing .
docker run \
    --network=daily-publish-supermarket-data_mongo-network \
    -e API_HOST=http://api:8000/ \
    -e MONGODB_URI=mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@mongodb:${MONGO_PORT} \
    -e API_TOKEN=${API_TOKEN} \
    -e KAGGLE_USERNAME=${KAGGLE_USERNAME} \
    -e KAGGLE_KEY=${KAGGLE_KEY} \
    -e KAGGLE_DATASET_REMOTE_NAME=${KAGGLE_DATASET_REMOTE_NAME} \
    -e ENABLED_SCRAPERS=${ENABLED_SCRAPERS} \
    -e ENABLED_FILE_TYPES=${ENABLED_FILE_TYPES} \
    -e LIMIT=${LIMIT} \
    -e NUM_OF_OCCASIONS=${NUM_OF_OCCASIONS} \
    -e SUPABASE_KEY=${SUPABASE_KEY} \
    -e SUPABASE_URL=${SUPABASE_URL} \
    supermarket-testing

if [ $? -ne 0 ]; then
    echo -e "\033[31mSystem tests failed. Exiting.\033[0m"
    exit 1
fi

echo "Step 11: Stopping all containers"
docker compose stop