#!/bin/bash

echo "Step 1: Loading environment variables from .env.prod if exists"
if [ -f .env.test ]; then
    export $(cat .env.test | xargs)
fi

echo "Step 2: Setting up test environment variables"
export MONGO_IP=mongodb
export API_IP=api
# limit the run time
export ENABLED_SCRAPERS=BAREKET
export LIMIT=10
# -- should correspond to the number of times the data should be validated
export NUM_OF_OCCASIONS=1
export REPEAT=ONCE
# ----------------------------
export STOP=ONCE
export WAIT_TIME_SECONDS=60

echo "Step 3: Stopping and removing existing Docker containers"
docker compose stop
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
docker compose build --no-cache
if [ $? -ne 0 ]; then
    echo -e "\033[31mDocker build failed. Exiting.\033[0m"
    exit 1
fi

echo "Step 7: Starting background services (MongoDB and API)"
docker compose up -d mongodb api

echo "Step 8: Starting data processor and waiting for scraping to complete"
docker compose up data_processor

echo "Step 9: print networks and running containers"
docker network ls
docker ps
echo "Step 9.1: Print API container logs"
docker logs raw-data-api


echo "Step 10: Running system tests"
if ! ./system_test.sh "${KAGGLE_DATASET_REMOTE_NAME}" "${MONGO_IP}" "${API_IP}" ; then
    echo -e "\033[31mTest Failed\033[0m"
    exit 1
fi

echo "Step 10: Stopping all containers"
docker compose stop