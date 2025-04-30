#!/bin/bash

echo "Step 1: Loading environment variables from .env.prod if exists"
if [ -f .env.prod ]; then
    export $(cat .env.prod | xargs)
fi

echo "Step 2: Setting up test environment variables"
# override the kaggle dataset
export KAGGLE_DATASET_REMOTE_NAME=test-super-dataset
export DOCKER_HOST=host.docker.internal
# limit the run time
export ENABLED_SCRAPERS=BAREKET
export LIMIT=10
export NUM_OF_OCCASIONS=1
export OPERATION=scraping,converting,clean_dump_files,api_update,publishing,clean_all_source_data

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

echo "Step 7: Starting background services (MongoDB and API)"
docker compose up -d mongodb api

echo "Step 8: Starting data processor and waiting for scraping to complete"
docker compose up data_processor

echo "Step 9: Running system tests"
if ! ./system_test.sh "${KAGGLE_DATASET_REMOTE_NAME}" "${DOCKER_HOST}"; then
    echo -e "\033[31mTest Failed\033[0m"
    exit 1
fi

echo "Step 10: Stopping all containers"
docker compose stop