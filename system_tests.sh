#!/bin/bash
# Usage: ./system_tests.sh
# Validates the stack ./local_test.sh left running (API, MongoDB and the test
# Kaggle dataset), then stops it. Run it after ./local_test.sh.

source "$(dirname "$0")/test_env.sh"

echo "Step 10: Running system tests"

docker build --target testing -t supermarket-testing .
docker run \
    --network=${COMPOSE_PROJECT_NAME}_mongo-network \
    -e API_HOST=http://api:8000/ \
    -e MONGODB_URI=mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@mongodb:${MONGO_PORT} \
    -e API_TOKEN=${API_TOKEN} \
    -e KAGGLE_API_TOKEN=${KAGGLE_API_TOKEN} \
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