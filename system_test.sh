#!/bin/bash

# Usage: ./system_test.sh <database_name>
TEST_DB_NAME="$1" 
if [ -z "$TEST_DB_NAME" ]; then
  echo "Usage: $0 <database_name>"
  exit 1
fi

MONGO_IP="${2:-host.docker.internal}"
API_IP="${3:-supermarket-api}"

docker build --target testing -t supermarket-testing .
docker run \
    --network=daily-publish-supermarket-data_mongo-network \
    -e API_HOST=http://${API_IP}:8080/ \
    -e MONGODB_URI=mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@${MONGO_IP}:${MONGO_PORT} \
    -e API_TOKEN=${API_TOKEN} \
    -e KAGGLE_USERNAME=${KAGGLE_USERNAME} \
    -e KAGGLE_KEY=${KAGGLE_KEY} \
    -e KAGGLE_DATASET_REMOTE_NAME=${TEST_DB_NAME} \
    -e ENABLED_SCRAPERS=${ENABLED_SCRAPERS} \
    -e ENABLED_FILE_TYPES=${ENABLED_FILE_TYPES} \
    -e LIMIT=${LIMIT} \
    -e NUM_OF_OCCASIONS=${NUM_OF_OCCASIONS} \
    supermarket-testing