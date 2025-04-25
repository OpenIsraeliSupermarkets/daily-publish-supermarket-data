#!/bin/bash
export $(cat .secrets | xargs)

# Clean the app data folder
if [ -d "$APP_DATA_PATH" ]; then
    rm -rf "$APP_DATA_PATH"/*
    echo "Cleaned contents of $APP_DATA_PATH"
else
    echo "Warning: $APP_DATA_PATH directory does not exist"
fi

# Clean the mongo data folder
if [ -d "$MONGO_DATA_PATH" ]; then
    rm -rf "$MONGO_DATA_PATH"/mongo_data/*
    echo "Cleaned contents of $MONGO_DATA_PATH"
else
    echo "Warning: $MONGO_DATA_PATH directory does not exist"
fi

docker compose stop
docker compose build --no-cache
docker compose up -d

python3 system_tests/main.py
