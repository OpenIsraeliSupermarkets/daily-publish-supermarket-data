#!/bin/bash
export $(cat .secrets | xargs)


docker compose stop

# Clean the app data folder
if [ -d "$APP_DATA_PATH" ]; then
    rm -rf "$APP_DATA_PATH"/.[!.]*  # Remove hidden files/folders except . and ..
    echo "Cleaned contents of $APP_DATA_PATH including hidden files"
else
    echo "Warning: $APP_DATA_PATH directory does not exist"
fi

# Clean the mongo data folder
if [ -d "$MONGO_DATA_PATH" ]; then
    rm -rf "$MONGO_DATA_PATH"/mongo_data/.[!.]*
    echo "Cleaned contents of $MONGO_DATA_PATH"
else
    echo "Warning: $MONGO_DATA_PATH directory does not exist"
fi


docker compose build --no-cache
docker compose up -d

if ! python3 system_tests/main.py; then
    echo -e "\033[31mTest Failed\033[0m"
    exit 1
fi
