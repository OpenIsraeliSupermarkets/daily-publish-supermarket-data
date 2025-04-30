#!/bin/bash
if [ -f .env.prod ]; then
    export $(cat .env.prod | xargs)
fi

# override the kaggle dataset
export KAGGLE_DATASET_REMOTE_NAME=test-super-dataset

# limit the run time
export ENABLED_SCRAPERS=BAREKET
export LIMIT=10
export NUM_OF_OCCASIONS=1
export OPERATION=scraping,converting,clean_dump_files,api_update,publishing,clean_all_source_data


# docker compose stop
# docker compose rm -f

# Clean the app data folder
if [ -d "$APP_DATA_PATH" ]; then
    rm -rf "$APP_DATA_PATH"  # Remove hidden files/folders except . and ..
    mkdir -p "$APP_DATA_PATH"
    echo "Cleaned contents of $APP_DATA_PATH including hidden files"
else
    echo "Warning: $APP_DATA_PATH directory does not exist"
fi

# Clean the mongo data folder
if [ -d "$MONGO_DATA_PATH" ]; then
    rm -rf "$MONGO_DATA_PATH/mongo_data"
    mkdir -p "$MONGO_DATA_PATH/mongo_data"
    echo "Cleaned contents of $MONGO_DATA_PATH"
else
    echo "Warning: $MONGO_DATA_PATH directory does not exist"
fi

# clean too
# docker compose build --no-cache

# # start background services
# docker compose up -d mongodb api

# # start data processor and wait for scraping to complete.
# docker compose up data_processor


if ! ./system_test.sh "${KAGGLE_DATASET_REMOTE_NAME}"; then
    echo -e "\033[31mTest Failed\033[0m"
    exit 1
fi

# docker compose stop
