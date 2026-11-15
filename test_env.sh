#!/bin/bash
# The system test environment, sourced by local_test.sh and system_tests.sh.

echo "Step 1: Loading environment variables from .env.test if exists"
if [ -f .env.test ]; then
    export $(cat .env.test | xargs)
fi

echo "Step 2: Setting up test environment variables"
# Step 10 joins the compose network by name, and compose derives that name from
# the checkout folder. Pin it so the test works from any folder or worktree.
export COMPOSE_PROJECT_NAME=publisher
# Same overrides as the CI system test (see docker-compose.ci.yml).
export COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml:docker-compose.ci.yml}
# limit the run time — keep in sync with tests.validation_utils.scrapers_to_test()
export ENABLED_SCRAPERS=BAREKET,WOLT
export LIMIT=10
export OUTPUT_DESTINATION=mongo
# -- should correspond to the number of times the data should be validated
export NUM_OF_OCCASIONS=1
