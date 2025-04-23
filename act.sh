#!/bin/bash
export $(cat .env.unittest | xargs)

if [ -z "$ACCESS_TOKEN" ]; then
    echo "ERROR: ACCESS_TOKEN environment variable is not set"
    exit 1
fi

echo "Running production API deployment workflow..."
act -W '.github/workflows/_stage_deploy_api.yml' -P self-hosted=catthehacker/ubuntu:act-latest || { echo "API deployment workflow failed. Aborting."; exit 1; }
echo "Production API deployment workflow completed"

echo "Running production scraping workflow..."
act schedule -W '.github/workflows/_stage_scrape.yml' -P self-hosted=catthehacker/ubuntu:act-latest -s GITHUB_TOKEN=$ACCESS_TOKEN  || { echo "Scraping workflow failed. Aborting."; exit 1; }
echo "Production scraping workflow completed"

python3 data_validation/main.py

echo "Running production publishing workflow..."
act schedule -W '.github/workflows/_stage_publishing.yml' -P self-hosted=catthehacker/ubuntu:act-latest  -s GITHUB_TOKEN=$ACCESS_TOKEN --secret-file .secrets || { echo "Publishing workflow failed. Aborting."; exit 1; }
echo "Production publishing workflow completed"