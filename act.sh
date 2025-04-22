#!/bin/bash
set -e

if [ -f .env.test ]; then
    export $(cat .env.test | xargs)
fi

echo "Running production API deployment workflow..."
act -W '.github/workflows/_stage_deploy_api.yml' -P self-hosted=catthehacker/ubuntu:act-latest || { echo "API deployment workflow failed. Aborting."; exit 1; }
echo "Production API deployment workflow completed"

echo "Running production scraping workflow..."
act -W '.github/workflows/_stage_scrape.yml' -P self-hosted=catthehacker/ubuntu:act-latest -j call-workflow -s GITHUB_TOKEN=$GITHUB_TOKEN || { echo "Scraping workflow failed. Aborting."; exit 1; }
echo "Production scraping workflow completed"

echo "Running production publishing workflow..."
act -W '.github/workflows/_stage_publishing.yml' -P self-hosted=catthehacker/ubuntu:act-latest  -j call-workflow -s GITHUB_TOKEN=$GITHUB_TOKEN|| { echo "Publishing workflow failed. Aborting."; exit 1; }
echo "Production publishing workflow completed"