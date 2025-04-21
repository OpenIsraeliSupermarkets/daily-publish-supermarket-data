#!/bin/bash
if [ -f .env.test ]; then
    export $(cat .env.test | xargs)
fi

echo "Running production API deployment workflow..."
act -W '.github/workflows/_prod_deploy_api.yml' -P self-hosted=catthehacker/ubuntu:act-latest
echo "Production API deployment workflow completed"

echo "Running production scraping workflow..."
act -W '.github/workflows/_prod_scrape.yml' -P self-hosted=catthehacker/ubuntu:act-latest
echo "Production scraping workflow completed"

echo "Running production publishing workflow..."
act -W '.github/workflows/_prod_publishing.yml' -P self-hosted=catthehacker/ubuntu:act-latest
echo "Production publishing workflow completed"