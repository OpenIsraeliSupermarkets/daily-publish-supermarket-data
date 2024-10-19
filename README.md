
Daliy publish to Kaggle
-------

This repository defines a cron jobs that runs every day.
The cron: 
  - Fetch the data from all supermarkets (every 4 hours)
  - Converted to data frame. (at midnight)
  - Compose kaggle dataset with a binding between the version number and the scrape date. (at midnight)
  - Create new version in Kaggle. (at midnight)


[Link to Kaggle](https://www.kaggle.com/datasets/erlichsefi/israeli-supermarkets-2024)

[![Last Publish Data to Kaggle Status](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/actions/workflows/publishing.yml/badge.svg)](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/actions/workflows/publishing.yml)
