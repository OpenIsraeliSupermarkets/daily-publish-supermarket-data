
Daliy publish to Kaggle
-------

This repository defines a cron job that runs every day at 11 PM israel time.
The cron: 
  - Fetch the data from all supermarkets.
  - Converted to data frame.
  - Compose kaggle dataset with a binding between the version number and the scrape date.
  - Create new version in Kaggle.


[Link to Kaggle](https://www.kaggle.com/datasets/erlichsefi/israeli-supermarkets-data)

[![Last Publish Data to Kaggle Status](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/actions/workflows/cron-publish.yml/badge.svg?event=schedule)](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/actions/workflows/cron-publish.yml)
