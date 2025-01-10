# test_daliy_raw_dump.py
import unittest
from unittest.mock import patch
import shutil
import os
import datetime
from daliy_raw_dump import SupermarketDataPublisher
from il_supermarket_scarper.scrappers_factory import ScraperFactory
from remotes import Dummy


def test_daliy_raw_dump():
    # params
    num_of_occasions = 3
    file_per_run = 1
    app_folder = "app_data"
    data_folder = "dumps"

    # run the process for couple of times
    publisher = SupermarketDataPublisher(
        remote_upload_class=Dummy,
        app_folder=app_folder,
        data_folder=data_folder,
        enabled_scrapers=ScraperFactory.sample(4),
        limit=file_per_run,
        start_at=datetime.datetime.now(),
        completed_by=datetime.datetime.now()
        + datetime.timedelta(minutes=num_of_occasions),
        num_of_occasions=num_of_occasions,
    )
    publisher.run(itreative_operations='scraping,converting,clean_dump_files',final_operations='publishing,clean_all_source_data')
    

test_daliy_raw_dump()
