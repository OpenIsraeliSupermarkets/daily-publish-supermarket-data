# test_daliy_raw_dump.py
import unittest
from unittest.mock import patch
import shutil
import os
import datetime
from daily_raw_dump import SupermarketDataPublisher
from il_supermarket_scarper.scrappers_factory import ScraperFactory
from il_supermarket_scarper import FileTypesFilters
from remotes import DummyFileStorge, MongoDbUploader, DummyDocumentDbUploader


def test_daliy_raw_dump():
    # params
    expected_duration_in_minutes = 2
    num_of_occasions = 2
    file_per_run = 10
    app_folder = "app_data"
    data_folder = "dumps"
    when_date = None  # datetime.datetime(2025,1,10,0,0,0)

    # run the process for couple of times
    publisher = SupermarketDataPublisher(
        long_term_db_target=DummyFileStorge,
        short_term_db_target=DummyDocumentDbUploader,
        app_folder=app_folder,
        data_folder=data_folder,
        enabled_scrapers=ScraperFactory.sample(n=1),
        enabled_file_types=None,
        limit=file_per_run,
        start_at=datetime.datetime.now(),
        completed_by=datetime.datetime.now()
        + datetime.timedelta(minutes=num_of_occasions * expected_duration_in_minutes),
        num_of_occasions=num_of_occasions,
        when_date=when_date,
    )
    publisher.run(
        now=True,
        itreative_operations="scraping,converting,api_update,clean_dump_files",
        final_operations="publishing,clean_all_source_data",
    )
    
    
    assert DummyFileStorge().was_updated_in_last_24h()
    assert DummyDocumentDbUploader().is_parser_updated()
    assert DummyDocumentDbUploader().get_number_of_updated() == num_of_occasions + 1


if __name__ == "__main__":
    test_daliy_raw_dump()
