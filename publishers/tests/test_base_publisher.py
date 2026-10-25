"""
Integration tests for the BaseSupermarketDataPublisher class.
Tests the various operations of the publisher including scraping (which also\nparses and pushes the rows) and uploading data.
"""

import os
import shutil
import pytest
import tempfile
import pandas as pd
from remotes import DummyFileStorage, DummyDocumentDbUploader
from utils import now
from tests.validation_utils import (
    validate_scraper_output,
    validate_converting_output,
    validate_state_after_api_update,
    validate_long_term_structure,
    validate_local_structure_deleted,
    scrapers_to_test,
)
from publishers.base_publisher import BaseSupermarketDataPublisher


@pytest.mark.parametrize(
    "queue_caps",
    [
        {},
        # every hop full almost all the time (#141); 3 is the smallest
        # scraper cap that fits the extra end-of-stream markers
        {
            "scrape_queue_max_files": 3,
            "parse_queue_max_files": 1,
            "parsed_rows_queue_max": 10,
        },
    ],
    ids=["unbounded", "tight-caps"],
)
def test_execute_scraping_integration(queue_caps):
    """
    Integration test for the scraping execution: scrape, parse and push in
    one pipeline (#114).

    Note: This test is marked as integration because it would actually invoke
    the ScarpingTask which might do external API calls or other side effects.
    In a CI/CD pipeline, you might want to skip this test with --skip-integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_scraping_")

    try:
        # Create a publisher with minimum scraping processes
        enabled_scrapers = scrapers_to_test()
        short_term_db_target = DummyDocumentDbUploader(db_path=temp_dir)
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=5,
            number_of_parseing_processs=5,
            enabled_scrapers=enabled_scrapers,
            limit=1,  # Limit to just 1 item for faster tests
            short_term_db_target=short_term_db_target,
            **queue_caps,
        )

        # Execute scraping
        publisher._execute_scraping()

        # the scraper hands files to the parser in memory: no dump files
        validate_scraper_output(
            publisher.data_folder,
            publisher.scraping_status_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )
        # the csv files were created and the parser states
        validate_converting_output(
            publisher.data_folder,
            publisher.converting_status_folder,
            publisher.outputs_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )
        # every parsed row was pushed, with its file's file_complete
        for csv_file in os.listdir(publisher.outputs_folder):
            table = csv_file.split(".")[0]
            content = short_term_db_target.get_destinations_content(table)
            rows = [c for c in content if "row_index" in c]
            markers = [c for c in content if c.get("file_complete") == "true"]
            expected = len(
                pd.read_csv(os.path.join(publisher.outputs_folder, csv_file))
            )
            assert sorted(r["row_index"] for r in rows) == list(range(expected))
            assert sum(m["total_expected_records"] for m in markers) == expected

    except Exception as e:
        pytest.fail(f"Scraping function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


def test_dump_files_clean_integration():
    """
    Integration test for updating the API database.

    This test depends on data from scraping, so it would normally be run
    after test_execute_scraping_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_api_db_")

    try:
        # Create a publisher with minimum processing
        enabled_scrapers = scrapers_to_test()
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=5,
            number_of_parseing_processs=5,
            limit=1,
            enabled_scrapers=enabled_scrapers,
            short_term_db_target=DummyDocumentDbUploader(db_path=temp_dir),
        )

        # Scraping also parses and pushes the rows
        publisher._execute_scraping()
        publisher._clean_all_dump_files()

        validate_converting_output(
            publisher.data_folder,
            publisher.converting_status_folder,
            publisher.outputs_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )

        # status didn't changed
        validate_scraper_output(
            publisher.data_folder,
            publisher.scraping_status_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )

        # Check if the DummyDocumentDbUploader was updated
        # In a real implementation, we would need to check the actual database state
    except Exception as e:
        pytest.fail(f"API database update function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


def test_update_api_database_integration():
    """
    Integration test for updating the API database.

    This test depends on data from scraping, so it would normally be run
    after test_execute_scraping_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_api_db_")

    try:
        # Create a publisher with minimum processing
        enabled_scrapers = scrapers_to_test()
        short_term_db_target = DummyDocumentDbUploader(db_path=temp_dir)
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=5,
            number_of_parseing_processs=5,
            limit=1,
            enabled_scrapers=enabled_scrapers,
            short_term_db_target=short_term_db_target,
        )

        # Scraping also parses and pushes the rows
        publisher._execute_scraping()
        publisher._clean_all_dump_files()
        publisher._update_api_database()
        validate_converting_output(
            publisher.data_folder,
            publisher.converting_status_folder,
            publisher.outputs_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )

        # status didn't changed
        validate_scraper_output(
            publisher.data_folder,
            publisher.scraping_status_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )
        validate_state_after_api_update(
            publisher.app_folder,
            publisher.outputs_folder,
            enabled_scrapers,
            short_term_db_target,
        )

    finally:
        # Clean up
        shutil.rmtree(temp_dir)


def test_upload_to_kaggle_integration():
    """
    Integration test for uploading to Kaggle.

    This test depends on data from scraping, so it would normally be run
    after test_execute_scraping_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_kaggle_")

    try:
        # Create a publisher with minimum processing
        remote_dataset_path = os.path.join(temp_dir, "remote_test_dataset")
        stage_folder = os.path.join(temp_dir, "stage")

        long_term_db_target = DummyFileStorage(
            dataset_remote_path=remote_dataset_path,
            dataset_path=stage_folder,
            when=now(),
        )
        enabled_scrapers = scrapers_to_test()

        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=5,
            number_of_parseing_processs=5,
            limit=1,
            enabled_scrapers=enabled_scrapers,
            long_term_db_target=long_term_db_target,
            short_term_db_target=DummyDocumentDbUploader(db_path=temp_dir),
        )

        # Scraping also parses and pushes the rows
        publisher._execute_scraping()

        # the csv file was created and the parser states
        validate_converting_output(
            publisher.data_folder,
            publisher.converting_status_folder,
            publisher.outputs_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )

        # status didn't changed
        validate_scraper_output(
            publisher.data_folder,
            publisher.scraping_status_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )
        # Upload to Kaggle
        publisher._upload_to_kaggle()

        validate_long_term_structure(
            long_term_db_target, stage_folder, enabled_scrapers
        )

        # Check if the DummyFileStorage was updated
        # In a real implementation, we would need to check the actual Kaggle dataset
    except Exception as e:
        pytest.fail(f"Kaggle upload function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


def test_clean_all_source_data_integration():
    """
    Integration test for uploading to Kaggle and cleaning up.

    This test depends on data from scraping, so it would normally be run
    after test_execute_scraping_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_upload_clean_")

    try:
        # Create a publisher with minimum processing
        enabled_scrapers = scrapers_to_test()
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=5,
            number_of_parseing_processs=5,
            limit=1,
            enabled_scrapers=enabled_scrapers,
            short_term_db_target=DummyDocumentDbUploader(db_path=temp_dir),
        )

        # Scraping also parses and pushes the rows
        publisher._execute_scraping()

        # Upload to Kaggle and clean
        publisher._clean_all_source_data()

        # Verify that directories are cleaned
        validate_local_structure_deleted(
            publisher.app_folder,
            publisher.data_folder,
            publisher.outputs_folder,
            publisher.scraping_status_folder,
            publisher.converting_status_folder,
        )
    except Exception as e:
        pytest.fail(f"Upload and clean function raised an exception: {e}")
    finally:
        # Clean up anything that might remain
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
