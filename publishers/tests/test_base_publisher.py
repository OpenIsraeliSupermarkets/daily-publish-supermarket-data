"""
Integration tests for the BaseSupermarketDataPublisher class.
Tests the various operations of the publisher including scraping, converting, and uploading data.
"""

import os
import shutil
import pytest
import tempfile
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


def test_execute_scraping_integration():
    """
    Integration test for the scraping execution.

    Note: This test is marked as integration because it would actually invoke
    the ScarpingTask which might do external API calls or other side effects.
    In a CI/CD pipeline, you might want to skip this test with --skip-integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_scraping_")

    try:
        # Create a publisher with minimum scraping processes
        enabled_scrapers = scrapers_to_test()
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=5,
            enabled_scrapers=enabled_scrapers,
            limit=1,  # Limit to just 1 item for faster tests
        )

        # Execute scraping
        publisher._execute_scraping()

        # Check if the data folder was created
        validate_scraper_output(publisher.data_folder, enabled_scrapers)

    except Exception as e:
        pytest.fail(f"Scraping function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


def test_execute_converting_integration():
    """
    Integration test for the converting execution.

    This test depends on data from scraping, so it would normally be run
    after test_execute_scraping_integration.
    """
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="test_converting_")

    try:
        # Create a publisher with minimum processing
        enabled_scrapers = scrapers_to_test()
        publisher = BaseSupermarketDataPublisher(
            app_folder=temp_dir,
            number_of_scraping_processes=5,
            number_of_parseing_processs=5,
            limit=1,
            enabled_scrapers=enabled_scrapers,
        )

        # We need to run scraping first to get data to convert
        publisher._execute_scraping()

        # Execute converting
        publisher._execute_converting()

        # the csv file was created and the parser states
        validate_converting_output(
            publisher.data_folder, publisher.outputs_folder, enabled_scrapers
        )

        # status didn't changed
        validate_scraper_output(publisher.data_folder, enabled_scrapers)

    except Exception as e:
        pytest.fail(f"Converting function raised an exception: {e}")
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


def test_dump_files_clean_integration():
    """
    Integration test for updating the API database.

    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
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
        )

        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()
        publisher._clean_all_dump_files()

        validate_converting_output(
            publisher.data_folder,
            publisher.outputs_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )

        # status didn't changed
        validate_scraper_output(
            publisher.data_folder, enabled_scrapers, dump_files_deleted=True
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

    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
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

        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()
        publisher._clean_all_dump_files()
        publisher._update_api_database()
        validate_converting_output(
            publisher.data_folder,
            publisher.outputs_folder,
            enabled_scrapers,
            dump_files_deleted=True,
        )

        # status didn't changed
        validate_scraper_output(
            publisher.data_folder, enabled_scrapers, dump_files_deleted=True
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

    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
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
        )

        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()

        # the csv file was created and the parser states
        validate_converting_output(
            publisher.data_folder, publisher.outputs_folder, enabled_scrapers
        )

        # status didn't changed
        validate_scraper_output(publisher.data_folder, enabled_scrapers)
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

    This test depends on data from converting, so it would normally be run
    after test_execute_converting_integration.
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
        )

        # We need to run scraping and converting first
        publisher._execute_scraping()
        publisher._execute_converting()

        # Upload to Kaggle and clean
        publisher._clean_all_source_data()

        # Verify that directories are cleaned
        validate_local_structure_deleted(
            publisher.app_folder,
            publisher.data_folder,
            publisher.outputs_folder,
            publisher.status_folder,
        )
    except Exception as e:
        pytest.fail(f"Upload and clean function raised an exception: {e}")
    finally:
        # Clean up anything that might remain
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
