"""
Integration tests for the SupermarketDataPublisher class.
Tests the full DAG execution pipeline from disk.
"""
import os
import tempfile
import mongomock
import random
from unittest.mock import patch
from il_supermarket_scarper.scrappers_factory import ScraperFactory
from publishers.dag_simulator import SupermarketDataPublisher
from tests.validation_utils import (
    validate_local_structure_deleted,
    validate_long_term_structure,
    validate_short_term_structure,
    validate_longterm_and_short_sync,
    scrapers_to_test
)
from remotes import DummyFileStorage, DummyDocumentDbUploader,KaggleUploader, MongoDbUploader
from utils import now


def run_full_dag_integration(remote_dataset_path, stage_folder, long_term_db_target, short_term_db_target, temp_dir, enabled_scrapers, file_per_run=3, num_of_occasions=2):
    """
    Test the full DAG integration for the SupermarketDataPublisher.
    Verifies data scraping, converting, API updates, and publishing.
    """
    # params
    wait_time_seconds = 5  # Time to wait between executions
    app_folder = "app_data"
    data_folder = "dumps"
    outputs_folder = "outputs"
    status_folder = "status"
    when_date = None

    app_folder = os.path.join(temp_dir, app_folder)
    outputs_folder = os.path.join(app_folder, outputs_folder)
    status_folder = os.path.join(app_folder, status_folder)
    data_folder = os.path.join(app_folder, data_folder)

    # run the process with wait time between executions
    publisher = SupermarketDataPublisher(
        long_term_db_target=long_term_db_target,
        short_term_db_target=short_term_db_target,
        app_folder=app_folder,
        data_folder=data_folder,
        enabled_scrapers=enabled_scrapers,
        enabled_file_types=None,
        limit=file_per_run,
        num_of_occasions=num_of_occasions,
        when_date=when_date,
        wait_time_seconds=wait_time_seconds,
    )
    
    # Run with wait time approach
    publisher.run(
        operations="scraping,converting,api_update,clean_dump_files",
        final_operations="publishing,clean_all_source_data"
    )

    validate_short_term_structure(
        short_term_db_target,
        enabled_scrapers,
        num_of_occasions=num_of_occasions
    )
    validate_long_term_structure(
        long_term_db_target, stage_folder, enabled_scrapers
    )
    validate_longterm_and_short_sync(
        enabled_scrapers,
        short_term_db_target,
        long_term_db_target,
        num_of_expected_files=num_of_occasions * file_per_run if file_per_run else None,
    )   

    # validate the output
    validate_local_structure_deleted(app_folder, data_folder, outputs_folder, status_folder)



def test_full_dag_integration_from_disk():
    
    with tempfile.TemporaryDirectory() as temp_dir:
        
        
        remote_dataset_path = os.path.join(temp_dir, "remote_test_dataset")
        stage_folder = os.path.join(temp_dir, "stage")
        long_term_db_target = DummyFileStorage(
                dataset_remote_path=remote_dataset_path,
                dataset_path=stage_folder,
                when=now(),
            )
        short_term_db_target = DummyDocumentDbUploader(db_path=temp_dir)
        
        run_full_dag_integration(
            remote_dataset_path,
            stage_folder,
            long_term_db_target,
            short_term_db_target,
            temp_dir,
            enabled_scrapers=random.sample(scrapers_to_test(), 1)
        )
        
def test_full_dag_integration_real():
    
    with tempfile.TemporaryDirectory() as temp_dir:
        
        remote_dataset_path = os.path.join(temp_dir, "remote_test_dataset")
        stage_folder = os.path.join(temp_dir, "stage")
        long_term_db_target = KaggleUploader(
                dataset_path=stage_folder,
                dataset_remote_name="test-super-dataset-2",
                when=now(),
            )
        
        with patch("pymongo.MongoClient", mongomock.MongoClient):
            short_term_db_target = MongoDbUploader(mongodb_uri=temp_dir)
        
            run_full_dag_integration(
                remote_dataset_path,
                stage_folder,
                long_term_db_target,
                short_term_db_target,
                temp_dir,
                enabled_scrapers=random.sample(scrapers_to_test(), 1)
                
            )