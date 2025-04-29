#!/usr/bin/env python3
"""
Script to download data from Kaggle and validate its structure.
This script connects to a Kaggle dataset, downloads the data,
and validates the structure of the downloaded data.
"""

import os
import tempfile
import logging
from datetime import datetime
import pytz
import sys

# Add parent directory to Python path so we can import modules from it
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from remotes import KaggleUploader,MongoDbUploader
from tests.validation_utils import validate_long_term_structure, validate_short_term_structure, validate_longterm_and_short_sync


def validate_data_storage(dataset_remote_name, enabled_scrapers, mongodb_uri, file_per_run=None,num_of_occasions=None, upload_to_long_term_db=False):
    """
    Download data from Kaggle and validate its structure.

    Args:
        dataset_name: Name of the Kaggle dataset
        enabled_scrapers: List of enabled scrapers
    Returns:
        bool: True if validation passes, False otherwise
    """

    # Create temporary directories
    temp_dir = tempfile.mkdtemp(prefix="kaggle_validation_")
    stage_folder = os.path.join(temp_dir, "test_stage")
    os.makedirs(stage_folder, exist_ok=True)

    logging.info(f"Created temporary directory: {temp_dir}")

    try:
        # Initialize KaggleUploader
        long_term_db_target = KaggleUploader(
            dataset_path=stage_folder,
            dataset_remote_name=dataset_remote_name,
            when=datetime.now(tz=pytz.utc),
        )
        
        short_term_db_target = MongoDbUploader(
            mongodb_uri=mongodb_uri
        )
        validate_short_term_structure(
            short_term_db_target,
            enabled_scrapers,
            num_of_occasions=num_of_occasions,
        )
        if upload_to_long_term_db:
            validate_long_term_structure(
                long_term_db_target, stage_folder, enabled_scrapers, in_app=False
            )
        
            validate_longterm_and_short_sync(
                enabled_scrapers,
                short_term_db_target,
                long_term_db_target,
                num_of_expected_files=num_of_occasions * file_per_run if file_per_run else None,
            )   
        

    finally:
        # Clean up temporary files
        import shutil

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logging.info(f"Cleaned up temporary directory: {temp_dir}")
